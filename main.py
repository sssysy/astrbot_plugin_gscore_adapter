"""AstrBot ↔ gsuid_core 适配器插件入口.

职责(平台侧):
- 插件生命周期: 加载即建立与 core 的 WS 连接, 卸载/重载时优雅断开;
- 监听 AstrBot 全部消息事件, 转换为 MessageReceive 上报 core;
- 监听平台元事件(进群/退群/戳一戳), 单独成包上报(见 meta_event.py);
- GSCORE_ONLY_PREFIXES 命中时拦截 AstrBot 后续 LLM 流程.

协议侧(连接/下发/回执/控制包)见 client.py 与 send_utils.py.
图片上报优先走自建图床(可重复读取), 不再依赖官方一次性 file token.
"""

import asyncio
import hashlib
import mimetypes
import time
from base64 import b64encode
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import override
from urllib.parse import urlparse, urlunparse

import aiofiles
from aiohttp import web
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult, filter
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core import astrbot_config, file_token_service
from astrbot.core.message.components import (
    At,
    File,
    Forward,
    Image,
    Node,
    Nodes,
    Plain,
    Reply,
)
from astrbot.core.platform.message_type import MessageType
from astrbot.core.star.filter.event_message_type import EventMessageType

from .client import GsClient
from .meta_event import build_meta_receive
from .models import Message as GsMessage
from .models import MessageReceive

PLUGIN_NAME = "astrbot_plugin_gscore_adapter"
_NODE_MARK = "[合并转发]"
_NODE_MAX_DEPTH = 3
# 图片缓存保留时长: 下游可能多次拉取, 不宜过短
_IMG_CACHE_MAX_AGE = 3600
_IMG_CACHE_SUFFIXES = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}


def _cfg_str(config: AstrBotConfig, key: str, default: str) -> str:
    """从配置读取字符串项; AstrBotConfig 为弱类型 dict, 统一收窄为 str."""
    val = config.get(key)
    return str(val) if val is not None else default


def _cfg_int(config: AstrBotConfig, key: str, default: int) -> int:
    """从配置读取整数项; 非法值回退默认值."""
    val = config.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _cfg_str_list(config: AstrBotConfig, key: str) -> list[str]:
    """从配置读取字符串列表项, 过滤空串与非字符串元素."""
    val = config.get(key)
    if not isinstance(val, list):
        return []
    return [item for item in val if isinstance(item, str) and item]


def _onebot_temp_group_id(event: AstrMessageEvent) -> str:
    """读取 SnowLuma 群临时私聊携带的来源群号."""
    if event.get_platform_name() != "aiocqhttp":
        return ""
    raw = getattr(event.message_obj, "raw_message", None)
    if raw is None:
        return ""

    getter = getattr(raw, "get", None)
    if not callable(getter):
        return ""
    if getter("sub_type") != "group":
        return ""

    value = getter("group_id")
    if value is None:
        sender = getter("sender")
        if isinstance(sender, dict):
            value = sender.get("group_id")
        elif sender is not None:
            value = getattr(sender, "group_id", None)
    return str(value) if value is not None else ""


@register(
    PLUGIN_NAME,
    "KimigaiiWuyi",
    "用于链接SayuCore（早柚核心）的适配器！适用于多种游戏功能, 原神、星铁、绝区零、鸣朝、雀魂等游戏的最佳工具箱！",
    "0.5.8",
)
class GsCoreAdapter(Star):
    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.config: AstrBotConfig = config
        self.GSCORE_ONLY_PREFIXES: list[str] = _cfg_str_list(
            config, "GSCORE_ONLY_PREFIXES"
        )

        self.temp_dir: Path = StarTools.get_data_dir(PLUGIN_NAME) / "temp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        # 独立于 AstrBot 事件 temp: pipeline 结束会删事件临时图, 副本须放插件目录
        self.img_cache_dir: Path = StarTools.get_data_dir(PLUGIN_NAME) / "img_cache"
        self.img_cache_dir.mkdir(parents=True, exist_ok=True)

        self.image_host_port: int = _cfg_int(config, "IMAGE_HOST_PORT", 6186)
        self._img_app: web.Application | None = None
        self._img_runner: web.AppRunner | None = None
        self._img_site: web.TCPSite | None = None
        self._img_host_ready: bool = False

        self.client: GsClient = GsClient(
            context,
            bot_id=_cfg_str(config, "BOT_ID", "AstrBot"),
            host=_cfg_str(config, "IP", "localhost"),
            port=_cfg_str(config, "PORT", "8765"),
            ws_token=_cfg_str(config, "WS_TOKEN", ""),
            max_retry=_cfg_int(config, "MAX_RETRY_TIMES", 30),
            temp_dir=self.temp_dir,
        )

    @override
    async def initialize(self) -> None:
        self._clean_temp_dir()
        # 不按 0 全量清: 重载后已有 URL 仍可读, 只按 TTL 淘汰
        self._prune_img_cache()
        await self._start_image_host()
        await self.client.start()

    @override
    async def terminate(self) -> None:
        await self._stop_image_host()
        await self.client.stop()

    def _image_host_base(self) -> str:
        """对外可达的图床根地址."""
        configured = _cfg_str(self.config, "IMAGE_HOST_BASE", "").rstrip("/")
        if configured:
            return configured
        port = self.image_host_port
        callback = str(astrbot_config.get("callback_api_base") or "").rstrip("/")
        if callback:
            parsed = urlparse(callback)
            host = parsed.hostname or "127.0.0.1"
            scheme = parsed.scheme or "http"
            return urlunparse((scheme, f"{host}:{port}", "", "", "", ""))
        return f"http://127.0.0.1:{port}"

    async def _serve_cached_image(self, request: web.Request) -> web.StreamResponse:
        name = request.match_info["name"]
        if not name or "/" in name or "\\" in name or ".." in name:
            raise web.HTTPNotFound()
        path = (self.img_cache_dir / name).resolve()
        try:
            path.relative_to(self.img_cache_dir.resolve())
        except ValueError:
            raise web.HTTPNotFound() from None
        if not path.is_file():
            raise web.HTTPNotFound()
        media_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        return web.FileResponse(
            path,
            headers={
                "Cache-Control": "public, max-age=3600",
                "Content-Type": media_type,
            },
        )

    async def _start_image_host(self) -> None:
        """启动可重复读取的图片 HTTP 服务.

        官方 /api/file/{token} 为单次有效+300s, 下游多次拉取会 404;
        自建路由按内容 hash 命名, 缓存存活期内可任意次数 GET.
        """
        try:
            app = web.Application()
            app.router.add_get("/img/{name}", self._serve_cached_image)
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            site = web.TCPSite(runner, "0.0.0.0", self.image_host_port)
            await site.start()
        except OSError as e:
            logger.warning(
                f"[GsCore] 图床端口 {self.image_host_port} 启动失败({e}), "
                "上报将回退官方 token/base64"
            )
            self._img_host_ready = False
            return
        except Exception as e:
            logger.warning(f"[GsCore] 图床启动失败: {e}")
            self._img_host_ready = False
            return

        self._img_app = app
        self._img_runner = runner
        self._img_site = site
        self._img_host_ready = True
        logger.info(
            f"[GsCore] 图床已启动: {self._image_host_base()}/img/<hash> "
            f"(port={self.image_host_port})"
        )

    async def _stop_image_host(self) -> None:
        runner = self._img_runner
        self._img_app = None
        self._img_runner = None
        self._img_site = None
        self._img_host_ready = False
        if runner is None:
            return
        try:
            await runner.cleanup()
        except Exception as e:
            logger.warning(f"[GsCore] 图床关闭异常: {e}")

    def _clean_temp_dir(self) -> None:
        """清理上次运行遗留的临时文件(file/video 段发送时落盘)."""
        try:
            for f in self.temp_dir.iterdir():
                if f.is_file():
                    f.unlink()
        except OSError as e:
            logger.warning(f"[GsCore] 清理临时目录失败: {e}")

    def _prune_img_cache(self, max_age: float = _IMG_CACHE_MAX_AGE) -> None:
        """清理过期图片缓存副本(启动时 max_age=0 全量清)."""
        try:
            now = time.time()
            for f in self.img_cache_dir.iterdir():
                if not f.is_file():
                    continue
                if now - f.stat().st_mtime > max_age:
                    f.unlink()
        except OSError as e:
            logger.warning(f"[GsCore] 清理图片缓存失败: {e}")

    async def _cache_image_for_file_service(self, src_path: str) -> str:
        """把图片复制到插件 img_cache, 供文件服务注册.

        AstrBot pipeline 结束会删除事件级 temp 图; 若直接注册原路径,
        gscore 稍后拉取会 404. 副本按内容 hash 命名以便去重.
        """
        src = Path(src_path)
        async with aiofiles.open(src, "rb") as f:
            data = await f.read()
        digest = hashlib.sha256(data).hexdigest()[:16]
        suffix = src.suffix.lower()
        if suffix not in _IMG_CACHE_SUFFIXES:
            suffix = ".bin"
        dest = self.img_cache_dir / f"{digest}{suffix}"
        if not dest.exists():
            async with aiofiles.open(dest, "wb") as f:
                await f.write(data)
        self._prune_img_cache()
        return str(dest)

    def _is_gscore_only_message(self, event: AstrMessageEvent) -> bool:
        if not self.GSCORE_ONLY_PREFIXES:
            return False

        raw_text = event.message_str.lstrip()
        if not raw_text:
            return False

        return any(raw_text.startswith(prefix) for prefix in self.GSCORE_ONLY_PREFIXES)

    async def _convert_file(self, file_msg: File) -> GsMessage | None:
        """把 File 转为 core 的 file 段 (name|value)"""
        logger.debug(f"[GsCore] 转换文件消息: {file_msg}")
        name = str(file_msg.name or "file")

        url = getattr(file_msg, "url", None)
        if isinstance(url, str) and url.startswith("http"):
            return GsMessage(type="file", data=f"{name}|{url}")

        for attr in ("file_", "url"):
            val = getattr(file_msg, attr, None)
            if isinstance(val, str) and val.startswith("base64://"):
                return GsMessage(type="file", data=f"{name}|{val}")

        file_path = getattr(file_msg, "file_", None)
        if not file_path:
            logger.warning(f"[GsCore] 文件消息缺少路径: {file_msg}")
            return None

        path = Path(str(file_path))
        if not path.exists():
            logger.warning(f"[GsCore] 文件不存在: {file_path}")
            return None

        file_val = await file_to_base64(path)
        return GsMessage(type="file", data=f"{name}|{file_val}")

    async def _convert_image(self, image_msg: Image) -> GsMessage | None:
        """图片上报 core: 直连 URL → 自建图床 → 官方 token → base64 兜底.

        gscore 下游插件默认按网络 URL 消费图片, 且可能对同一 URL 多次拉取.
        官方 file token 单次有效+300s, 不能满足; 优先走自建图床.
        """
        logger.debug(f"[GsCore] 转换图片消息: {image_msg}")

        # 1. 消息段自身已是网络 URL
        for attr in ("url", "file"):
            val = getattr(image_msg, attr, None)
            if isinstance(val, str) and val.startswith(("http://", "https://")):
                return GsMessage(type="image", data=val)

        local_path: str | None = None
        try:
            local_path = await image_msg.convert_to_file_path()
        except Exception as e:
            logger.debug(f"[GsCore] convert_to_file_path 失败: {e}")

        # 2. 自建图床(可重复读取)
        if local_path and self._img_host_ready:
            try:
                cached_path = await self._cache_image_for_file_service(local_path)
                name = Path(cached_path).name
                public_url = f"{self._image_host_base()}/img/{name}"
                logger.debug(f"[GsCore] 图片已写入自建图床: {public_url}")
                return GsMessage(type="image", data=public_url)
            except Exception as e:
                logger.warning(f"[GsCore] 自建图床写入失败, 尝试官方 token: {e}")

        # 3. 官方文件服务(单次有效, 兼容旧路径)
        if local_path:
            try:
                callback_host = str(
                    astrbot_config.get("callback_api_base") or ""
                ).rstrip("/")
                if not callback_host:
                    raise RuntimeError("未配置 callback_api_base")

                cached_path = await self._cache_image_for_file_service(local_path)
                token = await file_token_service.register_file(cached_path)
                public_url = f"{callback_host}/api/file/{token}"
                logger.debug(f"[GsCore] 图片已注册文件服务: {public_url}")
                return GsMessage(type="image", data=public_url)
            except Exception as e:
                logger.warning(
                    f"[GsCore] 图片无法生成图床链接(请检查图床/callback_api_base), "
                    f"回退 base64: {e}"
                )

        # 4. base64 兜底
        img_path = (
            local_path
            or getattr(image_msg, "path", None)
            or getattr(image_msg, "file", None)
            or getattr(image_msg, "url", None)
        )
        if not img_path:
            logger.warning(f"[GsCore] 图片消息缺少路径: {image_msg}")
            return None

        file_path = Path(str(img_path))
        if not file_path.exists():
            file_path = Path(__file__).parent / str(img_path)
        if not file_path.exists():
            logger.warning(f"[GsCore] 图片文件不存在: {img_path}")
            return None

        async with aiofiles.open(file_path, "rb") as f:
            img_data = await f.read()

        base64_data = b64encode(img_data).decode("utf-8")
        return GsMessage(type="image", data=f"base64://{base64_data}")

    def _reply_text(self, reply: Reply) -> str:
        """引用正文：优先平台解析好的纯文本，否则拼 chain 里的 Plain."""
        if reply.message_str:
            return str(reply.message_str)
        if reply.text:
            return str(reply.text)
        parts: list[str] = []
        for item in reply.chain or []:
            if isinstance(item, Plain):
                parts.append(item.text)
        return "".join(parts)

    def _node_preview(self, items: list[GsMessage]) -> str:
        lines: list[str] = [_NODE_MARK]
        for item in items:
            if item.type == "text" and item.data is not None:
                text = str(item.data).strip()
                if text:
                    lines.append(text)
            elif item.type == "image":
                lines.append("[图片]")
            elif item.type == "record":
                lines.append("[语音]")
            elif item.type == "video":
                lines.append("[视频]")
            elif item.type == "file":
                lines.append("[文件]")
        return "\n".join(lines)

    async def _parse_ob_forward(
        self,
        raw: object,
        event: AstrMessageEvent,
        depth: int,
        seen: set[str],
    ) -> list[GsMessage]:
        messages: object
        if isinstance(raw, dict) and "messages" in raw:
            messages = raw["messages"]
        else:
            messages = raw
        if not isinstance(messages, list):
            return [GsMessage(type="text", data=_NODE_MARK)]

        items: list[GsMessage] = []
        for entry in messages:
            if not isinstance(entry, dict):
                continue
            payload = entry
            if (
                "type" in entry
                and entry["type"] == "node"
                and "data" in entry
                and isinstance(entry["data"], dict)
            ):
                payload = entry["data"]

            nickname = ""
            if "sender" in payload and isinstance(payload["sender"], dict):
                sender = payload["sender"]
                if "nickname" in sender and sender["nickname"]:
                    nickname = str(sender["nickname"])
            elif "name" in payload and payload["name"]:
                nickname = str(payload["name"])
            if nickname:
                items.append(GsMessage(type="text", data=f"{nickname}:"))

            content: object = None
            if "content" in payload:
                content = payload["content"]
            elif "message" in payload:
                content = payload["message"]
            if isinstance(content, str) and content:
                items.append(GsMessage(type="text", data=content))
            elif isinstance(content, list):
                items.extend(
                    await self._ob_dict_segs_to_gs(content, event, depth, seen)
                )
        return items if items else [GsMessage(type="text", data=_NODE_MARK)]

    def _forward_id_from_dict(self, data: dict[str, object]) -> str:
        if "id" in data and data["id"] is not None:
            return str(data["id"])
        if "message_id" in data and data["message_id"] is not None:
            return str(data["message_id"])
        return ""

    async def _ob_dict_segs_to_gs(
        self,
        segs: list[object],
        event: AstrMessageEvent,
        depth: int,
        seen: set[str],
    ) -> list[GsMessage]:
        items: list[GsMessage] = []
        for seg in segs:
            if not isinstance(seg, dict):
                continue
            typ = str(seg["type"]) if "type" in seg and seg["type"] is not None else ""
            data = seg["data"] if "data" in seg and isinstance(seg["data"], dict) else {}
            if typ == "text" and "text" in data:
                items.append(GsMessage(type="text", data=str(data["text"])))
            elif typ == "image":
                url = data["url"] if "url" in data else (data["file"] if "file" in data else "")
                if url:
                    items.append(GsMessage(type="image", data=str(url)))
            elif typ == "at" and "qq" in data:
                items.append(GsMessage(type="at", data=str(data["qq"])))
            elif typ in {"forward", "forward_msg"}:
                fid = self._forward_id_from_dict(data)
                if fid:
                    items.append(GsMessage(type="text", data=_NODE_MARK))
                    items.extend(
                        await self._fetch_forward_items(event, fid, depth + 1, seen)
                    )
                else:
                    items.append(GsMessage(type="text", data=_NODE_MARK))
        return items

    async def _fetch_forward_items(
        self,
        event: AstrMessageEvent,
        forward_id: str,
        depth: int = 0,
        seen: set[str] | None = None,
    ) -> list[GsMessage]:
        visited = seen if seen is not None else set()
        if not forward_id or forward_id in visited or depth >= _NODE_MAX_DEPTH:
            return [GsMessage(type="text", data=_NODE_MARK)]
        visited.add(forward_id)
        platform_id = event.get_platform_id()
        platform = self.context.get_platform_inst(platform_id) if platform_id else None
        if platform is None or event.get_platform_name() != "aiocqhttp":
            return [GsMessage(type="text", data=_NODE_MARK)]
        bot = platform.get_client()
        try:
            raw = await bot.call_action("get_forward_msg", id=forward_id)
        except Exception as exc:
            logger.warning(f"[GsCore] 拉取合并转发失败: {exc}")
            return [GsMessage(type="text", data=_NODE_MARK)]
        return await self._parse_ob_forward(raw, event, depth, visited)

    async def _chain_to_node_items(
        self,
        chain: list[object],
        event: AstrMessageEvent,
        depth: int = 0,
        seen: set[str] | None = None,
    ) -> list[GsMessage]:
        visited = seen if seen is not None else set()
        items: list[GsMessage] = []
        for item in chain:
            if isinstance(item, Forward):
                items.append(GsMessage(type="text", data=_NODE_MARK))
                items.extend(
                    await self._fetch_forward_items(event, str(item.id), depth + 1, visited)
                )
                continue
            if isinstance(item, (Node, Nodes)):
                items.extend(
                    await self._flatten_nodes(item, event, depth + 1, visited)
                )
                continue
            items.extend(await self._build_single_content(item, event, from_reply=True))
        return items

    async def _flatten_nodes(
        self,
        msg: Node | Nodes,
        event: AstrMessageEvent,
        depth: int,
        seen: set[str],
    ) -> list[GsMessage]:
        if depth >= _NODE_MAX_DEPTH:
            return [GsMessage(type="text", data=_NODE_MARK)]
        items: list[GsMessage] = [GsMessage(type="text", data=_NODE_MARK)]
        nodes = msg.nodes if isinstance(msg, Nodes) else [msg]
        for node in nodes:
            if node.name:
                items.append(GsMessage(type="text", data=f"{node.name}:"))
            items.extend(
                await self._chain_to_node_items(node.content or [], event, depth, seen)
            )
        return items

    async def _forward_to_node(
        self, msg: Forward, event: AstrMessageEvent
    ) -> GsMessage:
        return GsMessage(
            type="node",
            data=await self._fetch_forward_items(event, str(msg.id)),
        )

    async def _nodes_to_node(
        self, msg: Node | Nodes, event: AstrMessageEvent
    ) -> GsMessage:
        items = await self._flatten_nodes(msg, event, 0, set())
        return GsMessage(
            type="node",
            data=items if items else [GsMessage(type="text", data=_NODE_MARK)],
        )

    async def _build_single_content(
        self,
        msg: object,
        event: AstrMessageEvent,
        *,
        from_reply: bool = False,
    ) -> list[GsMessage]:
        """把单个 AstrBot 消息段转换为 core 消息段."""
        if isinstance(msg, Image):
            image_data = await self._convert_image(msg)
            return [image_data] if image_data else []
        if isinstance(msg, File):
            file_data = await self._convert_file(msg)
            return [file_data] if file_data else []
        if isinstance(msg, Plain):
            return [GsMessage(type="text", data=msg.text)]
        if isinstance(msg, At):
            return [GsMessage(type="at", data=str(msg.qq))]
        if isinstance(msg, Forward):
            return [await self._forward_to_node(msg, event)]
        if isinstance(msg, (Node, Nodes)):
            return [await self._nodes_to_node(msg, event)]

        # 引用消息内经常会带 Json/Face 等 core 不消费的消息段；这些不应阻止
        # 当前消息里的命令文本继续上报。
        if not from_reply:
            logger.warning(f"[GsCore] 不支持的消息类型: {type(msg)}")
        return []

    async def _build_content(self, event: AstrMessageEvent) -> list[GsMessage]:
        """把 AstrBot 消息链转换为上报 core 的 GsMessage 列表.

        当前消息的文本/at/图片优先, 引用与合并转发作为上下文附在末尾,
        避免命令匹配先看到 reply/node。
        """
        current_message: list[GsMessage] = []
        quoted_context: list[GsMessage] = []

        for msg in event.get_messages():
            if isinstance(msg, Reply):
                quoted_context.append(GsMessage(type="reply_id", data=str(msg.id)))
                reply_text = self._reply_text(msg)
                quoted_nodes: list[GsMessage] = []
                for reply_msg in msg.chain or []:
                    if isinstance(reply_msg, Image):
                        quoted_context.extend(
                            await self._build_single_content(
                                reply_msg, event, from_reply=True
                            )
                        )
                    elif isinstance(reply_msg, File):
                        quoted_context.extend(
                            await self._build_single_content(
                                reply_msg, event, from_reply=True
                            )
                        )
                    elif isinstance(reply_msg, Forward):
                        node = await self._forward_to_node(reply_msg, event)
                        quoted_nodes.append(node)
                    elif isinstance(reply_msg, (Node, Nodes)):
                        node = await self._nodes_to_node(reply_msg, event)
                        quoted_nodes.append(node)
                if quoted_nodes:
                    first = quoted_nodes[0]
                    node_items: list[GsMessage] = []
                    if isinstance(first.data, list):
                        for raw in first.data:
                            if isinstance(raw, GsMessage):
                                node_items.append(raw)
                    preview = self._node_preview(node_items) if node_items else _NODE_MARK
                    if not reply_text or _NODE_MARK not in reply_text:
                        reply_text = preview if not reply_text else f"{_NODE_MARK}\n{reply_text}"
                quoted_context.append(GsMessage(type="reply", data=reply_text))
                quoted_context.extend(quoted_nodes)
                continue

            current_message.extend(await self._build_single_content(msg, event))

        return current_message + quoted_context

    @filter.event_message_type(EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent) -> None:
        # 幂等: 连接循环若已退出(超过最大重试次数)则重新拉起
        await self.client.start()

        pn = event.get_platform_name()
        # bot_id在gscore内部数据库具有唯一标识符，修改将会造成breaking change
        bot_id = "onebot" if pn == "aiocqhttp" else pn
        # bot_self_id 使用平台实例 id, 下发时据此路由回对应平台
        platform_id = event.get_platform_id() or event.get_self_id()
        pm = 1 if event.is_admin() else 6

        # 元事件(进群/退群/戳一戳)优先: 命中则单独成包上报, 不进普通消息流程
        meta_msg = build_meta_receive(event, bot_id, platform_id, pm)
        if meta_msg is not None:
            logger.info(f"【发送】[gsuid-core][Meta]: {meta_msg.content[0].type}")
            await self.client.report(meta_msg)
            return

        content = await self._build_content(event)
        if not content:
            return

        self_id = event.get_self_id()
        user_id = str(event.get_sender_id())
        if pn == "qq_official":
            avatar = f"https://q.qlogo.cn/qqapp/{self_id}/{user_id}/100"
        elif pn == "aiocqhttp":
            avatar = f"https://q1.qlogo.cn/g?b=qq&nk={user_id}&s=640"
        else:
            avatar = ""

        msg = MessageReceive(
            bot_id=bot_id,
            bot_self_id=platform_id,
            user_type=(
                "group"
                if event.get_message_type() == MessageType.GROUP_MESSAGE
                else "direct"
            ),
            group_id=(
                event.get_group_id()
                or _onebot_temp_group_id(event)
                or None
            ),
            user_id=user_id,
            sender={"nickname": event.get_sender_name(), "avatar": avatar},
            content=content,
            # 非 onebot 平台下发时以 msg_id 回读会话 id(core 会原样带回)
            msg_id=event.get_session_id(),
            user_pm=pm,
        )
        logger.info(f"【发送】[gsuid-core]: {msg.bot_id}")
        await self.client.report(msg)

        if self._is_gscore_only_message(event):
            # 按 AstrBot 文档显式阻断事件传播, 不参与后续 LLM 等流程
            event.stop_event()
            logger.info(
                "[GsCore] 当前消息命中GSCORE_ONLY_PREFIXES，已调用 stop_event() 拦截后续 AstrBot LLM 流程"
            )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("连接core", alias={"链接core"})
    async def connect_core(
        self, event: AstrMessageEvent
    ) -> AsyncGenerator[MessageEventResult, None]:
        """手动重连 gsuid_core."""
        await self.client.start()
        for _ in range(6):
            if self.client.is_connected:
                break
            await asyncio.sleep(0.5)
        if self.client.is_connected:
            yield event.plain_result("链接成功！")
        else:
            yield event.plain_result("正在尝试连接core, 请稍后通过日志确认连接状态...")


async def file_to_base64(file_path: Path) -> str:
    async with aiofiles.open(str(file_path), "rb") as file:
        file_content = await file.read()
    return b64encode(file_content).decode("utf-8")
