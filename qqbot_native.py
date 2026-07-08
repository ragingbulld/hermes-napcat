"""Official QQBot native adapter bundled inside the hermes-napcat plugin.

This is intentionally plugin-local and does not import Hermes' built-in
``gateway.platforms.qqbot.QQAdapter``.  It implements the small core needed for
native Markdown QQBot access:

- appId/clientSecret token refresh
- QQBot gateway WebSocket identify/heartbeat
- inbound C2C, group @, guild/channel, and guild-DM message events
- outbound C2C/group Markdown messages, guild text messages
- outbound C2C/group official RichMedia document messages for URLs/small local files
- owner/admin/user ACL + pre-tool-call guard

Advanced official QQBot features (media upload, buttons, channel reactions) can
be layered on later without changing Hermes core.
"""
from __future__ import annotations

import asyncio
import base64
from contextlib import suppress
import html
import json
import logging
import mimetypes
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult, cache_image_from_bytes

logger = logging.getLogger(__name__)

PLATFORM_NAME = "qqbot_native"
API_BASE = "https://api.sgroup.qq.com"
TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
GATEWAY_URL_PATH = "/gateway"
MSG_TYPE_TEXT = 0
MSG_TYPE_MARKDOWN = 2
MSG_TYPE_MEDIA = 7
MEDIA_TYPE_FILE = 4
MAX_MESSAGE_LENGTH = 4000
MAX_INLINE_FILE_BYTES = 9_500_000
DEFAULT_API_TIMEOUT = aiohttp.ClientTimeout(total=30)
FILE_UPLOAD_TIMEOUT = 120.0
LAST_MESSAGE_STATE_PATH = Path(os.getenv("HERMES_HOME", "~/.hermes")).expanduser() / "qqbot_native_last_messages.json"
PASSIVE_REPLY_WINDOW_SECONDS = 5 * 60
IMAGE_URL_KEYS = {
    "url",
    "uri",
    "src",
    "file",
    "file_url",
    "fileUrl",
    "image_url",
    "imageUrl",
    "file_image",
    "fileImage",
    "pic_url",
    "picUrl",
    "origin_url",
    "originUrl",
    "download_url",
    "downloadUrl",
}

# Keep this separate from NapCat's QQ-number ACL. Official QQBot exposes OpenIDs.
_OWNER_ONLY_TOOLS = {"memory", "fact_store", "fact_feedback"}
_USER_ALLOWED_TOOLS: set[str] = set()

QQBOT_NATIVE_PLATFORM_PROMPT = (
    "官方 QQBot Native 插件会话。身份使用 OpenID，不使用普通 QQ 号；"
    "权限身份只按配置中的 OpenID 判定，不按昵称、群名片或群名判定。"
    "工具权限策略：owner 在本插件 ACL 层不额外限制；admin 默认可用工具，"
    "但不能使用 owner-only 的长期记忆/用户画像工具；普通用户只能普通聊天，"
    "不能调用任何工具，也不能使用 /new、/reset、/approve 等 slash 指令。"
    "涉及越权工具或指令时必须拒绝，或让用户联系 owner/admin。"
    "群聊隐私规则：USER PROFILE、长期记忆和画像默认属于 owner 本人，"
    "不是群里所有发言者；群聊回复中不得披露 owner 的个人信息、个人画像或私密记忆。"
    "可以在不明说隐私内容的前提下内部参考非敏感偏好来改善回答；"
    "如用户要求查看、复述或确认 owner 个人信息/私密记忆，应拒绝并建议 owner 私聊。"
    "权限信息本身只可按当前发言者前缀/OpenID 简要判断，不得把 owner 画像套到 admin 或普通群友身上。"
)


def check_qqbot_native_requirements() -> bool:
    return True


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        return default
    return bool(value)


def _as_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(part).strip() for part in value if str(part).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _load_acl_config() -> tuple[set[str], set[str]]:
    try:
        from hermes_cli.config import load_config

        cfg = load_config() or {}
    except Exception:
        cfg = {}
    extra = (((cfg.get("platforms") or {}).get(PLATFORM_NAME) or {}).get("extra") or {})
    owners = set(_as_str_list(extra.get("owners") or extra.get("owner")))
    admins = set(_as_str_list(extra.get("admins")))
    return owners, admins


def _role_for_user(user_id: str, owners: set[str], admins: set[str]) -> str:
    user_id = str(user_id or "").strip()
    if user_id and user_id in owners:
        return "owner"
    if user_id and user_id in admins:
        return "admin"
    return "user"


def _safe_identity_part(value: str, *, max_len: int = 40) -> str:
    cleaned = re.sub(r"[\r\n\t]+", " ", str(value or "")).strip()
    cleaned = cleaned.replace("[", "［").replace("]", "］")
    cleaned = re.sub(r"\s+", " ", cleaned)
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 1] + "…"
    return cleaned


def _sender_identity_label(role: str, user_id: str) -> str:
    role_label = {"owner": "owner", "admin": "admin", "user": "user"}.get(role, "user")
    oid = _safe_identity_part(user_id, max_len=32) or "unknown"
    return f"[{role_label}]<{oid}>"


def _guess_image_ext(url: str, content_type: str = "") -> str:
    ctype = (content_type or "").split(";", 1)[0].strip().lower()
    if ctype:
        ext = mimetypes.guess_extension(ctype)
        if ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}:
            return ".jpg" if ext == ".jpeg" else ext
    path = url.split("?", 1)[0].lower()
    for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"):
        if path.endswith(ext):
            return ".jpg" if ext == ".jpeg" else ext
    return ".jpg"


def _looks_like_image_url(url: str) -> bool:
    lowered = str(url or "").lower()
    if not lowered.startswith(("http://", "https://")):
        return False
    if any(lowered.split("?", 1)[0].endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")):
        return True
    # Official QQ group image URLs are often temporary download endpoints with
    # no image extension, e.g. multimedia.nt.qq.com.cn/download?... .
    return "multimedia.nt.qq.com" in lowered or "gchat.qpic.cn" in lowered or "qpic.cn" in lowered


def _decode_qqbot_ext_payload(value: str) -> Any | None:
    raw = html.unescape(str(value or "").strip())
    if not raw:
        return None
    padding = "=" * (-len(raw) % 4)
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            decoded = decoder((raw + padding).encode())
            text = decoded.decode("utf-8", errors="replace").strip()
            return json.loads(text)
        except Exception:
            continue
    return None


def qqbot_native_acl_pre_tool_call(tool_name: str, **_: Any) -> dict | None:
    """Hard permission gate for official QQBot-native sessions."""
    try:
        from gateway.session_context import get_session_env

        platform = get_session_env("HERMES_SESSION_PLATFORM", "")
        user_id = get_session_env("HERMES_SESSION_USER_ID", "")
    except Exception:
        return None
    if platform != PLATFORM_NAME:
        return None

    owners, admins = _load_acl_config()
    role = _role_for_user(user_id, owners, admins)
    if role == "owner":
        return None
    if role == "admin":
        if tool_name in _OWNER_ONLY_TOOLS:
            return {
                "action": "block",
                "message": f"QQBot Native 权限不足：工具 {tool_name} 仅 owner 可用。",
            }
        return None
    if tool_name not in _USER_ALLOWED_TOOLS:
        return {
            "action": "block",
            "message": f"QQBot Native 权限不足：普通用户不能调用工具 {tool_name}。",
        }
    return None


class QQBotNativeAdapter(BasePlatformAdapter):
    """Plugin-local official QQBot adapter with native Markdown output."""

    SUPPORTS_MESSAGE_EDITING = False
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: PlatformConfig) -> None:
        super().__init__(config, Platform(PLATFORM_NAME))
        extra = getattr(config, "extra", {}) or {}
        self._app_id = str(extra.get("app_id") or os.getenv("QQBOT_NATIVE_APP_ID") or os.getenv("QQ_APP_ID", "")).strip()
        self._client_secret = str(
            extra.get("client_secret")
            or os.getenv("QQBOT_NATIVE_CLIENT_SECRET")
            or os.getenv("QQ_CLIENT_SECRET", "")
        ).strip()
        self._markdown_support = _as_bool(extra.get("markdown_support"), default=True)
        self._guild_markdown_support = _as_bool(extra.get("guild_markdown_support"), default=False)
        self._owners = set(_as_str_list(extra.get("owners") or extra.get("owner")))
        self._admins = set(_as_str_list(extra.get("admins")))
        self._dm_policy = str(extra.get("dm_policy", "allowlist")).strip().lower()
        self._allow_from = set(_as_str_list(extra.get("allow_from") or extra.get("allowFrom")))
        self._group_policy = str(extra.get("group_policy", "allowlist")).strip().lower()
        self._group_allow_chats = {
            str(x).strip()
            for x in _as_str_list(
                extra.get("group_allow_chats")
                or extra.get("group_allow_from")
                or extra.get("groupAllowFrom")
            )
            if str(x).strip()
        }
        self._group_sessions_per_user = _as_bool(extra.get("group_sessions_per_user"), default=True)
        self._require_mention = _as_bool(extra.get("group_require_mention", extra.get("require_mention")), default=True)
        self._access_token = ""
        self._token_expires_at = 0.0
        self._token_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._listen_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_interval = 30.0
        self._session_id: str | None = None
        self._last_seq: int | None = None
        self._last_msg_id: dict[str, str] = {}
        self._last_msg_ts: dict[str, float] = {}
        self._load_last_message_state()
        self._chat_type_map: dict[str, str] = {}
        self._seen_messages: dict[str, float] = {}

    @property
    def enforces_own_access_policy(self) -> bool:
        return True

    @property
    def authorization_is_upstream(self) -> bool:
        return True

    @property
    def is_connected(self) -> bool:
        return bool(self._running and self._ws and not self._ws.closed)

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        del is_reconnect
        if not self._app_id or not self._client_secret:
            logger.warning("QQBot Native disabled: app_id/client_secret not configured")
            return False
        try:
            self._session = aiohttp.ClientSession(trust_env=True)
            await self._ensure_token()
            gateway_url = await self._get_gateway_url()
            await self._open_ws(gateway_url)
            self._running = True
            self._listen_task = asyncio.create_task(self._listen_loop(), name="qqbot-native-listen")
            logger.info("QQBot Native connected")
            return True
        except Exception as exc:
            logger.error("QQBot Native connect failed: %s", exc, exc_info=True)
            await self.disconnect()
            return False

    async def disconnect(self) -> None:
        self._running = False
        for task in (self._listen_task, self._heartbeat_task):
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        self._listen_task = None
        self._heartbeat_task = None
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _ensure_token(self) -> str:
        if self._access_token and time.time() < self._token_expires_at - 60:
            return self._access_token
        async with self._token_lock:
            if self._access_token and time.time() < self._token_expires_at - 60:
                return self._access_token
            if not self._session:
                self._session = aiohttp.ClientSession(trust_env=True)
            async with self._session.post(
                TOKEN_URL,
                json={"appId": self._app_id, "clientSecret": self._client_secret},
                timeout=DEFAULT_API_TIMEOUT,
            ) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"token HTTP {resp.status}: {text[:200]}")
                data = json.loads(text)
            token = data.get("access_token")
            if not token:
                raise RuntimeError(f"token response missing access_token: {data}")
            self._access_token = str(token)
            self._token_expires_at = time.time() + int(data.get("expires_in", 7200))
            return self._access_token

    async def _api_request(self, method: str, path: str, body: dict | None = None) -> dict:
        if not self._session:
            self._session = aiohttp.ClientSession(trust_env=True)
        token = await self._ensure_token()
        headers = {"Authorization": f"QQBot {token}", "Content-Type": "application/json"}
        async with self._session.request(
            method,
            f"{API_BASE}{path}",
            json=body,
            headers=headers,
            timeout=DEFAULT_API_TIMEOUT,
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"QQBot Native API HTTP {resp.status} {path}: {text[:300]}")
            if not text.strip():
                return {}
            return json.loads(text)

    async def _get_gateway_url(self) -> str:
        data = await self._api_request("GET", GATEWAY_URL_PATH)
        url = str(data.get("url") or "").strip()
        if not url:
            raise RuntimeError(f"gateway response missing url: {data}")
        return url

    async def _open_ws(self, gateway_url: str) -> None:
        if not self._session:
            self._session = aiohttp.ClientSession(trust_env=True)
        proxy = (
            os.getenv("WSS_PROXY")
            or os.getenv("wss_proxy")
            or os.getenv("HTTPS_PROXY")
            or os.getenv("https_proxy")
            or os.getenv("ALL_PROXY")
            or os.getenv("all_proxy")
        )
        self._ws = await self._session.ws_connect(
            gateway_url,
            proxy=proxy,
        )

    async def _listen_loop(self) -> None:
        reconnect_delay = 2.0
        try:
            while self._running:
                try:
                    if not self._ws or self._ws.closed:
                        gateway_url = await self._get_gateway_url()
                        await self._open_ws(gateway_url)
                        logger.info("QQBot Native websocket reconnected")

                    while self._running and self._ws and not self._ws.closed:
                        msg = await self._ws.receive()
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            reconnect_delay = 2.0
                            self._dispatch_payload(json.loads(msg.data))
                        elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE}:
                            logger.warning(
                                "QQBot Native websocket closed/error: type=%s extra=%s exception=%s",
                                msg.type,
                                getattr(msg, "extra", None),
                                self._ws.exception() if self._ws else None,
                            )
                            break

                    if not self._running:
                        break
                    logger.warning("QQBot Native listen loop reconnecting in %.1fs", reconnect_delay)
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60.0)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if not self._running:
                        break
                    logger.error(
                        "QQBot Native listen loop error; reconnecting in %.1fs: %s",
                        reconnect_delay,
                        exc,
                        exc_info=True,
                    )
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60.0)
        except asyncio.CancelledError:
            raise
        finally:
            if self._running:
                logger.warning("QQBot Native listen loop exited unexpectedly; marking disconnected")
            self._running = False

    def _dispatch_payload(self, payload: dict[str, Any]) -> None:
        op = payload.get("op")
        t = payload.get("t")
        s = payload.get("s")
        d = payload.get("d")
        if isinstance(s, int):
            self._last_seq = s
        if op == 10:
            hello = d if isinstance(d, dict) else {}
            self._heartbeat_interval = float(hello.get("heartbeat_interval", 30000)) / 1000.0 * 0.8
            asyncio.create_task(self._send_identify())
            if self._heartbeat_task is None or self._heartbeat_task.done():
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            return
        if op == 0 and t:
            if t == "READY" and isinstance(d, dict):
                self._session_id = str(d.get("session_id") or "")
                logger.info("QQBot Native ready, session_id=%s", self._session_id)
                return
            if t in {"C2C_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE", "GUILD_MESSAGE_CREATE", "GUILD_AT_MESSAGE_CREATE", "DIRECT_MESSAGE_CREATE"}:
                asyncio.create_task(self._on_message(str(t), d))
                return
        if op == 7 and self._ws and not self._ws.closed:
            asyncio.create_task(self._ws.close())

    async def _send_identify(self) -> None:
        token = await self._ensure_token()
        payload = {
            "op": 2,
            "d": {
                "token": f"QQBot {token}",
                "intents": (1 << 25) | (1 << 30) | (1 << 12) | (1 << 26),
                "shard": [0, 1],
                "properties": {"$os": "linux", "$browser": "hermes-napcat", "$device": "hermes-napcat"},
            },
        }
        if self._ws and not self._ws.closed:
            await self._ws.send_json(payload)

    async def _heartbeat_loop(self) -> None:
        try:
            while self._running and self._ws and not self._ws.closed:
                await asyncio.sleep(max(5.0, self._heartbeat_interval))
                if self._ws and not self._ws.closed:
                    await self._ws.send_json({"op": 1, "d": self._last_seq})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("QQBot Native heartbeat stopped: %s", exc)

    def _is_duplicate(self, msg_id: str) -> bool:
        now = time.time()
        self._seen_messages = {k: v for k, v in self._seen_messages.items() if now - v < 300}
        if msg_id in self._seen_messages:
            return True
        self._seen_messages[msg_id] = now
        return False

    def _load_last_message_state(self) -> None:
        try:
            data = json.loads(LAST_MESSAGE_STATE_PATH.read_text())
            if not isinstance(data, dict):
                return
            now = time.time()
            for chat_id, item in data.items():
                if not isinstance(item, dict):
                    continue
                msg_id = str(item.get("msg_id") or "")
                ts = float(item.get("ts") or 0)
                if msg_id and now - ts <= PASSIVE_REPLY_WINDOW_SECONDS:
                    self._last_msg_id[str(chat_id)] = msg_id
                    self._last_msg_ts[str(chat_id)] = ts
        except FileNotFoundError:
            return
        except Exception as exc:
            logger.debug("QQBot Native could not load last-message state: %s", exc)

    def _remember_message_id(self, chat_id: str, message_id: str) -> None:
        if not chat_id or not message_id:
            return
        now = time.time()
        self._last_msg_id[chat_id] = message_id
        self._last_msg_ts[chat_id] = now
        try:
            LAST_MESSAGE_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            data = {
                key: {"msg_id": saved_msg_id, "ts": self._last_msg_ts.get(key, now)}
                for key, saved_msg_id in self._last_msg_id.items()
                if now - self._last_msg_ts.get(key, 0) <= PASSIVE_REPLY_WINDOW_SECONDS
            }
            LAST_MESSAGE_STATE_PATH.write_text(json.dumps(data), encoding="utf-8")
        except Exception as exc:
            logger.debug("QQBot Native could not persist last-message state: %s", exc)

    def _recent_reply_anchor(self, chat_id: str) -> str | None:
        msg_id = self._last_msg_id.get(chat_id)
        ts = self._last_msg_ts.get(chat_id, 0)
        if msg_id and time.time() - ts <= PASSIVE_REPLY_WINDOW_SECONDS:
            return msg_id
        return None

    def _collect_image_candidates(self, value: Any, *, inherited_mime: str = "", image_hint: bool = False) -> list[tuple[str, str]]:
        """Collect possible official QQBot image URLs from nested event data."""
        found: list[tuple[str, str]] = []
        if isinstance(value, list):
            for item in value:
                found.extend(self._collect_image_candidates(item, inherited_mime=inherited_mime, image_hint=image_hint))
            return found
        if not isinstance(value, dict):
            return found

        mime = str(
            value.get("content_type")
            or value.get("contentType")
            or value.get("mime_type")
            or value.get("mimeType")
            or inherited_mime
            or ""
        ).strip()
        filename = str(value.get("filename") or value.get("file_name") or value.get("name") or "")
        local_image_hint = image_hint or mime.lower().startswith("image/") or filename.lower().endswith(
            (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")
        )

        for key, item in value.items():
            if isinstance(item, str) and key in IMAGE_URL_KEYS:
                url = item.strip()
                if url.startswith(("http://", "https://")) and (local_image_hint or _looks_like_image_url(url)):
                    found.append((url, mime or "image/jpeg"))
            elif isinstance(item, (dict, list)):
                found.extend(self._collect_image_candidates(item, inherited_mime=mime, image_hint=local_image_hint))
        return found

    def _image_candidates_from_content(self, content: str) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
        # Official QQ group rich-media placeholders can look like:
        # <faceType=6,faceId="0",ext="<base64-json>">.  The ext JSON may carry
        # a temporary image URL even when the message has no attachments field.
        for match in re.finditer(r'\bext=(?:"([^"]+)"|\'([^\']+)\')', content or ""):
            payload = _decode_qqbot_ext_payload(match.group(1) or match.group(2) or "")
            if payload is not None:
                found.extend(self._collect_image_candidates(payload, image_hint=True))
        for match in re.finditer(r"https?://[^\s<>'\"]+", content or ""):
            url = match.group(0).strip()
            if _looks_like_image_url(url):
                found.append((url, "image/jpeg"))
        return found

    def _extract_image_candidates(self, payload: dict[str, Any], content: str) -> list[tuple[str, str]]:
        candidates: list[tuple[str, str]] = []
        for key in ("attachments", "attachment", "images", "image", "media", "file_image"):
            value = payload.get(key)
            if isinstance(value, str) and _looks_like_image_url(value):
                candidates.append((value, "image/jpeg"))
            else:
                candidates.extend(self._collect_image_candidates(value))
        candidates.extend(self._image_candidates_from_content(content))
        deduped: list[tuple[str, str]] = []
        seen: set[str] = set()
        for url, mime in candidates:
            if url not in seen:
                seen.add(url)
                deduped.append((url, mime or "image/jpeg"))
        return deduped

    async def _download_image_candidates(self, candidates: list[tuple[str, str]]) -> tuple[list[str], list[str]]:
        if not candidates:
            return [], []
        if not self._session:
            self._session = aiohttp.ClientSession(trust_env=True)
        token = await self._ensure_token()
        paths: list[str] = []
        media_types: list[str] = []
        headers = {"Authorization": f"QQBot {token}", "Accept": "image/*,*/*;q=0.8"}
        for url, hinted_mime in candidates[:4]:
            try:
                async with self._session.get(url, headers=headers, timeout=DEFAULT_API_TIMEOUT) as resp:
                    data = await resp.read()
                    if resp.status >= 400:
                        raise RuntimeError(f"HTTP {resp.status}: {data[:160]!r}")
                    mime = (resp.headers.get("Content-Type") or hinted_mime or "image/jpeg").split(";", 1)[0].strip()
                path = cache_image_from_bytes(data, _guess_image_ext(url, mime))
                paths.append(path)
                media_types.append(mime if mime.startswith("image/") else "image/jpeg")
            except Exception as exc:
                logger.warning("QQBot Native image download failed: url=%s error=%s", url[:180], exc)
        if candidates and not paths:
            logger.warning("QQBot Native found %d image candidate(s), but none could be downloaded", len(candidates))
        return paths, media_types

    @staticmethod
    def _detect_message_type(media_urls: list[str], media_types: list[str]) -> MessageType:
        """Mirror Hermes' built-in qqbot attachment → MessageType behavior."""
        if not media_urls:
            return MessageType.TEXT
        if not media_types:
            return MessageType.PHOTO
        first_type = (media_types[0] or "").lower()
        if "video" in first_type:
            return MessageType.VIDEO
        if "image" in first_type or "photo" in first_type:
            return MessageType.PHOTO
        if "audio" in first_type or "voice" in first_type or "silk" in first_type:
            return MessageType.VOICE
        return MessageType.TEXT

    @staticmethod
    def _attachment_url(att: dict[str, Any]) -> str:
        for key in ("url", "file_url", "fileUrl", "download_url", "downloadUrl", "uri"):
            raw = str(att.get(key) or "").strip()
            if raw.startswith("//"):
                return f"https:{raw}"
            if raw:
                return raw
        return ""

    async def _process_attachments(self, attachments: Any) -> dict[str, Any]:
        """Process standard official QQBot attachments like the built-in adapter.

        Image attachments are downloaded and cached into local `media_urls`.
        Non-image attachments are preserved as short text notes; rich-media
        placeholders embedded in `content` are handled by `_process_event_media`.
        """
        if not isinstance(attachments, list):
            return {"image_urls": [], "image_media_types": [], "attachment_info": ""}

        image_urls: list[str] = []
        image_media_types: list[str] = []
        other_attachments: list[str] = []

        for att in attachments:
            if not isinstance(att, dict):
                continue
            ct = str(
                att.get("content_type")
                or att.get("contentType")
                or att.get("mime_type")
                or att.get("mimeType")
                or ""
            ).strip().lower()
            filename = str(att.get("filename") or att.get("file_name") or att.get("name") or "")
            url = self._attachment_url(att)
            if not url:
                continue
            if ct.startswith("image/") or filename.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")) or _looks_like_image_url(url):
                paths, types = await self._download_image_candidates([(url, ct or "image/jpeg")])
                image_urls.extend(paths)
                image_media_types.extend(types)
            else:
                label = filename or ct or "attachment"
                other_attachments.append(f"[file: {label}]")

        return {
            "image_urls": image_urls,
            "image_media_types": image_media_types,
            "attachment_info": "\n".join(other_attachments),
        }

    async def _process_event_media(self, payload: dict[str, Any], content: str) -> tuple[list[str], list[str], str]:
        """Return `(media_urls, media_types, attachment_info)` for a QQBot event."""
        att_result = await self._process_attachments(payload.get("attachments"))
        media_urls = list(att_result["image_urls"])
        media_types = list(att_result["image_media_types"])
        attachment_info = str(att_result.get("attachment_info") or "")

        fallback_payload = {k: v for k, v in payload.items() if k != "attachments"}
        fallback_candidates = self._extract_image_candidates(fallback_payload, content)
        fallback_urls, fallback_types = await self._download_image_candidates(fallback_candidates)
        seen = set(media_urls)
        for path, mime in zip(fallback_urls, fallback_types):
            if path not in seen:
                seen.add(path)
                media_urls.append(path)
                media_types.append(mime)
        return media_urls, media_types, attachment_info

    @staticmethod
    def _append_attachment_info(text: str, attachment_info: str) -> str:
        info = (attachment_info or "").strip()
        if not info:
            return text
        return f"{text}\n\n{info}".strip() if text.strip() else info

    async def _on_message(self, event_type: str, d: Any) -> None:
        if not isinstance(d, dict):
            return
        msg_id = str(d.get("id") or "")
        if not msg_id or self._is_duplicate(msg_id):
            return
        content = str(d.get("content") or "").strip()
        raw_author = d.get("author")
        author: dict[str, Any] = raw_author if isinstance(raw_author, dict) else {}
        timestamp = self._parse_timestamp(str(d.get("timestamp") or ""))

        if event_type == "C2C_MESSAGE_CREATE":
            user_openid = str(author.get("user_openid") or author.get("id") or "").strip()
            if user_openid and self._dm_allowed(user_openid):
                media_urls, media_types, attachment_info = await self._process_event_media(d, content)
                await self._emit_event(
                    "c2c",
                    user_openid,
                    user_openid,
                    self._append_attachment_info(content, attachment_info),
                    msg_id,
                    timestamp,
                    raw_payload=d,
                    media_urls=media_urls,
                    media_types=media_types,
                    message_type=self._detect_message_type(media_urls, media_types),
                )
            elif user_openid:
                logger.info("QQBot Native ignored C2C message from unallowed openid=%s", user_openid)
            return

        if event_type == "GROUP_AT_MESSAGE_CREATE":
            group_openid = str(d.get("group_openid") or "").strip()
            member_openid = str(author.get("member_openid") or author.get("user_openid") or "").strip()
            if group_openid and self._group_allowed(group_openid):
                media_urls, media_types, attachment_info = await self._process_event_media(d, content)
                await self._emit_event(
                    "group",
                    group_openid,
                    member_openid,
                    self._append_attachment_info(self._strip_at_mention(content), attachment_info),
                    msg_id,
                    timestamp,
                    raw_payload=d,
                    media_urls=media_urls,
                    media_types=media_types,
                    message_type=self._detect_message_type(media_urls, media_types),
                )
            elif group_openid:
                logger.info(
                    "QQBot Native ignored group message from unallowed group_openid=%s member_openid=%s",
                    group_openid,
                    member_openid,
                )
            return

        if event_type in {"GUILD_MESSAGE_CREATE", "GUILD_AT_MESSAGE_CREATE"}:
            channel_id = str(d.get("channel_id") or "").strip()
            guild_id = str(d.get("guild_id") or "").strip()
            user_id = str(author.get("id") or "").strip()
            if channel_id and self._group_allowed(guild_id or channel_id):
                media_urls, media_types, attachment_info = await self._process_event_media(d, content)
                await self._emit_event(
                    "guild",
                    channel_id,
                    user_id,
                    self._append_attachment_info(content, attachment_info),
                    msg_id,
                    timestamp,
                    guild_id=guild_id,
                    raw_payload=d,
                    media_urls=media_urls,
                    media_types=media_types,
                    message_type=self._detect_message_type(media_urls, media_types),
                )
            elif channel_id:
                logger.info(
                    "QQBot Native ignored guild message from unallowed guild_id=%s channel_id=%s user_id=%s",
                    guild_id,
                    channel_id,
                    user_id,
                )
            return

        if event_type == "DIRECT_MESSAGE_CREATE":
            guild_id = str(d.get("guild_id") or "").strip()
            user_id = str(author.get("id") or "").strip()
            if guild_id and self._dm_allowed(user_id):
                media_urls, media_types, attachment_info = await self._process_event_media(d, content)
                await self._emit_event(
                    "dm",
                    guild_id,
                    user_id,
                    self._append_attachment_info(content, attachment_info),
                    msg_id,
                    timestamp,
                    raw_payload=d,
                    media_urls=media_urls,
                    media_types=media_types,
                    message_type=self._detect_message_type(media_urls, media_types),
                )
            elif guild_id or user_id:
                logger.info(
                    "QQBot Native ignored direct message from unallowed guild_id=%s user_id=%s",
                    guild_id,
                    user_id,
                )

    async def _emit_event(
        self,
        chat_kind: str,
        chat_id: str,
        user_id: str,
        text: str,
        message_id: str,
        timestamp: datetime | None,
        *,
        guild_id: str | None = None,
        raw_payload: dict[str, Any] | None = None,
        media_urls: list[str] | None = None,
        media_types: list[str] | None = None,
        message_type: MessageType | None = None,
    ) -> None:
        media_urls = media_urls or []
        media_types = media_types or []
        text = self._strip_rich_media_placeholders(text).strip()
        if not text.strip() and not media_urls:
            if chat_kind == "group":
                text = "hi"
            else:
                return
        self._remember_message_id(chat_id, message_id)
        self._chat_type_map[chat_id] = chat_kind
        role = _role_for_user(user_id, self._owners, self._admins)
        if role == "user" and self._is_slash_command(text):
            logger.info(
                "QQBot Native blocked slash command from ordinary user: chat=%s user=%s command=%s",
                chat_id,
                user_id,
                text.split(maxsplit=1)[0][:80],
            )
            await self.send(chat_id, "普通用户不能使用指令，请直接用普通聊天提问。", reply_to=message_id)
            return
        visible = text
        is_group_like = chat_kind in {"group", "guild"}
        # Keep slash commands at column 0 so Hermes core can route them through
        # its command handler.  Normal group messages still get a model-visible
        # speaker prefix; slash commands carry identity through source/user_id
        # and the channel permission prompt below.
        if is_group_like and not text.lstrip().startswith("/"):
            visible = f"{_sender_identity_label(role, user_id)} {text}"
        source = self.build_source(
            chat_id=chat_id,
            user_id=user_id,
            chat_type="dm" if chat_kind in {"c2c", "dm"} else "group",
            guild_id=guild_id,
            message_id=message_id,
        )
        role_zh = {"owner": "所有者", "admin": "管理员", "user": "普通用户"}.get(role, "普通用户")
        identity_prompt = (
            f"当前发言者身份：{role_zh}；QQBot Native OpenID:{user_id}。"
            "仅此 OpenID 对应当前发言者；不要把 owner 画像/记忆套到其他发言者。"
        )
        event = MessageEvent(
            source=source,
            text=visible,
            message_type=message_type or self._detect_message_type(media_urls, media_types),
            raw_message={"chat_kind": chat_kind, "payload": raw_payload or {}},
            message_id=message_id,
            media_urls=media_urls,
            media_types=media_types,
            timestamp=timestamp,
            channel_prompt=identity_prompt,
        )
        await self.handle_message(event)

    @staticmethod
    def _strip_at_mention(content: str) -> str:
        return re.sub(r"<@!?\d+>\s*", "", content or "").strip()

    @staticmethod
    def _strip_rich_media_placeholders(content: str) -> str:
        text = content or ""
        text = re.sub(r"<faceType=\d+,[^>]*>", "", text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _is_slash_command(content: str) -> bool:
        return bool(re.match(r"^\s*/[A-Za-z][\w-]*(?:\s|$)", content or ""))

    @staticmethod
    def _parse_timestamp(value: str) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _dm_allowed(self, user_id: str) -> bool:
        if not user_id:
            return False
        if self._dm_policy == "disabled":
            return False
        if self._dm_policy == "open":
            return _as_bool(os.getenv("QQBOT_NATIVE_ALLOW_ALL_USERS") or os.getenv("GATEWAY_ALLOW_ALL_USERS"), False)
        if self._dm_policy == "allowlist":
            return user_id in self._allow_from or user_id in self._owners or user_id in self._admins
        if self._dm_policy == "pairing":
            return True
        return False

    def _group_allowed(self, chat_id: str) -> bool:
        if not chat_id:
            return False
        if self._group_policy == "disabled":
            return False
        if self._group_policy == "open":
            return True
        if self._group_policy == "allowlist":
            return chat_id in self._group_allow_chats
        return False

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: Optional[dict] = None) -> SendResult:
        del metadata
        if not content or not content.strip():
            return SendResult(success=True)
        chunks = self.truncate_message(self.format_message(content), self.MAX_MESSAGE_LENGTH)
        last = SendResult(success=False, error="no chunks")
        for chunk in chunks:
            last = await self._send_one(chat_id, chunk, reply_to)
            if not last.success:
                return last
            reply_to = None
        return last

    async def _send_one(self, chat_id: str, content: str, reply_to: Optional[str]) -> SendResult:
        kind = self._chat_type_map.get(chat_id)
        if not kind:
            if chat_id.startswith("group:") or chat_id in self._group_allow_chats:
                kind = "group"
            else:
                kind = "c2c"
        bare_chat_id = chat_id.removeprefix("group:")
        try:
            if kind == "guild":
                body = {"content": content[: self.MAX_MESSAGE_LENGTH]}
                if reply_to:
                    body["msg_id"] = reply_to
                data = await self._api_request("POST", f"/channels/{bare_chat_id}/messages", body)
            elif kind == "dm":
                return SendResult(
                    success=False,
                    error="QQBot Native guild DM sending is not implemented yet; use C2C or group/guild channel messages.",
                    retryable=False,
                )
            else:
                effective_reply_to = reply_to or self._recent_reply_anchor(bare_chat_id)
                body = self._build_text_body(content, effective_reply_to, markdown=(self._markdown_support and kind != "guild") or (kind == "guild" and self._guild_markdown_support))
                path = f"/v2/groups/{bare_chat_id}/messages" if kind == "group" else f"/v2/users/{bare_chat_id}/messages"
                data = await self._api_request("POST", path, body)
            return SendResult(success=True, message_id=str(data.get("id") or uuid.uuid4().hex[:12]), raw_response=data)
        except Exception as exc:
            logger.error("QQBot Native send failed: %s", exc)
            return SendResult(success=False, error=str(exc), retryable=True)

    def _build_text_body(self, content: str, reply_to: Optional[str], *, markdown: bool) -> dict[str, Any]:
        body: dict[str, Any]
        if markdown:
            body = {
                "msg_type": MSG_TYPE_MARKDOWN,
                "markdown": {"content": content[: self.MAX_MESSAGE_LENGTH]},
                "msg_seq": self._next_msg_seq(reply_to or "default"),
            }
        else:
            body = {
                "msg_type": MSG_TYPE_TEXT,
                "content": content[: self.MAX_MESSAGE_LENGTH],
                "msg_seq": self._next_msg_seq(reply_to or "default"),
            }
        if reply_to:
            body["msg_id"] = reply_to
        return body

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict] = None,
        **kwargs: Any,
    ) -> SendResult:
        """Send a document through official QQBot RichMedia."""
        del metadata, kwargs
        try:
            kind, bare_chat_id = self._resolve_outbound_kind(chat_id)
            if kind == "guild":
                return SendResult(
                    success=False,
                    error="QQBot Native document send is not supported for QQ guild/channel chats.",
                    retryable=False,
                )
            if kind == "dm":
                return SendResult(
                    success=False,
                    error="QQBot Native guild DM document sending is not implemented; use C2C or group chats.",
                    retryable=False,
                )

            effective_reply_to = reply_to or self._recent_reply_anchor(bare_chat_id)
            upload = await self._upload_document(kind, bare_chat_id, file_path, file_name=file_name)
            file_info = upload.get("file_info") or (upload.get("data") or {}).get("file_info")
            if not file_info:
                return SendResult(success=False, error=f"QQBot Native file upload returned no file_info: {upload}")

            body: dict[str, Any] = {
                "msg_type": MSG_TYPE_MEDIA,
                "media": {"file_info": file_info},
                "msg_seq": self._next_msg_seq(effective_reply_to or bare_chat_id),
            }
            if caption:
                body["content"] = caption[: self.MAX_MESSAGE_LENGTH]
            if effective_reply_to:
                body["msg_id"] = effective_reply_to

            path = f"/v2/groups/{bare_chat_id}/messages" if kind == "group" else f"/v2/users/{bare_chat_id}/messages"
            data = await self._api_request("POST", path, body)
            return SendResult(success=True, message_id=str(data.get("id") or uuid.uuid4().hex[:12]), raw_response=data)
        except Exception as exc:
            logger.error("QQBot Native document send failed: %s", exc)
            return SendResult(success=False, error=str(exc), retryable=False)

    def _resolve_outbound_kind(self, chat_id: str) -> tuple[str, str]:
        kind = self._chat_type_map.get(chat_id)
        if not kind:
            if chat_id.startswith("group:") or chat_id in self._group_allow_chats:
                kind = "group"
            else:
                kind = "c2c"
        return kind, str(chat_id).removeprefix("group:")

    async def _upload_document(self, kind: str, chat_id: str, source: str, *, file_name: Optional[str] = None) -> dict[str, Any]:
        if kind not in {"c2c", "group"}:
            raise ValueError(f"QQBot Native document upload does not support chat kind {kind!r}")
        src = str(source or "").strip()
        if not src:
            raise ValueError("file_path is required")
        body: dict[str, Any] = {
            "file_type": MEDIA_TYPE_FILE,
            "srv_send_msg": False,
        }
        if self._is_url(src):
            body["url"] = src
            resolved_name = file_name or Path(src.split("?", 1)[0]).name or "file"
        else:
            local_path = Path(src).expanduser()
            if not local_path.is_absolute():
                local_path = (Path.cwd() / local_path).resolve()
            if not local_path.exists() or not local_path.is_file():
                raise FileNotFoundError(f"file not found: {local_path}")
            size = local_path.stat().st_size
            resolved_name = file_name or local_path.name
            if size > MAX_INLINE_FILE_BYTES:
                return await self._upload_document_chunked(kind, chat_id, local_path, resolved_name)
            body["file_data"] = base64.b64encode(local_path.read_bytes()).decode("ascii")
        body["file_name"] = resolved_name or "file"
        path = f"/v2/groups/{chat_id}/files" if kind == "group" else f"/v2/users/{chat_id}/files"
        return await self._api_request("POST", path, body)

    async def _upload_document_chunked(self, kind: str, chat_id: str, local_path: Path, file_name: str) -> dict[str, Any]:
        try:
            module = __import__("gateway.platforms.qqbot.chunked_upload", fromlist=["ChunkedUploader"])
            ChunkedUploader = getattr(module, "ChunkedUploader")
        except Exception as exc:
            raise RuntimeError("Hermes QQBot chunked uploader is unavailable; cannot upload large local files") from exc

        uploader = ChunkedUploader(
            api_request=self._chunked_upload_api_request,
            http_put=self._chunked_upload_put,
            log_tag="QQBot Native",
        )
        return await uploader.upload(
            chat_type=kind,
            target_id=chat_id,
            file_path=str(local_path),
            file_type=MEDIA_TYPE_FILE,
            file_name=file_name,
        )

    async def _chunked_upload_api_request(self, method: str, path: str, body: Optional[dict] = None, timeout: float = FILE_UPLOAD_TIMEOUT) -> dict[str, Any]:
        del timeout
        return await self._api_request(method, path, body)

    async def _chunked_upload_put(self, url: str, data: bytes, headers: Optional[dict] = None, timeout: float = FILE_UPLOAD_TIMEOUT) -> Any:
        if not self._session:
            self._session = aiohttp.ClientSession(trust_env=True)
        async with self._session.put(
            url,
            data=data,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            text = await resp.text()
            return type("QQBotNativePutResponse", (), {"status_code": resp.status, "text": text})()

    @staticmethod
    def _is_url(source: str) -> bool:
        return str(source or "").lower().startswith(("http://", "https://"))

    @staticmethod
    def _next_msg_seq(seed: str) -> int:
        return (int(time.time()) ^ int(uuid.uuid5(uuid.NAMESPACE_URL, str(seed)).hex[:4], 16)) % 65536

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        kind = self._chat_type_map.get(chat_id, "unknown")
        return {"name": chat_id, "type": kind, "chat_id": chat_id}

    def format_message(self, content: str) -> str:
        # Keep Markdown intact: native QQBot Markdown is the point of this adapter.
        return content or ""


def validate_config(config: PlatformConfig) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(
        str(extra.get("app_id") or os.getenv("QQBOT_NATIVE_APP_ID") or os.getenv("QQ_APP_ID", "")).strip()
        and str(extra.get("client_secret") or os.getenv("QQBOT_NATIVE_CLIENT_SECRET") or os.getenv("QQ_CLIENT_SECRET", "")).strip()
    )


def _apply_yaml_config(_yaml_cfg: dict, platform_cfg: dict) -> dict:
    seeded: dict[str, Any] = {}
    extra = platform_cfg.get("extra") if isinstance(platform_cfg, dict) else None
    if isinstance(extra, dict):
        seeded.update(extra)
    for key in (
        "app_id", "client_secret", "markdown_support", "guild_markdown_support",
        "owners", "owner", "admins", "dm_policy", "allow_from", "group_policy",
        "group_allow_chats", "group_allow_from", "group_sessions_per_user",
        "require_mention", "group_require_mention",
    ):
        if isinstance(platform_cfg, dict) and key in platform_cfg:
            seeded[key] = platform_cfg[key]
    return seeded


def _env_enablement() -> dict[str, Any]:
    """Expose env-only QQBot Native settings to gateway/cron config loading."""
    seeded: dict[str, Any] = {}
    if os.getenv("QQBOT_NATIVE_APP_ID"):
        seeded["app_id"] = os.getenv("QQBOT_NATIVE_APP_ID")
    if os.getenv("QQBOT_NATIVE_CLIENT_SECRET"):
        seeded["client_secret"] = os.getenv("QQBOT_NATIVE_CLIENT_SECRET")
    home = os.getenv("QQBOT_NATIVE_HOME_CHANNEL", "").strip()
    if home:
        seeded["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("QQBOT_NATIVE_HOME_CHANNEL_NAME", "QQBot Native Home"),
            "thread_id": os.getenv("QQBOT_NATIVE_HOME_CHANNEL_THREAD_ID", "").strip() or None,
        }
    return seeded


def register_qqbot_native(ctx) -> None:
    ctx.register_hook("pre_tool_call", qqbot_native_acl_pre_tool_call)
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="QQBot Native (plugin)",
        adapter_factory=lambda cfg: QQBotNativeAdapter(cfg),
        check_fn=check_qqbot_native_requirements,
        validate_config=validate_config,
        env_enablement_fn=_env_enablement,
        install_hint="aiohttp is required (bundled with Hermes gateway installs)",
        emoji="🤖",
        allowed_users_env="QQBOT_NATIVE_ALLOWED_USERS",
        allow_all_env="QQBOT_NATIVE_ALLOW_ALL_USERS",
        cron_deliver_env_var="QQBOT_NATIVE_HOME_CHANNEL",
        max_message_length=MAX_MESSAGE_LENGTH,
        platform_hint=QQBOT_NATIVE_PLATFORM_PROMPT,
        apply_yaml_config_fn=_apply_yaml_config,
    )
