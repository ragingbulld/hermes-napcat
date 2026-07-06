"""NapCat (OneBot 11 reverse WebSocket) platform adapter for Hermes Agent.

Installed as a user plugin at:
    ~/.hermes/plugins/hermes-napcat/adapter.py

Configuration in ~/.hermes/config.yaml:

    platforms:
      napcat:
        enabled: true
        extra:
          http_api: "http://127.0.0.1:18801"
          access_token: ""
          self_id: "123456789"
          ws_port: 18800
          owners: []                 # QQ numbers with full owner permission
          admins: []                 # QQ numbers allowed to run dangerous operations
          group_allow_chats: []      # group IDs where ordinary users may trigger Hermes
          require_mention: true      # group messages must @ the bot
          processing_emoji: true     # react before processing accepted messages
          processing_emoji_id: "307" # QQ emoji ID, 307 = /喵喵
          post_response_emoji: true  # react again after a successful reply
          post_response_emoji_id: "478" # 用户确认的“对的对的” reaction ID
          private_typing_status: true # show “正在输入中” while preparing DM replies
          private_typing_event_type: 1
          private_typing_interval: 5
          private_typing_max_seconds: 120
          poke_after_response: false # do not poke after a successful reply
          admins: []                 # QQ numbers that can use admin-only tools
          media_max_mb: 5
"""
from __future__ import annotations

import asyncio
from contextlib import suppress
import json
import logging
import os
import re
import subprocess
import tempfile
from datetime import datetime
from typing import Any, Optional

import aiohttp
import aiohttp.web

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
    cache_image_from_bytes,
)
from gateway.config import Platform, PlatformConfig
from gateway.session import SessionSource, build_session_key

from .napcat_api import (
    call_onebot_api,
    get_login_info,
    get_msg,
    image_segment,
    record_segment,
    reply_segment,
    send_group_msg,
    send_private_msg,
    set_input_status,
    text_segment,
    upload_group_file,
    upload_private_file,
    video_segment,
)

logger = logging.getLogger(__name__)

_QQ_TEXT_LIMIT = 4500
_AUDIO_EXTS = {".mp3", ".opus", ".ogg", ".wav", ".flac", ".m4a", ".aac", ".silk", ".amr"}
_VIDEO_EXTS = {".mp4", ".avi", ".mkv", ".mov", ".webm", ".flv", ".wmv"}
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".ico", ".svg"}

_OWNER_ONLY_TOOLS = {
    "memory", "fact_store", "fact_feedback",
}
_USER_ALLOWED_TOOLS = {
    "web_search", "web_extract", "vision_analyze",
}


def _as_bool(value: Any, default: bool = False) -> bool:
    """Coerce bool-ish config values without treating arbitrary strings as true."""
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


def _load_acl_config() -> tuple[set[str], set[str], set[str]]:
    """Return (owners, admins, group_allow_chats) from config.yaml."""
    try:
        from hermes_cli.config import load_config

        cfg = load_config() or {}
    except Exception:
        cfg = {}
    platform_cfg = ((cfg.get("platforms") or {}).get("napcat") or {})
    extra = platform_cfg.get("extra") or {}
    owners = set(_as_str_list(extra.get("owners") or extra.get("owner")))
    admins = set(_as_str_list(extra.get("admins")))
    groups = {
        str(x).removeprefix("group:")
        for x in _as_str_list(extra.get("group_allow_chats"))
    }
    return owners, admins, groups


def _role_for_user(user_id: str, owners: set[str], admins: set[str]) -> str:
    user_id = str(user_id or "")
    if user_id and user_id in owners:
        return "owner"
    if user_id and user_id in admins:
        return "admin"
    return "user"


def _safe_identity_part(value: str, *, max_len: int = 32) -> str:
    """Keep sender labels compact and bracket-safe for gateway prefixes."""
    cleaned = re.sub(r"[\r\n\t]+", " ", str(value or "")).strip()
    cleaned = cleaned.replace("[", "［").replace("]", "］")
    cleaned = re.sub(r"\s+", " ", cleaned)
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 1] + "…"
    return cleaned


def _sender_identity_label(role: str, user_id: str, display_name: str) -> str:
    """Return the model-visible sender label used by shared group sessions.

    QQ nicknames/cards are user-controlled and can collide.  Prefixing the
    stable QQ number and NapCat role helps the model avoid mixing up speakers
    and avoid attempting tools for ordinary group users.
    """
    role_label = {"owner": "owner", "admin": "admin", "user": "user"}.get(role, "user")
    qq = _safe_identity_part(user_id, max_len=20) or "unknown"
    name = _safe_identity_part(display_name, max_len=32)
    return f"{role_label} QQ:{qq} {name}" if name else f"{role_label} QQ:{qq}"


def _napcat_acl_pre_tool_call(tool_name: str, **_: Any) -> dict | None:
    """Hard tool permission gate for NapCat sessions."""
    try:
        from gateway.session_context import get_session_env

        platform = get_session_env("HERMES_SESSION_PLATFORM", "")
        user_id = get_session_env("HERMES_SESSION_USER_ID", "")
    except Exception:
        return None
    if platform != "napcat":
        return None

    owners, admins, _ = _load_acl_config()
    role = _role_for_user(user_id, owners, admins)
    if role == "owner":
        return None
    if role == "admin":
        if tool_name in _OWNER_ONLY_TOOLS:
            return {
                "action": "block",
                "message": f"NapCat 权限不足：工具 {tool_name} 仅 owner 可用。",
            }
        return None
    if tool_name not in _USER_ALLOWED_TOOLS:
        return {
            "action": "block",
            "message": f"NapCat 权限不足：普通用户不能调用工具 {tool_name}。",
        }
    return None

# ── Markdown → QQ plain-text ──────────────────────────────────────────────────

def _strip_markdown(text: str) -> str:
    """Convert Markdown to clean QQ-friendly plain text.

    QQ does not render Markdown; raw syntax like **bold** or ## heading
    appears as literal characters.  This function converts the most common
    constructs to readable Unicode equivalents.
    """
    lines = text.splitlines()
    out: list[str] = []
    in_code = False
    code_lang = ""
    code_lines: list[str] = []

    for line in lines:
        # ── fenced code blocks ────────────────────────────────────────────
        fence = re.match(r"^(`{3,}|~{3,})(.*)", line.strip())
        if fence:
            if not in_code:
                in_code = True
                code_lang = fence.group(2).strip()
                code_lines = []
            else:
                in_code = False
                block = "\n".join(code_lines)
                label = f"[{code_lang}]" if code_lang else "[代码]"
                out.append(f"┌─{label}─")
                for cl in code_lines:
                    out.append("│ " + cl)
                out.append("└──────")
                code_lines = []
            continue
        if in_code:
            code_lines.append(line)
            continue

        # ── headings ──────────────────────────────────────────────────────
        h = re.match(r"^(#{1,6})\s+(.*)", line)
        if h:
            level, title = len(h.group(1)), h.group(2).strip()
            title = _inline(title)
            if level <= 2:
                out.append(f"【{title}】")
            else:
                out.append(f"▌ {title}")
            continue

        # ── horizontal rules ──────────────────────────────────────────────
        if re.match(r"^\s*[-*_]{3,}\s*$", line):
            out.append("────────────────")
            continue

        # ── blockquotes ───────────────────────────────────────────────────
        bq = re.match(r"^>\s?(.*)", line)
        if bq:
            out.append("「" + _inline(bq.group(1)) + "」")
            continue

        # ── unordered lists ───────────────────────────────────────────────
        ul = re.match(r"^(\s*)[-*+]\s+(.*)", line)
        if ul:
            indent = len(ul.group(1)) // 2
            out.append("  " * indent + "• " + _inline(ul.group(2)))
            continue

        # ── ordered lists ─────────────────────────────────────────────────
        ol = re.match(r"^(\s*)\d+[.)]\s+(.*)", line)
        if ol:
            indent = len(ol.group(1)) // 2
            num = re.match(r"^\s*(\d+)", line).group(1)
            out.append("  " * indent + num + ". " + _inline(ol.group(2)))
            continue

        # ── table rows ────────────────────────────────────────────────────
        if re.match(r"^\s*\|", line):
            # Skip separator rows (|---|---|)
            if re.match(r"^\s*\|[\s\-:|]+\|\s*$", line):
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            out.append("  ".join(_inline(c) for c in cells if c))
            continue

        # ── normal line ───────────────────────────────────────────────────
        out.append(_inline(line))

    return "\n".join(out).strip()


def _inline(text: str) -> str:
    """Strip inline Markdown from a single line."""
    # inline code: `code`
    text = re.sub(r"`([^`\n]+)`", r"\1", text)
    # bold+italic: ***text*** or ___text___
    text = re.sub(r"\*{3}(.+?)\*{3}", r"\1", text)
    text = re.sub(r"_{3}(.+?)_{3}", r"\1", text)
    # bold: **text** or __text__
    text = re.sub(r"\*{2}(.+?)\*{2}", r"\1", text)
    text = re.sub(r"_{2}(.+?)_{2}", r"\1", text)
    # italic: *text* or _text_  (only word-boundary _ to avoid false positives)
    text = re.sub(r"\*(.+?)\*", r"\1", text)
    text = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"\1", text)
    # strikethrough: ~~text~~
    text = re.sub(r"~~(.+?)~~", r"\1", text)
    # links: [text](url)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1（\2）", text)
    # images: ![alt](url)
    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"[\1]", text)
    # bare reference-style links: [text][ref]
    text = re.sub(r"\[([^\]]+)\]\[[^\]]*\]", r"\1", text)
    return text


def _file_ext(url: str) -> str:
    path = url.split("?")[0]
    dot = path.rfind(".")
    return path[dot:].lower() if dot != -1 else ""


def _classify_media(url: str) -> str:
    ext = _file_ext(url)
    if ext in _AUDIO_EXTS:
        return "audio"
    if ext in _VIDEO_EXTS:
        return "video"
    if ext in _IMAGE_EXTS:
        return "image"
    return "file"


def _extract_text(segments: list[dict]) -> str:
    parts = []
    for s in segments:
        if s["type"] == "text":
            parts.append(s["data"].get("text", ""))
        elif s["type"] == "at":
            parts.append(f"@{s['data'].get('qq', '')}")
    return "".join(parts).strip()


def _extract_images(segments: list[dict]) -> list[str]:
    return [
        s["data"].get("url") or s["data"].get("file", "")
        for s in segments if s["type"] == "image"
        if s["data"].get("url") or s["data"].get("file")
    ]


def _extract_record(segments: list[dict]) -> str | None:
    for s in segments:
        if s["type"] == "record":
            return s["data"].get("url") or s["data"].get("file")
    return None


def _extract_reply_id(segments: list[dict]) -> int | None:
    for s in segments:
        if s["type"] == "reply":
            try:
                return int(s["data"]["id"])
            except (KeyError, ValueError):
                pass
    return None


def _has_bot_mention(segments: list[dict], self_id: str) -> bool:
    return any(
        s["type"] == "at" and str(s["data"].get("qq")) == self_id
        for s in segments
    )


def _strip_bot_mention(segments: list[dict], self_id: str) -> list[dict]:
    return [
        s for s in segments
        if not (s["type"] == "at" and str(s["data"].get("qq")) == self_id)
    ]


def _chunk_text(text: str, limit: int = _QQ_TEXT_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        split = text.rfind("\n", 0, limit)
        if split <= 0:
            split = text.rfind(" ", 0, limit)
        if split <= 0:
            split = limit
        chunks.append(text[:split])
        text = text[split:].lstrip("\n")
    return chunks


async def _download_and_convert_wav(url: str, max_bytes: int) -> str | None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.read()
        if len(data) > max_bytes:
            return None
        fd, in_path = tempfile.mkstemp(suffix=".silk")
        os.close(fd)
        out_path = in_path.replace(".silk", ".wav")
        with open(in_path, "wb") as f:
            f.write(data)
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", in_path, "-ar", "16000", "-ac", "1", "-f", "wav", out_path],
            capture_output=True, timeout=15,
        )
        os.unlink(in_path)
        if result.returncode != 0:
            return None
        return out_path
    except Exception as exc:
        logger.debug("Voice download/convert failed: %s", exc)
        return None


def check_napcat_requirements() -> bool:
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        return False


class NapCatAdapter(BasePlatformAdapter):
    """Hermes platform adapter for QQ via NapCat (OneBot 11 reverse WebSocket).

    NapCat dials **out** to the WS server we start here; we reply via
    NapCat's HTTP API.
    """

    @property
    def enforces_own_access_policy(self) -> bool:
        """NapCat gates messages before dispatch.

        Group messages must come from a configured group_allow_chats entry;
        private messages must come from owner/admin QQ numbers.
        """
        return True

    @property
    def authorization_is_upstream(self) -> bool:
        """Treat messages that reach the gateway as already NapCat-authorized.

        The generic gateway env allowlists are sender-scoped and would block
        ordinary members in an allowlisted QQ group.  NapCat needs a mixed
        policy instead: group allowlist is chat-scoped and DMs are owner/admin
        only.  _process_message() enforces that before calling handle_message(),
        so the gateway should not apply a second sender allowlist to NapCat.
        """
        return True

    def __init__(self, config: PlatformConfig) -> None:
        super().__init__(config, Platform("napcat"))
        extra: dict[str, Any] = getattr(config, "extra", {}) or {}

        self._http_api: str = extra.get("http_api", "").rstrip("/")
        self._access_token: str = extra.get("access_token", "") or ""
        raw_self_id = str(extra.get("self_id", ""))
        # Treat placeholder values as empty so HTTP probe fills in real QQ
        self._self_id: str = "" if raw_self_id in ("YOUR_QQ_NUMBER", "YOURQQ_NUMBER") else raw_self_id
        self._ws_port: int = int(extra.get("ws_port", 18800))
        self._owners: list[str] = _as_str_list(extra.get("owners") or extra.get("owner"))
        self._admins: list[str] = _as_str_list(extra.get("admins"))
        self._group_allow_chats: list[str] = [
            str(x).removeprefix("group:")
            for x in _as_str_list(
                extra.get("group_allow_chats")
                or extra.get("group_allowed_chats")
                or extra.get("allowed_groups")
            )
        ]
        # Group trigger policy. Current deployment wants group messages to
        # mention the bot before Hermes replies; DMs still use sender allowlist.
        self._require_mention: bool = _as_bool(
            extra.get("group_require_mention", extra.get("require_mention", False)),
            default=False,
        )
        self._processing_emoji_enabled: bool = _as_bool(
            extra.get("processing_emoji", extra.get("processing_emoji_enabled", True)),
            default=True,
        )
        self._processing_emoji_id: str = str(extra.get("processing_emoji_id", "307"))
        self._post_response_emoji_enabled: bool = _as_bool(
            extra.get("post_response_emoji", extra.get("post_response_emoji_enabled", False)),
            default=False,
        )
        self._post_response_emoji_id: str = str(extra.get("post_response_emoji_id", "478"))
        self._private_typing_enabled: bool = _as_bool(
            extra.get("private_typing_status", extra.get("private_typing_enabled", True)),
            default=True,
        )
        self._private_typing_event_type: int = int(extra.get("private_typing_event_type", 1))
        self._private_typing_interval: float = max(
            1.0, float(extra.get("private_typing_interval", 5))
        )
        self._private_typing_max_seconds: float = max(
            0.0, float(extra.get("private_typing_max_seconds", 120))
        )
        self._poke_after_response: bool = _as_bool(extra.get("poke_after_response", False), default=False)
        self._media_max_mb: int = int(extra.get("media_max_mb", 5))

        self._runner: aiohttp.web.AppRunner | None = None
        self._active_ws: set[aiohttp.web.WebSocketResponse] = set()
        # Maps original incoming message_id -> (sender QQ, group_id or "").
        # send() consumes this after it successfully sends the reply, so the
        # post-processing poke happens after the visible answer is delivered.
        self._post_reply_pokes: dict[str, tuple[str, str]] = {}
        # Original incoming message IDs that should receive a second reaction
        # after Hermes has successfully sent its visible reply.
        self._post_reply_reactions: set[str] = set()

        # Wire up qq_tool so the agent can call QQ APIs directly
        try:
            from . import qq_tool as _qq_tool
            _qq_tool._init(self._http_api, self._access_token, self._owners, self._admins)
        except ImportError:
            pass

    # ── Connection ─────────────────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self._http_api:
            logger.error("NapCat: http_api is not configured")
            return False

        app = aiohttp.web.Application()
        app.router.add_get("/", self._ws_handler)
        self._runner = aiohttp.web.AppRunner(app)
        await self._runner.setup()
        site = aiohttp.web.TCPSite(self._runner, "0.0.0.0", self._ws_port)
        await site.start()
        self._is_connected = True
        logger.info("NapCat: reverse WS listening on ws://0.0.0.0:%d", self._ws_port)

        try:
            info = await get_login_info(self._http_api, self._access_token or None)
            if not self._self_id:
                self._self_id = str(info.get("user_id", ""))
            logger.info(
                "NapCat: bot is %s (QQ:%s)",
                info.get("nickname", "?"), info.get("user_id", "?"),
            )
        except Exception as exc:
            logger.warning("NapCat: HTTP probe failed (WS still running): %s", exc)

        return True

    async def disconnect(self) -> None:
        self._is_connected = False
        for ws in list(self._active_ws):
            await ws.close()
        self._active_ws.clear()
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        logger.info("NapCat: disconnected")

    # ── Inbound WS handler ─────────────────────────────────────────────────

    async def _ws_handler(self, request: aiohttp.web.Request) -> aiohttp.web.WebSocketResponse:
        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(request)
        self._active_ws.add(ws)
        logger.info("NapCat WS connected from %s", request.remote)
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    asyncio.create_task(self._handle_raw(msg.data))
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                    break
        finally:
            self._active_ws.discard(ws)
            logger.info("NapCat WS disconnected")
        return ws

    async def _handle_raw(self, raw: str) -> None:
        try:
            data: dict = json.loads(raw)
        except json.JSONDecodeError:
            return
        if data.get("post_type") != "message":
            return
        try:
            await self._process_message(data)
        except Exception:
            logger.exception("NapCat: error processing message")

    async def _process_message(self, event: dict) -> None:
        is_group = event.get("message_type") == "group"
        sender_id = str(event.get("user_id", ""))
        sender = event.get("sender", {})
        sender_name: str = sender.get("card") or sender.get("nickname") or sender_id
        group_id = str(event.get("group_id", "")) if is_group else ""
        chat_id = f"group:{group_id}" if is_group else sender_id
        segments: list[dict] = event.get("message", [])

        # Group mention handling. This deployment gates groups by both sender
        # allowlist and @ mention; DMs still use only the sender allowlist.
        # Set require_mention/group_require_mention false to allow whitelisted
        # users to trigger the bot in groups without @.
        if is_group:
            mentioned = bool(self._self_id and _has_bot_mention(segments, self._self_id))
            if self._require_mention and self._self_id and not mentioned:
                return
            if self._self_id and mentioned:
                segments = _strip_bot_mention(segments, self._self_id)

        # Authorization. Permissions are QQ-number based only. Display names are
        # intentionally ignored because they are user-controlled and spoofable.
        # - private chats: owner/admin only
        # - group chats: group must be allowlisted; ordinary users are accepted
        #   only inside those groups
        owners = set(self._owners)
        admins = set(self._admins)
        role = _role_for_user(sender_id, owners, admins)
        identity_label = _sender_identity_label(role, sender_id, sender_name)
        if is_group:
            if not self._group_allow_chats or group_id not in self._group_allow_chats:
                return
        else:
            if role not in {"owner", "admin"}:
                return

        # Mirror the gateway's own authorization check before adding QQ
        # reactions.  The adapter's chat allowlist is intentionally coarse
        # (which groups may reach Hermes), while GatewayRunner may still reject
        # a specific sender via allow_from/group_allow_from/env ACLs.  If we add
        # processing/done reactions before that second layer, an unauthorized
        # message can look "completed" even though Hermes sent no reply.
        upstream_authorized = self._is_sender_authorized(
            sender_id,
            "group" if is_group else "dm",
            chat_id,
        )
        if upstream_authorized is False:
            return

        text = _extract_text(segments)
        image_urls = _extract_images(segments)
        record_url = _extract_record(segments)

        # In group chats Hermes gateway already prefixes stored messages with
        # source.user_name so shared group sessions can distinguish speakers.
        # Use a stable identity label (role + QQ + nickname), not just a mutable
        # group card. Do not add another adapter-level prefix here, otherwise
        # Desktop history shows duplicate prefixes. Some QQ / NapCat event paths
        # already include a prefix in the extracted text; strip that copy and let
        # the gateway add exactly one display prefix. Keep slash commands
        # starting with "/" so the gateway command parser still recognizes them.
        if is_group and text:
            stripped_text = text.lstrip()
            if stripped_text.startswith("/"):
                text = stripped_text
            else:
                # Strip legacy/raw nickname prefixes if NapCat text already
                # includes one.  The canonical prefix is now source.user_name,
                # which includes role + QQ number + nickname.
                for prefix_name in (sender_name, identity_label):
                    sender_prefix = f"[{prefix_name}]"
                    if stripped_text.startswith(f"{sender_prefix}:"):
                        text = stripped_text[len(sender_prefix) + 1:].lstrip()
                        break
                    if stripped_text.startswith(sender_prefix):
                        text = stripped_text[len(sender_prefix):].lstrip()
                        break
                else:
                    text = stripped_text

        # Fetch quoted message text for reply context
        reply_id = _extract_reply_id(event.get("message", []))
        reply_text: str | None = None
        if reply_id:
            try:
                quoted = await get_msg(self._http_api, reply_id, self._access_token or None)
                q_sender = quoted.get("sender", {})
                q_user_id = str(q_sender.get("user_id", "") or "")
                q_name = (
                    q_sender.get("card")
                    or q_sender.get("nickname")
                    or q_user_id
                )
                q_role = _role_for_user(q_user_id, owners, admins)
                q_identity = _sender_identity_label(q_role, q_user_id, q_name)
                q_text = _extract_text(quoted.get("message", []))
                if q_text:
                    reply_text = f"[{q_identity}]: {q_text}"
                    text = f"[引用 {q_identity} 的消息: {q_text}]\n{text}"
            except Exception:
                pass

        # Determine MessageType and media
        media_urls: list[str] = []
        media_types: list[str] = []
        msg_type = MessageType.TEXT

        if image_urls:
            msg_type = MessageType.PHOTO
            max_bytes = self._media_max_mb * 1024 * 1024
            for url in image_urls[:1]:  # cache first image for vision tool
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url) as resp:
                            resp.raise_for_status()
                            img_data = await resp.read()
                    if len(img_data) <= max_bytes:
                        cached = cache_image_from_bytes(img_data)
                        media_urls.append(cached)
                        media_types.append("image/jpeg")
                except Exception as exc:
                    logger.debug("NapCat: image download failed: %s", exc)

        elif record_url:
            msg_type = MessageType.VOICE
            max_bytes = self._media_max_mb * 1024 * 1024
            wav = await _download_and_convert_wav(record_url, max_bytes)
            if wav:
                media_urls.append(wav)
                media_types.append("audio/wav")
                logger.debug("NapCat: voice -> %s", wav)

        if not text and not media_urls:
            return

        original_message_id = str(event.get("message_id", "") or "")
        if original_message_id:
            self._remember_post_reply_reaction(original_message_id)
            self._remember_post_reply_poke(original_message_id, sender_id, group_id)
            await self._mark_processing(original_message_id)

        source = SessionSource(
            platform=Platform("napcat"),
            chat_id=chat_id,
            chat_name=sender_name if not is_group else group_id,
            chat_type="group" if is_group else "dm",
            user_id=sender_id,
            user_name=identity_label,
        )

        role_zh = {"owner": "所有者", "admin": "管理员", "user": "普通用户"}.get(role, "普通用户")
        if role == "owner":
            permission_detail = (
                "你拥有 owner 权限：可使用完整本地工具、危险操作、记忆写入和修改用户画像记忆。"
            )
        elif role == "admin":
            permission_detail = (
                "你拥有 admin 权限：可执行危险操作和本地工具；但不能写入长期记忆或修改用户画像记忆。"
            )
        else:
            permission_detail = (
                "你是普通用户：只能普通聊天和使用公开查询/图片理解类工具；不得执行本地命令、读写本机文件、修改配置、写入记忆、修改用户画像记忆或进行 QQ 管理操作。"
            )
        permission_prompt = (
            f"[{role_zh}] QQ:{sender_id}。"
            "权限身份仅按 QQ 号判定，不按昵称/群名片判定。"
            f"{permission_detail}"
            "读取公开网页/回答普通问题可直接处理；涉及越权工具时必须拒绝或让用户联系 owner/admin。"
        )

        message_event = MessageEvent(
            text=text,
            message_type=msg_type,
            source=source,
            raw_message=event,
            message_id=str(event.get("message_id", "")),
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=str(reply_id) if reply_id else None,
            reply_to_text=reply_text,
            timestamp=datetime.fromtimestamp(event["time"]) if event.get("time") else datetime.now(),
            channel_prompt=permission_prompt,
        )

        # Set per-message context so admin-gated tools know who is asking
        try:
            from . import qq_tool as _qq_tool
            _qq_tool._set_context(sender_id, role=role)
        except ImportError:
            pass

        inline_command_completion = self._needs_inline_command_completion_reaction(message_event)

        typing_task: asyncio.Task[None] | None = None
        if not is_group:
            typing_task = self._start_private_typing(sender_id)
        try:
            await self.handle_message(message_event)
            if inline_command_completion:
                await self._clear_processing(message_event.message_id)
                await self._mark_post_response(message_event.message_id)
        finally:
            if typing_task:
                typing_task.cancel()
                with suppress(asyncio.CancelledError):
                    await typing_task

    def _remember_post_reply_poke(self, message_id: str, sender_id: str, group_id: str = "") -> None:
        if not self._poke_after_response or not message_id or not sender_id:
            return
        self._post_reply_pokes[str(message_id)] = (str(sender_id), str(group_id or ""))
        # Bound memory in case a message is accepted but no visible response is sent.
        while len(self._post_reply_pokes) > 256:
            self._post_reply_pokes.pop(next(iter(self._post_reply_pokes)), None)

    def _remember_post_reply_reaction(self, message_id: str) -> None:
        if not self._post_response_emoji_enabled or not message_id:
            return
        self._post_reply_reactions.add(str(message_id))
        while len(self._post_reply_reactions) > 256:
            self._post_reply_reactions.pop()

    def _needs_inline_command_completion_reaction(self, event: MessageEvent) -> bool:
        """Return True for busy-session slash commands handled inline by Hermes core.

        BasePlatformAdapter dispatches commands such as /status directly while a
        session is active, bypassing _process_message_background and therefore
        its on_processing_complete lifecycle hook. NapCat reactions live in that
        hook, so the adapter must finish the reaction swap itself for these
        command replies.
        """
        cmd = event.get_command()
        if not cmd:
            return False
        try:
            from hermes_cli.commands import should_bypass_active_session

            if not should_bypass_active_session(cmd):
                return False
            session_key = build_session_key(
                event.source,
                group_sessions_per_user=self.config.extra.get("group_sessions_per_user", True),
                thread_sessions_per_user=self.config.extra.get("thread_sessions_per_user", False),
            )
            if session_key not in self._active_sessions:
                return False
            try:
                if self._session_task_is_stale(session_key):
                    return False
            except Exception:
                pass
            return True
        except Exception as exc:
            logger.debug("NapCat: inline command reaction check failed: %s", exc)
            return False

    async def _mark_processing(self, message_id: str) -> None:
        """React to the triggering QQ message to show Hermes accepted it."""
        if not self._processing_emoji_enabled or not message_id:
            return
        try:
            await call_onebot_api(
                self._http_api,
                "set_msg_emoji_like",
                {
                    "message_id": int(message_id),
                    "emoji_id": str(self._processing_emoji_id),
                },
                self._access_token or None,
            )
        except Exception as exc:
            logger.debug("NapCat: processing emoji failed for %s: %s", message_id, exc)

    async def _clear_processing(self, message_id: str | None) -> None:
        """Remove the temporary processing reaction from the triggering QQ message."""
        if not self._processing_emoji_enabled or not message_id:
            return
        try:
            await call_onebot_api(
                self._http_api,
                "set_msg_emoji_like",
                {
                    "message_id": int(str(message_id)),
                    "emoji_id": str(self._processing_emoji_id),
                    "set": False,
                },
                self._access_token or None,
            )
        except Exception as exc:
            logger.debug("NapCat: clear processing emoji failed for %s: %s", message_id, exc)

    async def _mark_post_response(self, reply_to: str | None) -> None:
        """React to the original QQ message after Hermes successfully replies."""
        if not reply_to:
            return
        message_id = str(reply_to)
        if message_id not in self._post_reply_reactions:
            return
        self._post_reply_reactions.discard(message_id)
        try:
            await call_onebot_api(
                self._http_api,
                "set_msg_emoji_like",
                {
                    "message_id": int(message_id),
                    "emoji_id": str(self._post_response_emoji_id),
                },
                self._access_token or None,
            )
        except Exception as exc:
            logger.debug("NapCat: post-response emoji failed for %s: %s", message_id, exc)

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        """After successful delivery, replace processing reaction with done reaction."""
        if outcome is not ProcessingOutcome.SUCCESS:
            return
        message_id = getattr(event, "message_id", None)
        await self._clear_processing(message_id)
        await self._mark_post_response(message_id)

    def _start_private_typing(self, sender_id: str) -> asyncio.Task[None] | None:
        """Refresh QQ DM typing status while Hermes prepares a reply."""
        if not self._private_typing_enabled or not sender_id:
            return None
        try:
            user_id = int(sender_id)
        except (TypeError, ValueError):
            return None
        return asyncio.create_task(self._private_typing_loop(user_id))

    async def _private_typing_loop(self, user_id: int) -> None:
        loop = asyncio.get_running_loop()
        deadline = (
            loop.time() + self._private_typing_max_seconds
            if self._private_typing_max_seconds > 0
            else None
        )
        while deadline is None or loop.time() < deadline:
            try:
                await set_input_status(
                    self._http_api,
                    user_id,
                    self._private_typing_event_type,
                    self._access_token or None,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("NapCat: private typing status failed for %s: %s", user_id, exc)
                return
            await asyncio.sleep(self._private_typing_interval)

    async def _poke_sender(self, sender_id: str, group_id: str = "") -> None:
        """Poke the sender after a reply is successfully delivered."""
        if not self._poke_after_response or not sender_id:
            return
        try:
            params: dict[str, Any] = {"user_id": int(sender_id)}
            action = "friend_poke"
            if group_id:
                params["group_id"] = int(str(group_id).removeprefix("group:"))
                action = "group_poke"
            await call_onebot_api(self._http_api, action, params, self._access_token or None)
        except Exception as exc:
            logger.debug("NapCat: poke failed for user=%s group=%s: %s", sender_id, group_id, exc)

    async def _poke_after_reply(self, reply_to: str | None) -> None:
        if not reply_to:
            return
        ctx = self._post_reply_pokes.pop(str(reply_to), None)
        if not ctx:
            return
        sender_id, group_id = ctx
        await self._poke_sender(sender_id, group_id)

    # ── Outbound ───────────────────────────────────────────────────────────

    def _parse_chat_id(self, chat_id: str) -> tuple[bool, int]:
        if chat_id.startswith("group:"):
            return True, int(chat_id[6:])
        return False, int(chat_id)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict | None = None,
    ) -> SendResult:
        try:
            is_group, num_id = self._parse_chat_id(chat_id)
            chunks = _chunk_text(_strip_markdown(content))
            last_id: str | None = None
            for i, chunk in enumerate(chunks):
                segs: list[dict] = []
                if i == 0 and reply_to:
                    try:
                        segs.append(reply_segment(int(reply_to)))
                    except (ValueError, TypeError):
                        pass
                segs.append(text_segment(chunk))
                if is_group:
                    r = await send_group_msg(self._http_api, num_id, segs, self._access_token or None)
                else:
                    r = await send_private_msg(self._http_api, num_id, segs, self._access_token or None)
                last_id = str(r.get("message_id", ""))
            await self._poke_after_reply(reply_to)
            return SendResult(success=True, message_id=last_id)
        except Exception as exc:
            logger.error("NapCat send error: %s", exc)
            return SendResult(success=False, error=str(exc), retryable=True)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: str | None = None,
        metadata: dict | None = None,
    ) -> SendResult:
        try:
            is_group, num_id = self._parse_chat_id(chat_id)
            segs: list[dict] = [image_segment(image_url)]
            if caption:
                segs.append(text_segment(caption))
            if is_group:
                r = await send_group_msg(self._http_api, num_id, segs, self._access_token or None)
            else:
                r = await send_private_msg(self._http_api, num_id, segs, self._access_token or None)
            return SendResult(success=True, message_id=str(r.get("message_id", "")))
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        metadata: dict | None = None,
    ) -> SendResult:
        try:
            is_group, num_id = self._parse_chat_id(chat_id)
            segs = [record_segment(audio_path)]
            if is_group:
                r = await send_group_msg(self._http_api, num_id, segs, self._access_token or None)
            else:
                r = await send_private_msg(self._http_api, num_id, segs, self._access_token or None)
            return SendResult(success=True, message_id=str(r.get("message_id", "")))
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        metadata: dict | None = None,
    ) -> SendResult:
        try:
            is_group, num_id = self._parse_chat_id(chat_id)
            segs = [video_segment(video_path)]
            if is_group:
                r = await send_group_msg(self._http_api, num_id, segs, self._access_token or None)
            else:
                r = await send_private_msg(self._http_api, num_id, segs, self._access_token or None)
            return SendResult(success=True, message_id=str(r.get("message_id", "")))
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        filename: str | None = None,
        metadata: dict | None = None,
    ) -> SendResult:
        try:
            is_group, num_id = self._parse_chat_id(chat_id)
            name = filename or os.path.basename(file_path)
            if is_group:
                await upload_group_file(self._http_api, num_id, file_path, name, self._access_token or None)
            else:
                await upload_private_file(self._http_api, num_id, file_path, name, self._access_token or None)
            return SendResult(success=True)
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def get_chat_info(self, chat_id: str) -> dict:
        try:
            is_group, num_id = self._parse_chat_id(chat_id)
            if is_group:
                resp = await call_onebot_api(
                    self._http_api, "get_group_info",
                    {"group_id": num_id, "no_cache": True},
                    self._access_token or None,
                )
                g = resp["data"]
                return {"name": g.get("group_name", str(num_id)), "type": "group", "chat_id": chat_id}
            else:
                resp = await call_onebot_api(
                    self._http_api, "get_stranger_info",
                    {"user_id": num_id, "no_cache": True},
                    self._access_token or None,
                )
                u = resp["data"]
                return {"name": u.get("nickname", str(num_id)), "type": "dm", "chat_id": chat_id}
        except Exception as exc:
            return {"name": chat_id, "type": "unknown", "error": str(exc), "chat_id": chat_id}

    async def format_message(self, content: str) -> str:
        return _strip_markdown(content)

    async def send_typing(self, chat_id: str, metadata: dict | None = None) -> None:
        is_group, num_id = self._parse_chat_id(chat_id)
        if is_group:
            return
        await set_input_status(
            self._http_api,
            num_id,
            self._private_typing_event_type,
            self._access_token or None,
        )

    async def stop_typing(self, chat_id: str) -> None:
        pass

# ── Plugin registration ─────────────────────────────────────────────────────

def validate_config(config) -> bool:
    extra: dict[str, Any] = getattr(config, "extra", {}) or {}
    return bool(str(extra.get("http_api", "")).strip())


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> dict:
    """Seed PlatformConfig.extra from either platforms.napcat.extra or napcat.*."""
    seeded: dict[str, Any] = {}
    extra = platform_cfg.get("extra") if isinstance(platform_cfg, dict) else None
    if isinstance(extra, dict):
        seeded.update(extra)
    for key in (
        "http_api", "access_token", "self_id", "ws_port", "media_max_mb",
        "owners", "owner", "admins", "group_allow_chats", "group_allowed_chats", "allowed_groups",
        "require_mention", "group_require_mention", "processing_emoji",
        "processing_emoji_enabled", "processing_emoji_id", "poke_after_response",
        "post_response_emoji", "post_response_emoji_enabled", "post_response_emoji_id",
        "private_typing_status", "private_typing_enabled", "private_typing_event_type",
        "private_typing_interval", "private_typing_max_seconds",
    ):
        if isinstance(platform_cfg, dict) and key in platform_cfg:
            seeded[key] = platform_cfg[key]
    return seeded


def _env_enablement() -> dict[str, Any]:
    """Expose env-only NapCat home channel to gateway/cron config loading."""
    seeded: dict[str, Any] = {}
    home = os.getenv("NAPCAT_HOME_CHANNEL", "").strip()
    if home:
        seeded["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("NAPCAT_HOME_CHANNEL_NAME", "Home"),
            "thread_id": os.getenv("NAPCAT_HOME_CHANNEL_THREAD_ID", "").strip() or None,
        }
    return seeded


def register(ctx) -> None:
    # Importing this module registers qq_* tools into the dynamic "napcat"
    # toolset. The adapter initializes its HTTP endpoint/admin context at runtime.
    from . import qq_tool as _qq_tool  # noqa: F401

    ctx.register_hook("pre_tool_call", _napcat_acl_pre_tool_call)

    ctx.register_platform(
        name="napcat",
        label="NapCat / OneBot 11",
        adapter_factory=lambda cfg: NapCatAdapter(cfg),
        check_fn=check_napcat_requirements,
        validate_config=validate_config,
        env_enablement_fn=_env_enablement,
        install_hint="aiohttp is required (bundled with Hermes gateway installs)",
        emoji="🐧",
        allowed_users_env="NAPCAT_ALLOWED_USERS",
        allow_all_env="NAPCAT_ALLOW_ALL_USERS",
        cron_deliver_env_var="NAPCAT_HOME_CHANNEL",
        max_message_length=_QQ_TEXT_LIMIT,
        platform_hint="QQ/NapCat chat. QQ does not render Markdown; keep replies concise, plain text, and Chinese when appropriate.",
        apply_yaml_config_fn=_apply_yaml_config,
    )
