"""NapCat (OneBot 11 reverse WebSocket) platform adapter for Hermes Agent.

Installed as a user plugin at:
    ~/.hermes/plugins/hermes-napcat/adapter.py

Configuration in ~/.hermes/config.yaml:

    platforms:
      napcat:
        enabled: true
        extra:
          http_api: "http://127.0.0.1:18801"
          access_token: "<required-high-entropy-token>"
          self_id: "<BOT_QQ_ID>"
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
from collections import OrderedDict
from contextlib import suppress
import hmac
import json
import logging
import os
import re
import subprocess
import tempfile
import unicodedata
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
    download_public_url_bytes,
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
_USER_ALLOWED_TOOLS: set[str] = set()


def _ws_token_is_valid(
    expected_token: str,
    authorization_header: str | None,
    query_token: str | None,
) -> bool:
    """Validate the token sent by a OneBot reverse-WebSocket client."""
    expected = str(expected_token or "")
    if not expected:
        return False
    header = str(authorization_header or "").strip()
    presented = str(query_token or "")
    if header.lower().startswith("bearer "):
        presented = header[7:].strip()
    return bool(presented) and hmac.compare_digest(presented, expected)


def _event_matches_self_id(event: dict[str, Any], expected_self_id: str) -> bool:
    """Reject events that claim to originate from a different QQ bot."""
    expected = str(expected_self_id or "")
    actual = str((event or {}).get("self_id", "") or "")
    return bool(expected and actual and hmac.compare_digest(actual, expected))


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


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _bounded_float(value: Any, *, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


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


def _is_slash_command(content: str) -> bool:
    return bool(re.match(r"^\s*/[A-Za-z][\w-]*(?:\s|$)", content or ""))


def _is_non_final_progress_message(content: str, metadata: dict | None = None) -> bool:
    """Return True for gateway progress/status bubbles, never final replies.

    Hermes' long-running heartbeat currently reaches non-Discord adapters without
    the generic ``non_conversational`` metadata marker.  NapCat normally falls
    back to the active incoming message as a QQ reply anchor, so an unmarked
    heartbeat such as ``Working — 10 min`` can look like the final answer and can
    enter the plugin's queued-reply completion bookkeeping.  Recognize both the
    forward-compatible metadata marker and the current heartbeat text locally.
    """
    meta = metadata or {}
    if meta.get("non_conversational") or meta.get("non_conversational_history"):
        return True
    return bool(
        re.match(
            r"^\s*⏳\s*Working\s*[—-]\s*\d+\s*min(?:\b|\s|[—-])",
            str(content or ""),
            flags=re.IGNORECASE,
        )
    )


def _safe_identity_part(value: str, *, max_len: int = 32) -> str:
    """Keep sender labels compact and bracket-safe for gateway prefixes."""
    cleaned = re.sub(r"[\r\n\t]+", " ", str(value or "")).strip()
    cleaned = cleaned.replace("[", "［").replace("]", "］")
    cleaned = re.sub(r"\s+", " ", cleaned)
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 1] + "…"
    return cleaned


def _safe_display_name(value: str, *, max_len: int = 24) -> str:
    """Render a mutable QQ card as a one-line, non-structural decoration."""
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    cleaned = "".join(
        char for char in normalized if unicodedata.category(char) not in {"Cc", "Cf"}
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = cleaned.translate(
        str.maketrans({
            "[": "［", "]": "］", "<": "＜", ">": "＞",
            "「": "｢", "」": "｣",
        })
    )
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 1] + "…"
    return cleaned


def _sender_identity_label(role: str, user_id: str, display_name: str) -> str:
    """Return a trusted QQ identity core plus an optional safe card decoration."""
    role_label = {"owner": "owner", "admin": "admin", "user": "user"}.get(role, "user")
    qq = _safe_identity_part(user_id, max_len=20) or "unknown"
    stable = f"[{role_label}]<{qq}>"
    card = _safe_display_name(display_name)
    if not card or card == qq:
        return stable
    return f"{stable}「{card}」"


def _group_media_attribution_text(
    text: str,
    identity_label: str,
    *,
    has_image: bool,
    has_voice: bool,
) -> str:
    """Keep captionless group media attributable after vision/transcription."""
    if str(text or "").strip():
        return text
    return _captionless_media_context(
        text,
        identity_label,
        has_image=has_image,
        has_voice=has_voice,
    )


def _captionless_media_context(
    text: str,
    identity_label: str,
    *,
    has_image: bool,
    has_voice: bool,
) -> str:
    """Append a media marker without duplicating the current speaker label."""
    if has_image:
        marker = "[发送了图片]"
    elif has_voice:
        marker = "[发送了语音]"
    else:
        return text
    if str(text or "").strip():
        return f"{text.rstrip()}\n{marker}"
    return f"{identity_label} {marker}"


def _quoted_message_context(
    identity_label: str,
    quoted_text: str,
    quoted_segments: list[dict],
) -> tuple[str, str]:
    """Describe a quoted QQ message even when it has no extractable text."""
    content = str(quoted_text or "").strip()
    if content:
        return (
            f"[引用 {identity_label} 的消息: {content}]",
            f"{identity_label}: {content}",
        )
    types = {str(segment.get("type") or "") for segment in quoted_segments or []}
    if "image" in types:
        kind = "图片消息"
    elif "record" in types:
        kind = "语音消息"
    elif "video" in types:
        kind = "视频消息"
    elif "file" in types:
        kind = "文件消息"
    else:
        kind = "消息"
    summary = f"{identity_label} 的{kind}"
    return f"[引用 {summary}]", summary


def _napcat_acl_pre_tool_call(tool_name: str, **_: Any) -> dict | None:
    """Hard tool permission gate for NapCat sessions."""
    try:
        from gateway.session_context import get_session_env

        platform = get_session_env("HERMES_SESSION_PLATFORM", "")
        user_id = get_session_env("HERMES_SESSION_USER_ID", "")
    except Exception:
        if tool_name.startswith("qq_"):
            return {
                "action": "block",
                "message": "NapCat 权限校验失败：会话身份上下文不可用。",
            }
        return None
    if not platform:
        if tool_name.startswith("qq_"):
            return {
                "action": "block",
                "message": "NapCat 权限校验失败：QQ 工具缺少会话身份上下文。",
            }
        return None
    if platform != "napcat":
        if tool_name.startswith("qq_"):
            return {
                "action": "block",
                "message": "NapCat 权限校验失败：QQ 工具只能从 NapCat 身份会话调用。",
            }
        return None
    if not user_id:
        return {
            "action": "block",
            "message": "NapCat 权限校验失败：会话用户身份缺失。",
        }

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


def _collect_bot_mention_names(
    configured_names: list[str],
    login_nickname: str,
    group_member: dict[str, Any] | None = None,
) -> list[str]:
    """Return de-duplicated bot aliases, including its current group card."""
    member = group_member or {}
    candidates = [
        *configured_names,
        login_nickname,
        str(member.get("card") or ""),
        str(member.get("nickname") or ""),
    ]
    names: list[str] = []
    for candidate in candidates:
        name = str(candidate or "").strip()
        if name and name not in names:
            names.append(name)
    return names


def _strip_textual_bot_mention(text: str, names: list[str]) -> tuple[bool, str]:
    """Accept QQ clients that emit ``@[灵溪] text`` as plain text.

    Real QQ mentions arrive as CQ ``at`` segments and are preferred. Some clients
    or copy/paste paths produce a literal text prefix like ``@[灵溪] 出来说话``;
    treat only a leading explicit @name as a mention so ordinary text such as
    ``问下灵溪`` still does not trigger when ``require_mention`` is enabled.
    """
    raw = str(text or "").lstrip()
    # Prefer the longest configured name when aliases overlap (for example,
    # "灵溪" and the group card "灵溪灵溪灵").  Matching the short alias first
    # would leave the unmatched suffix in the user's message.
    for name in sorted(names, key=len, reverse=True):
        escaped = re.escape(name)
        pattern = rf"^@\s*(?:\[{escaped}\]|{escaped})(?:\s+|[:,，：、]?\s*)"
        match = re.match(pattern, raw)
        if match:
            return True, raw[match.end():].lstrip()
    return False, text


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
    in_path = ""
    out_path = ""
    success = False
    try:
        data = await download_public_url_bytes(url, max_bytes=max_bytes, timeout=25)
        fd, in_path = tempfile.mkstemp(suffix=".silk")
        os.close(fd)
        out_path = in_path.replace(".silk", ".wav")
        with open(in_path, "wb") as f:
            f.write(data)
        max_seconds = max(1, min(600, max_bytes // 32000))
        result = await asyncio.to_thread(
            subprocess.run,
            [
                "ffmpeg", "-y", "-i", in_path,
                "-t", str(max_seconds),
                "-ar", "16000", "-ac", "1", "-f", "wav", out_path,
            ],
            capture_output=True,
            timeout=15,
        )
        if result.returncode != 0 or os.path.getsize(out_path) > max_bytes + 4096:
            return None
        success = True
        return out_path
    except Exception as exc:
        logger.debug("Voice download/convert failed: %s", exc)
        return None
    finally:
        if in_path:
            with suppress(OSError):
                os.unlink(in_path)
        if out_path and not success:
            with suppress(OSError):
                os.unlink(out_path)


def _cleanup_event_temp_media(event: Any) -> None:
    metadata = getattr(event, "metadata", None)
    paths = list(metadata.pop("_napcat_temp_media_paths", ()) if isinstance(metadata, dict) else ())
    paths.extend(getattr(event, "_napcat_temp_media_paths", ()) or ())
    if hasattr(event, "_napcat_temp_media_paths"):
        setattr(event, "_napcat_temp_media_paths", [])
    for path in paths:
        with suppress(OSError):
            os.unlink(path)


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
        self._ws_port: int = _bounded_int(
            extra.get("ws_port", 18800), default=18800, minimum=1, maximum=65535
        )
        self._ws_host: str = str(extra.get("ws_host", "0.0.0.0") or "0.0.0.0")
        self._ws_access_token: str = str(
            extra.get("ws_access_token", self._access_token) or ""
        )
        self._ws_allowed_ips: set[str] = set(_as_str_list(extra.get("ws_allowed_ips")))
        self._ws_max_message_bytes: int = _bounded_int(
            extra.get("ws_max_message_bytes", 2 * 1024 * 1024),
            default=2 * 1024 * 1024,
            minimum=64 * 1024,
            maximum=16 * 1024 * 1024,
        )
        self._ws_heartbeat_seconds: float = _bounded_float(
            extra.get("ws_heartbeat_seconds", 30),
            default=30.0,
            minimum=5.0,
            maximum=300.0,
        )
        self._ws_max_inflight: int = _bounded_int(
            extra.get("ws_max_inflight", 32), default=32, minimum=1, maximum=256
        )
        # Keep SessionSource.platform equal to the real adapter platform.
        # A previous Desktop grouping workaround aliased NapCat messages as
        # qqbot, but the gateway then applied qqbot authorization instead of
        # NapCat's own group ACL.  That caused messages to receive QQ reactions
        # while the core silently rejected them.  Desktop grouping must be fixed
        # in UI/session presentation code, not by changing runtime platform id.
        if extra.get("desktop_source_platform") and str(extra.get("desktop_source_platform")).lower() != "napcat":
            logger.warning(
                "NapCat: ignoring desktop_source_platform=%r; runtime source platform must remain napcat",
                extra.get("desktop_source_platform"),
            )
        self._source_platform = self.platform
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
        # Optional textual aliases. Real OneBot ``at`` segments are matched by
        # QQ number; plain-text @ fallbacks also learn the login nickname and the
        # bot's current per-group card dynamically.
        self._mention_names: list[str] = _as_str_list(extra.get("mention_names"))
        self._login_nickname: str = ""
        self._mention_name_cache_seconds: float = max(
            0.0, float(extra.get("mention_name_cache_seconds", 300))
        )
        self._group_mention_name_cache: OrderedDict[str, tuple[float, list[str]]] = OrderedDict()
        self._group_mention_name_cache_max: int = _bounded_int(
            extra.get("mention_name_cache_max", 512), default=512, minimum=16, maximum=4096
        )
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
        self._raw_tasks: set[asyncio.Task[None]] = set()
        self._seen_message_ids: OrderedDict[str, float] = OrderedDict()
        # Maps original incoming message_id -> (sender QQ, group_id or "").
        # send() consumes this after it successfully sends the reply, so the
        # post-processing poke happens after the visible answer is delivered.
        self._post_reply_pokes: dict[str, tuple[str, str]] = {}
        # Original incoming message IDs that should receive a second reaction
        # after Hermes has successfully sent its visible reply.
        self._post_reply_reactions: set[str] = set()
        # Chat -> message_id currently being processed.  BasePlatformAdapter's
        # media batch path does not pass reply_to into send_image/send_voice, so
        # use this as a plugin-local fallback to keep attachments/replies anchored
        # to the message whose turn is actually running, not a later queued one.
        self._active_reply_anchors: dict[str, str] = {}
        # BasePlatformAdapter's queue-mode text debounce merges rapid TEXT
        # follow-ups into one pending event and rewrites the event.message_id to
        # the latest QQ message.  On QQ that makes quote-replies drift between
        # queued messages.  Keep a plugin-local FIFO tail instead: the base
        # pending slot remains the next turn; this tail holds later turns.
        self._queued_text_followups: dict[str, list[MessageEvent]] = {}
        # GatewayRunner may drain queued follow-ups inside one outer
        # BasePlatformAdapter event: it sends intermediate "first responses"
        # itself, then returns only the final queued response to Base, whose
        # final delivery still carries the original event's reply anchor.  Track
        # QQ message ids that arrive while a session is active so plugin-side
        # sends can quote each inline/final response to the queued turn it
        # actually answers, without patching Hermes core.
        self._busy_followup_reply_anchors: dict[str, list[str]] = {}
        self._next_final_reply_anchors: dict[str, str] = {}
        # Reaction completion needs the same queued-anchor correction as quote
        # replies.  GatewayRunner can send the first queued response inline and
        # then deliver the final queued response through the outer event's
        # on_processing_complete hook, whose event.message_id is stale.
        self._inline_completion_reply_anchors: dict[str, str] = {}
        self._pending_completion_reply_anchors: dict[str, str] = {}

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
        if not self._ws_access_token:
            logger.error("NapCat: refusing reverse-WebSocket listener without an access token")
            return False
        if self._runner is not None:
            logger.debug("NapCat: reverse WS listener is already running")
            return True

        app = aiohttp.web.Application()
        app.router.add_get("/", self._ws_handler)
        self._runner = aiohttp.web.AppRunner(app)
        try:
            await self._runner.setup()
            site = aiohttp.web.TCPSite(self._runner, self._ws_host, self._ws_port)
            await site.start()
        except Exception:
            await self._runner.cleanup()
            self._runner = None
            raise
        self._is_connected = True
        logger.info("NapCat: reverse WS listening on ws://%s:%d", self._ws_host, self._ws_port)

        try:
            info = await get_login_info(self._http_api, self._access_token or None)
            if not self._self_id:
                self._self_id = str(info.get("user_id", ""))
            self._login_nickname = str(info.get("nickname") or "").strip()
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
        tasks = list(self._raw_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._raw_tasks.clear()
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        logger.info("NapCat: disconnected")

    # ── Inbound WS handler ─────────────────────────────────────────────────

    async def _ws_handler(self, request: aiohttp.web.Request) -> aiohttp.web.WebSocketResponse:
        remote = str(request.remote or "")
        if self._ws_allowed_ips and remote not in self._ws_allowed_ips:
            logger.warning("NapCat WS rejected from untrusted IP %s", remote or "?")
            raise aiohttp.web.HTTPForbidden(text="untrusted reverse-WebSocket source")
        if not _ws_token_is_valid(
            self._ws_access_token,
            request.headers.get("Authorization"),
            request.query.get("access_token"),
        ):
            logger.warning("NapCat WS rejected: invalid token from %s", remote or "?")
            raise aiohttp.web.HTTPUnauthorized(text="invalid reverse-WebSocket token")

        for old_ws in list(self._active_ws):
            await old_ws.close(code=1008, message=b"replaced by authenticated peer")
        self._active_ws.clear()

        ws = aiohttp.web.WebSocketResponse(
            max_msg_size=self._ws_max_message_bytes,
            heartbeat=self._ws_heartbeat_seconds,
        )
        await ws.prepare(request)
        self._active_ws.add(ws)
        logger.info("NapCat authenticated WS connected from %s", remote)
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if len(self._raw_tasks) >= self._ws_max_inflight:
                        logger.warning(
                            "NapCat WS frame dropped at inflight cap=%d",
                            self._ws_max_inflight,
                        )
                        continue
                    task = asyncio.create_task(self._handle_raw(msg.data))
                    self._raw_tasks.add(task)
                    task.add_done_callback(self._raw_tasks.discard)
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
        if not isinstance(data, dict):
            return
        if not _event_matches_self_id(data, self._self_id):
            logger.warning(
                "NapCat event rejected: self_id=%r expected=%r",
                data.get("self_id"),
                self._self_id,
            )
            return
        if data.get("post_type") != "message":
            return
        message_id = str(data.get("message_id", "") or "")
        if message_id:
            now = asyncio.get_running_loop().time()
            cutoff = now - 600.0
            while self._seen_message_ids:
                _, seen_at = next(iter(self._seen_message_ids.items()))
                if seen_at >= cutoff:
                    break
                self._seen_message_ids.popitem(last=False)
            if message_id in self._seen_message_ids:
                logger.debug("NapCat duplicate message ignored: %s", message_id)
                return
            self._seen_message_ids[message_id] = now
            while len(self._seen_message_ids) > 4096:
                self._seen_message_ids.popitem(last=False)
        try:
            await self._process_message(data)
        except Exception:
            logger.exception("NapCat: error processing message")

    async def _get_group_mention_names(self, group_id: str) -> list[str]:
        """Return textual @ aliases for the bot in one group.

        OneBot ``at`` segments are always matched by the bot QQ number. This
        lookup is only for clients/copy paths that flatten an @ mention into
        plain text, where the bot's per-group card is needed to remove the full
        visible name. Results are cached briefly so ordinary group traffic does
        not call NapCat for every message.
        """
        now = asyncio.get_running_loop().time()
        cached = self._group_mention_name_cache.get(group_id)
        if cached and cached[0] > now:
            self._group_mention_name_cache.move_to_end(group_id)
            return cached[1]
        if cached:
            self._group_mention_name_cache.pop(group_id, None)

        member: dict[str, Any] = {}
        if self._http_api and self._self_id and group_id:
            try:
                response = await call_onebot_api(
                    self._http_api,
                    "get_group_member_info",
                    {
                        "group_id": int(group_id),
                        "user_id": int(self._self_id),
                        "no_cache": True,
                    },
                    self._access_token or None,
                )
                member = response.get("data") or {}
            except Exception as exc:
                logger.debug(
                    "NapCat: failed to fetch bot group card for group %s: %s",
                    group_id,
                    exc,
                )

        names = _collect_bot_mention_names(
            self._mention_names,
            self._login_nickname,
            member,
        )
        self._group_mention_name_cache[group_id] = (
            now + self._mention_name_cache_seconds,
            names,
        )
        self._group_mention_name_cache.move_to_end(group_id)
        while len(self._group_mention_name_cache) > self._group_mention_name_cache_max:
            self._group_mention_name_cache.popitem(last=False)
        return names

    async def _process_message(self, event: dict) -> None:
        is_group = event.get("message_type") == "group"
        sender_id = str(event.get("user_id", ""))
        sender = event.get("sender", {})
        sender_name: str = sender.get("card") or sender.get("nickname") or sender_id
        group_id = str(event.get("group_id", "")) if is_group else ""
        chat_id = f"group:{group_id}" if is_group else sender_id
        segments: list[dict] = event.get("message", [])
        mentioned = False
        textual_mention_text: str | None = None

        # Authorize before mention-name lookup so rejected groups cannot consume
        # NapCat HTTP calls or grow the per-group alias cache.
        owners = set(self._owners)
        admins = set(self._admins)
        role = _role_for_user(sender_id, owners, admins)
        identity_label = _sender_identity_label(role, sender_id, sender_name)
        if is_group:
            if not self._group_allow_chats or group_id not in self._group_allow_chats:
                return
        elif role not in {"owner", "admin"}:
            return

        # Group mention handling. This deployment gates groups by chat allowlist
        # Set require_mention/group_require_mention false to allow whitelisted
        # users to trigger the bot in groups without @.
        if is_group:
            mentioned = bool(self._self_id and _has_bot_mention(segments, self._self_id))
            if not mentioned:
                mention_names = await self._get_group_mention_names(group_id)
                textual_mentioned, stripped = _strip_textual_bot_mention(
                    _extract_text(segments),
                    mention_names,
                )
                if textual_mentioned:
                    mentioned = True
                    textual_mention_text = stripped
            if self._require_mention and self._self_id and not mentioned:
                return
            if self._self_id and mentioned:
                segments = _strip_bot_mention(segments, self._self_id)

        # Authorization above is QQ-number based only. Display names remain
        # decorative because they are user-controlled and spoofable.

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
        if textual_mention_text is not None:
            text = textual_mention_text
        image_urls = _extract_images(segments)
        record_url = _extract_record(segments)
        captionless_media = bool(is_group and not text.strip() and (image_urls or record_url))

        if is_group and mentioned and not text.strip() and not image_urls and not record_url:
            text = "hi"

        if role == "user" and _is_slash_command(text):
            logger.info(
                "NapCat blocked slash command from ordinary user: chat=%s user=%s command=%s",
                chat_id,
                sender_id,
                text.split(maxsplit=1)[0][:80],
            )
            original_message_id = str(event.get("message_id", "") or "")
            await self.send(
                chat_id,
                "普通用户不能使用指令，请直接用普通聊天提问。",
                reply_to=original_message_id or None,
            )
            return

        # In group chats, add a model-visible speaker prefix ourselves in the
        # MC-like form `[role]<QQ> message`, then leave source.user_name empty
        # so the Hermes gateway does not add its own `[source.user_name]` wrapper.
        # Use only role + QQ: nicknames/cards are mutable and not authority.
        # Some QQ / NapCat event paths already include a prefix in the extracted
        # text; strip that copy first. Keep slash commands starting with `/` so
        # the gateway command parser still recognizes them.
        if is_group and text:
            stripped_text = text.lstrip()
            if stripped_text.startswith("/"):
                text = stripped_text
            else:
                # Strip legacy/raw nickname prefixes and previous stable
                # identity-prefix variants if NapCat text already includes one.
                candidate_prefixes = (
                    f"[{sender_name}]",
                    identity_label,
                    f"[发言者身份={role};QQ={sender_id}]",
                    f"[{role}:{sender_id}]",
                )
                for sender_prefix in candidate_prefixes:
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
                q_segments = quoted.get("message", [])
                q_text = _extract_text(q_segments)
                quote_context, reply_text = _quoted_message_context(
                    q_identity,
                    q_text,
                    q_segments,
                )
                text = f"{quote_context}\n{text}"
            except Exception:
                pass

        if captionless_media:
            text = _captionless_media_context(
                text,
                identity_label,
                has_image=bool(image_urls),
                has_voice=bool(record_url),
            )

        # Prefix normal group messages after quote extraction so the whole
        # model-visible user turn reads like MC chat: `[owner]<QQ> content`.
        # Slash commands must stay unprefixed for the gateway command parser.
        if is_group and text and not text.lstrip().startswith("/"):
            if not text.startswith(identity_label):
                text = f"{identity_label} {text}"

        # Determine MessageType and media
        media_urls: list[str] = []
        media_types: list[str] = []
        msg_type = MessageType.TEXT

        if image_urls:
            msg_type = MessageType.PHOTO
            max_bytes = self._media_max_mb * 1024 * 1024
            for url in image_urls[:1]:  # cache first image for vision tool
                try:
                    img_data = await download_public_url_bytes(
                        url,
                        max_bytes=max_bytes,
                        timeout=25,
                    )
                    cached = cache_image_from_bytes(img_data)
                    media_urls.append(cached)
                    media_types.append("image/jpeg")
                except Exception as exc:
                    logger.debug("NapCat: image download failed: %s", exc)

        elif record_url:
            msg_type = MessageType.VOICE

        if not text and not media_urls and not record_url:
            return

        original_message_id = str(event.get("message_id", "") or "")

        source = SessionSource(
            platform=self._source_platform,
            chat_id=chat_id,
            chat_name=sender_name if not is_group else group_id,
            chat_type="group" if is_group else "dm",
            user_id=sender_id,
            # Group messages are manually prefixed as `[role]<QQ> ...` above.
            # Leaving user_name empty prevents the gateway from wrapping the
            # prefix again as `[[role]<QQ>] ...`.
            user_name="" if is_group else identity_label,
            message_id=original_message_id or None,
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
                "你是普通用户：只能普通聊天，不能调用任何工具，也不能使用 /new、/reset、/approve 等 slash 指令；不得执行本地命令、读写本机文件、修改配置、写入记忆、修改用户画像记忆或进行 QQ 管理操作。"
            )
        privacy_prompt = ""
        if is_group:
            privacy_prompt = (
                "群聊隐私规则：USER PROFILE、长期记忆和画像默认属于 owner 本人，而不是群里所有发言者。"
                "在群聊回复中不得披露 owner 的个人信息、个人画像或私密记忆。"
                "可以在不明说隐私内容的前提下内部参考非敏感偏好来改善回答；如用户要求查看/复述/确认 owner 个人信息或私密记忆，应拒绝并建议 owner 私聊。"
                "权限信息本身只可按当前前缀/QQ 简要判断，不得把 owner 画像套到 admin 或普通群友身上。"
            )
        permission_prompt = (
            f"[{role_zh}] QQ:{sender_id}。"
            "权限身份仅按 QQ 号判定，不按昵称/群名片判定。"
            f"{permission_detail}"
            "读取公开网页/回答普通问题可直接处理；涉及越权工具时必须拒绝或让用户联系 owner/admin。"
            f"{privacy_prompt}"
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
        message_event.metadata["_napcat_temp_media_paths"] = []

        typing_task: asyncio.Task[None] | None = None
        try:
            if record_url:
                max_bytes = self._media_max_mb * 1024 * 1024
                wav = await _download_and_convert_wav(record_url, max_bytes)
                if wav:
                    message_event.metadata["_napcat_temp_media_paths"] = [wav]
                    message_event.media_urls.append(wav)
                    message_event.media_types.append("audio/wav")
                    logger.debug("NapCat: voice -> %s", wav)

            if not text and not message_event.media_urls:
                return

            if original_message_id:
                self._remember_post_reply_reaction(original_message_id)
                self._remember_post_reply_poke(original_message_id, sender_id, group_id)
                await self._mark_processing(original_message_id)

            inline_command_completion = self._needs_inline_command_completion_reaction(message_event)
            if not is_group:
                typing_task = self._start_private_typing(sender_id)
            await self.handle_message(message_event)
            if inline_command_completion:
                await self._complete_inline_command(message_event)
        except BaseException:
            _cleanup_event_temp_media(message_event)
            raise
        finally:
            if typing_task:
                typing_task.cancel()
                with suppress(asyncio.CancelledError):
                    await typing_task

    async def _complete_inline_command(self, event: MessageEvent) -> None:
        """Finish an inline command whose core path skips processing-complete hooks."""
        try:
            await self._clear_processing(event.message_id)
            await self._mark_post_response(event.message_id)
        finally:
            _cleanup_event_temp_media(event)

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
        """Clean temporary media, then update completion reactions after delivery."""
        _cleanup_event_temp_media(event)
        message_id = getattr(event, "message_id", None)
        chat_id = str(getattr(getattr(event, "source", None), "chat_id", "") or "")
        actual_message_id = self._pending_completion_reply_anchors.pop(chat_id, None) or message_id
        if chat_id and message_id and self._active_reply_anchors.get(chat_id) == str(message_id):
            self._active_reply_anchors.pop(chat_id, None)
        if outcome is not ProcessingOutcome.SUCCESS:
            return
        original_message_id = getattr(event, "_hermes_original_message_id", None)
        stale_ids: set[str] = set()
        if original_message_id and str(original_message_id) != str(actual_message_id or ""):
            stale_ids.add(str(original_message_id))
        if message_id and str(message_id) != str(actual_message_id or ""):
            stale_ids.add(str(message_id))
        for stale_id in stale_ids:
            # A queued follow-up may become the visible final reply target while
            # the outer event was an older message.  Clear stale processing
            # reactions so QQ does not appear to complete the wrong item; mark
            # completion on the actual final reply target below.
            await self._clear_processing(stale_id)
            self._post_reply_reactions.discard(stale_id)
            self._post_reply_pokes.pop(stale_id, None)
        await self._clear_processing(actual_message_id)
        await self._mark_post_response(actual_message_id)

    async def on_processing_start(self, event: MessageEvent) -> None:
        """Remember the actual running turn's reply anchor for media sends."""
        message_id = getattr(event, "message_id", None)
        chat_id = str(getattr(getattr(event, "source", None), "chat_id", "") or "")
        if chat_id and message_id:
            self._active_reply_anchors[chat_id] = str(message_id)
            while len(self._active_reply_anchors) > 256:
                self._active_reply_anchors.pop(next(iter(self._active_reply_anchors)), None)
        self._promote_queued_text_followup(event)

    async def handle_message(self, event: MessageEvent) -> None:
        """Record QQ follow-up anchors before Hermes core queues/drains them."""
        try:
            chat_id = str(getattr(getattr(event, "source", None), "chat_id", "") or "")
            message_id = getattr(event, "message_id", None)
            session_key = self._session_key_for_event(event)
            if event.get_command() in {"new", "reset"} and chat_id:
                self._busy_followup_reply_anchors.pop(chat_id, None)
                self._next_final_reply_anchors.pop(chat_id, None)
                self._inline_completion_reply_anchors.pop(chat_id, None)
                self._pending_completion_reply_anchors.pop(chat_id, None)
            elif (
                chat_id
                and message_id
                and session_key in self._active_sessions
                and not self._needs_inline_command_completion_reaction(event)
            ):
                anchors = self._busy_followup_reply_anchors.setdefault(chat_id, [])
                msg = str(message_id)
                if msg not in anchors and self._next_final_reply_anchors.get(chat_id) != msg:
                    anchors.append(msg)
                if len(anchors) > 32:
                    del anchors[:-32]
                logger.debug(
                    "NapCat queued follow-up anchor recorded: chat=%s message_id=%s depth=%d",
                    chat_id,
                    msg,
                    len(anchors),
                )
        except Exception:
            pass
        await super().handle_message(event)

    def _session_key_for_event(self, event: MessageEvent) -> str:
        return build_session_key(
            event.source,
            group_sessions_per_user=self.config.extra.get("group_sessions_per_user", True),
            thread_sessions_per_user=self.config.extra.get("thread_sessions_per_user", False),
        )

    async def _queue_text_debounce(self, session_key: str, event: MessageEvent) -> None:
        """Queue QQ text follow-ups as separate turns instead of merging text.

        Hermes core's queue-mode debounce is useful for bursty platforms, but
        QQ quote-replies are per message.  Merging multiple queued QQ texts into
        one MessageEvent mutates message_id to the newest message, which can make
        the first queued answer reply to the second message (or vice versa).
        """
        if session_key not in self._pending_messages:
            self._pending_messages[session_key] = event
            return
        tail = self._queued_text_followups.setdefault(session_key, [])
        tail.append(event)
        if len(tail) > 32:
            dropped = tail.pop(0)
            logger.warning(
                "NapCat queued text follow-up dropped at cap: session=%s message_id=%s",
                session_key,
                getattr(dropped, "message_id", "?"),
            )

    async def _flush_text_debounce_now(self, session_key: str) -> bool:
        """No-op for NapCat text FIFO: _queue_text_debounce queues immediately."""
        return False

    def _discard_text_debounce(self, session_key: str) -> None:
        super()._discard_text_debounce(session_key)
        self._queued_text_followups.pop(session_key, None)

    def _promote_queued_text_followup(self, event: MessageEvent) -> None:
        """Move the next FIFO tail item into the base pending slot.

        Called when a queued turn starts.  The base adapter has just popped the
        pending slot for that running turn, so staging the next tail item here
        lets the normal base drain cascade continue without modifying Hermes
        core.
        """
        try:
            session_key = self._session_key_for_event(event)
        except Exception:
            return
        if session_key in self._pending_messages:
            return
        tail = self._queued_text_followups.get(session_key)
        if not tail:
            return
        next_event = tail.pop(0)
        if not tail:
            self._queued_text_followups.pop(session_key, None)
        self._pending_messages[session_key] = next_event

    def _stage_next_final_reply_anchor(self, chat_id: str) -> str | None:
        queued = self._busy_followup_reply_anchors.get(chat_id)
        if not queued:
            return None
        staged = queued.pop(0)
        self._next_final_reply_anchors[chat_id] = staged
        if not queued:
            self._busy_followup_reply_anchors.pop(chat_id, None)
        return staged

    def _effective_reply_to(
        self,
        chat_id: str,
        reply_to: str | None = None,
        metadata: dict | None = None,
    ) -> str | None:
        """Use the reply anchor for the turn actually being answered.

        Hermes core's in-band queued-followup path can return the final queued
        answer through the outer/original event.  For QQ quote replies, that
        would reuse the first message's id.  We detect the two send shapes:
        non-notify inline first-response sends advance the queued-anchor cursor;
        notify final sends with the stale explicit anchor are redirected to the
        staged queued id.
        """
        chat_key = str(chat_id)
        notify = bool((metadata or {}).get("notify"))
        active_anchor = self._active_reply_anchors.get(chat_key)

        if reply_to:
            explicit = str(reply_to)
            staged = self._next_final_reply_anchors.get(chat_key)
            if notify and staged and active_anchor and explicit == active_anchor:
                self._next_final_reply_anchors.pop(chat_key, None)
                self._pending_completion_reply_anchors[chat_key] = staged
                logger.debug(
                    "NapCat redirected queued final reply anchor: chat=%s %s -> %s",
                    chat_key,
                    explicit,
                    staged,
                )
                return staged
            return explicit

        # Inline delivery before a queued follow-up is processed.  If a previous
        # queued turn is now producing its own intermediate first response, use
        # its staged anchor; otherwise use the currently active/original anchor.
        anchor = self._next_final_reply_anchors.pop(chat_key, None) or active_anchor
        if not notify:
            staged = self._stage_next_final_reply_anchor(chat_key)
            if staged and anchor:
                self._inline_completion_reply_anchors[chat_key] = str(anchor)
        return anchor

    def _reply_segment_for(
        self,
        chat_id: str,
        reply_to: str | None = None,
        metadata: dict | None = None,
    ) -> dict | None:
        anchor = self._effective_reply_to(chat_id, reply_to, metadata)
        if not anchor:
            return None
        try:
            return reply_segment(int(anchor))
        except (ValueError, TypeError):
            return None

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
            progress_only = _is_non_final_progress_message(content, metadata)
            # Heartbeats/status bubbles are informational side-channel messages,
            # not answers.  Never quote the active QQ message and never let them
            # consume queued-reply completion state or trigger done/poke actions.
            effective_reply_to = (
                None
                if progress_only
                else self._effective_reply_to(chat_id, reply_to, metadata)
            )
            chunks = _chunk_text(_strip_markdown(content))
            last_id: str | None = None
            for i, chunk in enumerate(chunks):
                segs: list[dict] = []
                if i == 0 and effective_reply_to:
                    try:
                        segs.append(reply_segment(int(effective_reply_to)))
                    except (ValueError, TypeError):
                        pass
                segs.append(text_segment(chunk))
                if is_group:
                    r = await send_group_msg(self._http_api, num_id, segs, self._access_token or None)
                else:
                    r = await send_private_msg(self._http_api, num_id, segs, self._access_token or None)
                last_id = str(r.get("message_id", ""))
            if not progress_only:
                inline_completion_anchor = self._inline_completion_reply_anchors.pop(str(chat_id), None)
                if inline_completion_anchor and effective_reply_to and inline_completion_anchor == str(effective_reply_to):
                    await self._clear_processing(inline_completion_anchor)
                    await self._mark_post_response(inline_completion_anchor)
                await self._poke_after_reply(effective_reply_to)
            return SendResult(success=True, message_id=last_id)
        except Exception as exc:
            logger.error("NapCat send error: %s", exc)
            return SendResult(success=False, error=str(exc), retryable=True)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: str | None = None,
        reply_to: str | None = None,
        metadata: dict | None = None,
    ) -> SendResult:
        try:
            is_group, num_id = self._parse_chat_id(chat_id)
            segs: list[dict] = []
            reply_seg = self._reply_segment_for(chat_id, reply_to, metadata)
            if reply_seg:
                segs.append(reply_seg)
            segs.append(image_segment(image_url))
            if caption:
                segs.append(text_segment(caption))
            if is_group:
                r = await send_group_msg(self._http_api, num_id, segs, self._access_token or None)
            else:
                r = await send_private_msg(self._http_api, num_id, segs, self._access_token or None)
            return SendResult(success=True, message_id=str(r.get("message_id", "")))
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: str | None = None,
        reply_to: str | None = None,
        metadata: dict | None = None,
    ) -> SendResult:
        """Send a Hermes-local image by converting it to a base64 OneBot ref."""
        return await self.send_image(
            chat_id=chat_id,
            image_url=image_path,
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        reply_to: str | None = None,
        metadata: dict | None = None,
    ) -> SendResult:
        try:
            is_group, num_id = self._parse_chat_id(chat_id)
            segs = []
            reply_seg = self._reply_segment_for(chat_id, reply_to, metadata)
            if reply_seg:
                segs.append(reply_seg)
            segs.append(record_segment(audio_path))
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
        reply_to: str | None = None,
        metadata: dict | None = None,
    ) -> SendResult:
        try:
            is_group, num_id = self._parse_chat_id(chat_id)
            segs = []
            reply_seg = self._reply_segment_for(chat_id, reply_to, metadata)
            if reply_seg:
                segs.append(reply_seg)
            segs.append(video_segment(video_path))
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
        "http_api", "access_token", "self_id", "ws_port", "ws_host", "ws_access_token",
        "ws_allowed_ips", "ws_max_message_bytes", "ws_heartbeat_seconds", "ws_max_inflight",
        "media_max_mb",
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
