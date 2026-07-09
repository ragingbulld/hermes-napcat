"""OneBot 11 HTTP API async client."""
from __future__ import annotations

import base64
import asyncio
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import logging
import os
from pathlib import Path
import shutil
import socket
import tempfile
import threading
from urllib.parse import quote, unquote, urlparse
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=10)


def _env_mb_to_bytes(name: str, default_mb: int) -> int:
    raw = os.getenv(name, str(default_mb)).strip()
    try:
        mb = int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using %s MB", name, raw, default_mb)
        mb = default_mb
    return max(0, mb) * 1024 * 1024


_MAX_BASE64_MEDIA_BYTES = _env_mb_to_bytes("NAPCAT_BASE64_MEDIA_MAX_MB", 20)
# 0 disables the local pre-check. 100 MB is a conservative QQ-friendly default;
# actual account/group limits may still be lower and are enforced by QQ/NapCat.
_MAX_UPLOAD_BYTES = _env_mb_to_bytes("NAPCAT_UPLOAD_MAX_MB", 100)
_MEDIA_SEGMENT_TYPES = {"image", "record", "video"}


def normalize_media_reference(file_ref: str, *, max_bytes: int | None = None) -> str:
    """Return a NapCat-friendly media reference.

    Hermes and NapCat can run on different hosts/containers in this deployment,
    so a Hermes-local path like ``/tmp/a.png`` may be unreadable to NapCat.
    OneBot media segments accept ``base64://...``; convert existing local files
    to that form while leaving URLs, existing base64 refs, and unknown paths as-is.
    """
    ref = str(file_ref or "")
    if not ref:
        return ref
    lowered = ref.lower()
    if lowered.startswith(("http://", "https://", "base64://")):
        return ref
    path = _local_path_from_ref(ref)
    try:
        if not path.is_file():
            return ref
        limit = max_bytes if max_bytes is not None else _MAX_BASE64_MEDIA_BYTES
        size = path.stat().st_size
        if limit and size > limit:
            logger.warning(
                "NapCat media file too large for base64 conversion (%s bytes > %s): %s",
                size,
                limit,
                path,
            )
            return ref
        return "base64://" + base64.b64encode(path.read_bytes()).decode("ascii")
    except Exception as exc:
        logger.debug("NapCat media base64 conversion skipped for %r: %s", ref, exc)
        return ref


def _is_timeout_like(exc: Exception) -> bool:
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return True
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    return "timeout" in name or "timed out" in text or "timeout" in text


async def _download_url_as_base64_ref(url: str, *, max_bytes: int | None = None) -> str | None:
    """Fetch a remote media URL and return a OneBot base64:// reference."""
    limit = max_bytes if max_bytes is not None else _MAX_BASE64_MEDIA_BYTES
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                chunks: list[bytes] = []
                total = 0
                async for chunk in resp.content.iter_chunked(1024 * 256):
                    total += len(chunk)
                    if limit and total > limit:
                        logger.warning(
                            "NapCat remote media too large for URL->base64 fallback (%s bytes > %s): %s",
                            total,
                            limit,
                            url,
                        )
                        return None
                    chunks.append(chunk)
        return "base64://" + base64.b64encode(b"".join(chunks)).decode("ascii")
    except Exception as exc:
        logger.debug("NapCat URL->base64 fallback skipped for %s: %s", url, exc)
        return None


def _clone_message_with_replaced_files(message: Any, replacements: dict[str, str]) -> Any:
    if not isinstance(message, list) or not replacements:
        return message
    cloned: list[Any] = []
    for segment in message:
        if not isinstance(segment, dict):
            cloned.append(segment)
            continue
        seg = dict(segment)
        seg_type = str(seg.get("type") or "").lower()
        data = seg.get("data")
        if seg_type in _MEDIA_SEGMENT_TYPES and isinstance(data, dict):
            new_data = dict(data)
            file_ref = str(new_data.get("file") or "")
            if file_ref in replacements:
                new_data["file"] = replacements[file_ref]
                seg["data"] = new_data
        cloned.append(seg)
    return cloned


async def _remote_media_url_replacements(message: Any) -> dict[str, str]:
    if not isinstance(message, list):
        return {}
    replacements: dict[str, str] = {}
    for segment in message:
        if not isinstance(segment, dict):
            continue
        if str(segment.get("type") or "").lower() not in _MEDIA_SEGMENT_TYPES:
            continue
        data = segment.get("data")
        if not isinstance(data, dict):
            continue
        file_ref = str(data.get("file") or "")
        if not file_ref.lower().startswith(("http://", "https://")):
            continue
        if file_ref in replacements:
            continue
        base64_ref = await _download_url_as_base64_ref(file_ref)
        if base64_ref:
            replacements[file_ref] = base64_ref
    return replacements


def _prepare_local_media_url_replacements(message: Any, temp_dir: str) -> dict[str, str]:
    if not isinstance(message, list):
        return {}
    replacements: dict[str, str] = {}
    used_names: set[str] = set()
    for segment in message:
        if not isinstance(segment, dict):
            continue
        if str(segment.get("type") or "").lower() not in _MEDIA_SEGMENT_TYPES:
            continue
        data = segment.get("data")
        if not isinstance(data, dict):
            continue
        file_ref = str(data.get("file") or "")
        lowered = file_ref.lower()
        if not file_ref or lowered.startswith(("http://", "https://", "base64://")):
            continue
        path = _local_path_from_ref(file_ref)
        if not path.is_file() or file_ref in replacements:
            continue
        name = path.name or "media"
        if name in used_names:
            stem = path.stem or "media"
            suffix = path.suffix
            i = 2
            while f"{stem}-{i}{suffix}" in used_names:
                i += 1
            name = f"{stem}-{i}{suffix}"
        used_names.add(name)
        target = Path(temp_dir) / name
        try:
            os.symlink(path, target)
        except Exception:
            shutil.copy2(path, target)
        replacements[file_ref] = name
    return replacements


class _QuietFileHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003 - stdlib signature
        logger.debug("NapCat temp file server: " + format, *args)


def _local_path_from_ref(file_ref: str) -> Path:
    ref = str(file_ref or "")
    if ref.lower().startswith("file://"):
        parsed = urlparse(ref)
        return Path(unquote(parsed.path or "")).expanduser()
    return Path(ref).expanduser()


def _is_remote_http_api(base_url: str) -> bool:
    host = (urlparse(base_url or "").hostname or "").lower()
    return bool(host and host not in {"localhost", "127.0.0.1", "::1"})


def _safe_served_name(name: str | None, fallback: str = "file") -> str:
    safe = Path(str(name or "")).name.strip()
    return safe or fallback


def _validate_upload_local_file(local_path: Path) -> int:
    if not local_path.exists():
        raise FileNotFoundError(f"file not found: {local_path}")
    if not local_path.is_file():
        raise ValueError(f"not a regular file: {local_path}")
    size = local_path.stat().st_size
    if size <= 0:
        raise ValueError(f"file is empty: {local_path}")
    if _MAX_UPLOAD_BYTES and size > _MAX_UPLOAD_BYTES:
        raise ValueError(
            f"file too large for configured NapCat upload limit: "
            f"{size} bytes > {_MAX_UPLOAD_BYTES} bytes "
            f"(set NAPCAT_UPLOAD_MAX_MB=0 to disable the local pre-check)"
        )
    return size


async def _download_url_to_file(
    url: str,
    target: Path,
    *,
    timeout: float = 60,
) -> int:
    total = 0
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            with target.open("wb") as fh:
                async for chunk in resp.content.iter_chunked(1024 * 256):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if _MAX_UPLOAD_BYTES and total > _MAX_UPLOAD_BYTES:
                        raise ValueError(
                            f"remote file too large for configured NapCat upload limit: "
                            f"{total} bytes > {_MAX_UPLOAD_BYTES} bytes"
                        )
                    fh.write(chunk)
    if total <= 0:
        raise ValueError(f"remote file is empty: {url}")
    return total


def _copy_or_link_single_file(local_path: Path, temp_dir: str, served_name: str) -> str:
    name = _safe_served_name(served_name, local_path.name or "file")
    target = Path(temp_dir) / name
    if target.exists():
        stem = target.stem or "file"
        suffix = target.suffix
        i = 2
        while target.exists():
            target = Path(temp_dir) / f"{stem}-{i}{suffix}"
            i += 1
        name = target.name
    try:
        os.symlink(local_path, target)
    except Exception:
        shutil.copy2(local_path, target)
    return name


async def _call_upload_with_temp_url(
    base_url: str,
    action: str,
    params: dict[str, Any],
    access_token: str | None,
    timeout: float,
    *,
    local_path: Path,
    served_name: str,
    file_key: str = "file",
    method: str = "temp_url",
) -> dict[str, Any]:
    _validate_upload_local_file(local_path)
    with tempfile.TemporaryDirectory(prefix="napcat-upload-") as temp_dir:
        exposed_name = _copy_or_link_single_file(local_path, temp_dir, served_name)
        handler = partial(_QuietFileHandler, directory=temp_dir)
        server = ThreadingHTTPServer(("0.0.0.0", 0), handler)
        thread = threading.Thread(target=server.serve_forever, name="napcat-file-upload", daemon=True)
        thread.start()
        try:
            retry_params = dict(params)
            retry_params[file_key] = (
                f"http://{local_route_host_for(base_url)}:{server.server_port}/"
                f"{quote(exposed_name)}"
            )
            resp = await call_onebot_api(base_url, action, retry_params, access_token, timeout=timeout)
            resp["_hermes_upload_method"] = method
            resp["_hermes_upload_size"] = local_path.stat().st_size
            resp["_hermes_upload_name"] = _safe_served_name(served_name, local_path.name)
            return resp
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


def local_route_host_for(remote_url: str) -> str:
    """Return the local address likely reachable by the NapCat host."""
    override = os.getenv("NAPCAT_PUBLIC_MEDIA_HOST") or os.getenv("HERMES_PUBLIC_MEDIA_HOST")
    if override:
        return override.strip()
    parsed = urlparse(remote_url or "")
    host = parsed.hostname
    if not host:
        return "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect((host, port))
        return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        sock.close()


async def call_onebot_api(
    base_url: str,
    action: str,
    params: dict[str, Any] | None = None,
    access_token: str | None = None,
    timeout: float = 10,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/{action}"
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=timeout)
    ) as session:
        async with session.post(url, json=params or {}, headers=headers) as resp:
            resp.raise_for_status()
            data: dict[str, Any] = await resp.json()
            if data.get("retcode", 0) != 0:
                raise RuntimeError(
                    f"OneBot API error {action}: retcode={data.get('retcode')} status={data.get('status')}"
                )
            return data


async def call_onebot_api_with_local_file_url_fallback(
    base_url: str,
    action: str,
    params: dict[str, Any],
    access_token: str | None = None,
    timeout: float = 60,
    *,
    file_key: str = "file",
) -> dict[str, Any]:
    """Call a file-upload API with safer cross-host fallbacks.

    For remote NapCat HTTP APIs, readable Hermes-local files go straight through
    a short-lived single-file HTTP URL instead of first waiting for a guaranteed
    path-read failure.  Timeout-like errors are never retried because QQ/NapCat
    may have accepted the upload already.  Remote URLs keep the normal NapCat
    path first; if NapCat cannot fetch them, Hermes downloads the URL to a temp
    file and exposes that single file back to NapCat.
    """
    file_ref = str((params or {}).get(file_key) or "")
    display_name = _safe_served_name(str((params or {}).get("name") or ""), "file")
    lowered = file_ref.lower()

    if not file_ref:
        raise ValueError(f"missing upload parameter: {file_key}")

    if lowered.startswith(("http://", "https://")):
        try:
            resp = await call_onebot_api(base_url, action, params, access_token, timeout=timeout)
            resp["_hermes_upload_method"] = "remote_url"
            return resp
        except Exception as first_exc:
            if _is_timeout_like(first_exc):
                raise
            with tempfile.TemporaryDirectory(prefix="napcat-url-upload-") as temp_dir:
                parsed_name = _safe_served_name(unquote(urlparse(file_ref).path), display_name)
                local_path = Path(temp_dir) / parsed_name
                try:
                    await _download_url_to_file(file_ref, local_path, timeout=timeout)
                except Exception:
                    raise first_exc
                return await _call_upload_with_temp_url(
                    base_url,
                    action,
                    params,
                    access_token,
                    timeout,
                    local_path=local_path,
                    served_name=display_name if display_name != "file" else parsed_name,
                    file_key=file_key,
                    method="downloaded_url_temp_url",
                )

    local_path = _local_path_from_ref(file_ref)
    if local_path.exists():
        _validate_upload_local_file(local_path)
        if _is_remote_http_api(base_url):
            return await _call_upload_with_temp_url(
                base_url,
                action,
                params,
                access_token,
                timeout,
                local_path=local_path,
                served_name=display_name if display_name != "file" else local_path.name,
                file_key=file_key,
                method="temp_url_direct",
            )

    try:
        resp = await call_onebot_api(base_url, action, params, access_token, timeout=timeout)
        resp["_hermes_upload_method"] = "direct"
        if local_path.exists():
            resp["_hermes_upload_size"] = local_path.stat().st_size
            resp["_hermes_upload_name"] = display_name if display_name != "file" else local_path.name
        return resp
    except Exception as first_exc:
        if _is_timeout_like(first_exc):
            raise
        if not local_path.is_file():
            raise first_exc
        return await _call_upload_with_temp_url(
            base_url,
            action,
            params,
            access_token,
            timeout,
            local_path=local_path,
            served_name=display_name if display_name != "file" else local_path.name,
            file_key=file_key,
            method="temp_url_retry",
        )


async def call_onebot_api_with_media_fallback(
    base_url: str,
    action: str,
    params: dict[str, Any],
    access_token: str | None = None,
    timeout: float = 30,
) -> dict[str, Any]:
    """Call a message-send API, then retry media refs through safer forms.

    Timeout-like failures are not retried to avoid duplicate QQ messages.  On
    parameter/fetch failures, remote media URLs are retried as base64:// refs;
    remaining readable local paths are retried through a short-lived HTTP URL.
    """
    saved_first_exc: Exception | None = None
    try:
        return await call_onebot_api(base_url, action, params, access_token, timeout=timeout)
    except Exception as first_exc:
        if _is_timeout_like(first_exc):
            raise
        saved_first_exc = first_exc
        message = (params or {}).get("message")

    remote_replacements = await _remote_media_url_replacements(message)
    if remote_replacements:
        retry_params = dict(params)
        retry_params["message"] = _clone_message_with_replaced_files(message, remote_replacements)
        try:
            return await call_onebot_api(base_url, action, retry_params, access_token, timeout=timeout)
        except Exception as second_exc:
            if _is_timeout_like(second_exc):
                raise
            logger.debug("NapCat URL->base64 media fallback failed for %s: %s", action, second_exc)

    with tempfile.TemporaryDirectory(prefix="napcat-media-") as temp_dir:
        local_names = _prepare_local_media_url_replacements(message, temp_dir)
        if not local_names:
            if saved_first_exc is not None:
                raise saved_first_exc
            raise RuntimeError(f"OneBot API error {action}: media fallback unavailable")
        handler = partial(_QuietFileHandler, directory=temp_dir)
        server = ThreadingHTTPServer(("0.0.0.0", 0), handler)
        thread = threading.Thread(target=server.serve_forever, name="napcat-media-send", daemon=True)
        thread.start()
        try:
            host = local_route_host_for(base_url)
            replacements = {
                original: f"http://{host}:{server.server_port}/{quote(name)}"
                for original, name in local_names.items()
            }
            retry_params = dict(params)
            retry_params["message"] = _clone_message_with_replaced_files(message, replacements)
            return await call_onebot_api(base_url, action, retry_params, access_token, timeout=timeout)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


async def get_login_info(base_url: str, access_token: str | None = None) -> dict[str, Any]:
    resp = await call_onebot_api(base_url, "get_login_info", access_token=access_token)
    return resp["data"]


async def send_private_msg(
    base_url: str,
    user_id: int,
    message: list[dict],
    access_token: str | None = None,
) -> dict[str, Any]:
    resp = await call_onebot_api_with_media_fallback(
        base_url, "send_private_msg",
        {"user_id": user_id, "message": message},
        access_token=access_token,
    )
    return resp["data"]


async def set_input_status(
    base_url: str,
    user_id: int,
    event_type: int = 1,
    access_token: str | None = None,
) -> dict[str, Any]:
    """Set QQ private-chat input status, e.g. “正在输入中”."""
    resp = await call_onebot_api(
        base_url,
        "set_input_status",
        {"user_id": user_id, "event_type": event_type},
        access_token=access_token,
    )
    return resp.get("data", {})


async def send_group_msg(
    base_url: str,
    group_id: int,
    message: list[dict],
    access_token: str | None = None,
) -> dict[str, Any]:
    resp = await call_onebot_api_with_media_fallback(
        base_url, "send_group_msg",
        {"group_id": group_id, "message": message},
        access_token=access_token,
    )
    return resp["data"]


async def get_msg(
    base_url: str,
    message_id: int,
    access_token: str | None = None,
) -> dict[str, Any]:
    resp = await call_onebot_api(
        base_url, "get_msg",
        {"message_id": message_id},
        access_token=access_token,
    )
    return resp["data"]


async def upload_group_file(
    base_url: str,
    group_id: int,
    file: str,
    name: str,
    access_token: str | None = None,
) -> dict[str, Any]:
    resp = await call_onebot_api_with_local_file_url_fallback(
        base_url,
        "upload_group_file",
        {"group_id": group_id, "file": file, "name": name},
        access_token=access_token,
        timeout=60,
    )
    return resp.get("data", {})


async def upload_private_file(
    base_url: str,
    user_id: int,
    file: str,
    name: str,
    access_token: str | None = None,
) -> dict[str, Any]:
    resp = await call_onebot_api_with_local_file_url_fallback(
        base_url,
        "upload_private_file",
        {"user_id": user_id, "file": file, "name": name},
        access_token=access_token,
        timeout=60,
    )
    return resp.get("data", {})


# ---------- segment builders ----------

def text_segment(text: str) -> dict:
    return {"type": "text", "data": {"text": text}}

def image_segment(file_url: str) -> dict:
    return {"type": "image", "data": {"file": normalize_media_reference(file_url)}}

def at_segment(qq: int | str) -> dict:
    return {"type": "at", "data": {"qq": str(qq)}}

def reply_segment(message_id: int | str) -> dict:
    return {"type": "reply", "data": {"id": str(message_id)}}

def record_segment(file_url: str) -> dict:
    return {"type": "record", "data": {"file": normalize_media_reference(file_url)}}

def video_segment(file_url: str) -> dict:
    return {"type": "video", "data": {"file": normalize_media_reference(file_url)}}
