"""OneBot 11 HTTP API async client."""
from __future__ import annotations

import base64
import asyncio
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import logging
import os
from pathlib import Path
import secrets
import shutil
import socket
import tempfile
import threading
from urllib.parse import quote, unquote, urljoin, urlparse
from typing import Any, Optional

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult

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
# 0 disables only the pre-check for trusted local paths. Remote downloads always
# retain a separate positive hard limit to prevent unbounded disk writes.
_MAX_UPLOAD_BYTES = _env_mb_to_bytes("NAPCAT_UPLOAD_MAX_MB", 100)
_MAX_REMOTE_DOWNLOAD_BYTES = _env_mb_to_bytes("NAPCAT_REMOTE_DOWNLOAD_MAX_MB", 100)
if _MAX_REMOTE_DOWNLOAD_BYTES <= 0:
    logger.warning("NAPCAT_REMOTE_DOWNLOAD_MAX_MB must be positive; using 100 MB")
    _MAX_REMOTE_DOWNLOAD_BYTES = 100 * 1024 * 1024
_MEDIA_SEGMENT_TYPES = {"image", "record", "video"}


def _remote_upload_limit() -> int:
    if _MAX_UPLOAD_BYTES > 0:
        return min(_MAX_UPLOAD_BYTES, _MAX_REMOTE_DOWNLOAD_BYTES)
    return _MAX_REMOTE_DOWNLOAD_BYTES


def _is_public_ip(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Return True only for globally routable media destinations."""
    return bool(address.is_global)


class _PublicOnlyResolver(AbstractResolver):
    """Resolve hosts at connection time and reject every non-public result."""

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: int = socket.AF_UNSPEC,
    ) -> list[ResolveResult]:
        infos = await asyncio.get_running_loop().getaddrinfo(
            host,
            port,
            family=family,
            type=socket.SOCK_STREAM,
        )
        results: list[ResolveResult] = []
        for resolved_family, _, proto, _, sockaddr in infos:
            address = ipaddress.ip_address(sockaddr[0])
            if not _is_public_ip(address):
                raise ValueError(f"media connection resolved to a non-public address: {host}")
            results.append(
                {
                    "hostname": host,
                    "host": str(address),
                    "port": port,
                    "family": resolved_family,
                    "proto": proto,
                    "flags": socket.AI_NUMERICHOST,
                }
            )
        if not results:
            raise ValueError(f"media hostname did not resolve: {host}")
        return results

    async def close(self) -> None:
        return None


async def ensure_public_http_url(url: str) -> None:
    """Reject non-HTTP and non-public destinations before Hermes fetches media."""
    parsed = urlparse(str(url or ""))
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("media URL must use http(s) with a hostname")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("media URL must not contain embedded credentials")
    port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    try:
        literal = ipaddress.ip_address(parsed.hostname)
        addresses = [literal]
    except ValueError:
        infos = await asyncio.get_running_loop().getaddrinfo(
            parsed.hostname,
            port,
            type=socket.SOCK_STREAM,
        )
        addresses = []
        for info in infos:
            try:
                addresses.append(ipaddress.ip_address(info[4][0]))
            except (ValueError, IndexError):
                continue
    if not addresses or any(not _is_public_ip(address) for address in addresses):
        raise ValueError(f"media URL resolves to a non-public address: {parsed.hostname}")


def _headers_for_redirect(
    headers: dict[str, str],
    current_url: str,
    next_url: str,
) -> dict[str, str]:
    """Drop credentials when a manually followed redirect changes origin."""
    current = urlparse(current_url)
    target = urlparse(next_url)

    def origin(parsed: Any) -> tuple[str, str, int]:
        scheme = parsed.scheme.lower()
        port = parsed.port or (443 if scheme == "https" else 80)
        return scheme, (parsed.hostname or "").lower(), port

    if origin(current) == origin(target):
        return dict(headers)
    # Arbitrary caller headers may contain credentials under application-specific
    # names (for example X-Api-Key). Never forward any of them across origins.
    return {}


async def download_public_url_bytes(
    url: str,
    *,
    max_bytes: int,
    timeout: float = 25,
    max_redirects: int = 3,
) -> bytes:
    """Stream a public HTTP(S) URL into bounded memory, validating redirects."""
    current = str(url or "")
    timeout_cfg = aiohttp.ClientTimeout(total=timeout, connect=min(10, timeout), sock_read=min(15, timeout))
    connector = aiohttp.TCPConnector(resolver=_PublicOnlyResolver(), use_dns_cache=False)
    async with aiohttp.ClientSession(timeout=timeout_cfg, connector=connector) as session:
        for redirect_count in range(max_redirects + 1):
            await ensure_public_http_url(current)
            async with session.get(current, allow_redirects=False) as resp:
                if resp.status in {301, 302, 303, 307, 308}:
                    location = resp.headers.get("Location")
                    if not location or redirect_count >= max_redirects:
                        raise ValueError("media URL redirect limit exceeded")
                    current = urljoin(current, location)
                    continue
                resp.raise_for_status()
                content_length = resp.headers.get("Content-Length")
                if content_length and int(content_length) > max_bytes:
                    raise ValueError("remote media exceeds configured size limit")
                data = bytearray()
                async for chunk in resp.content.iter_chunked(256 * 1024):
                    data.extend(chunk)
                    if len(data) > max_bytes:
                        raise ValueError("remote media exceeds configured size limit")
                if not data:
                    raise ValueError("remote media is empty")
                return bytes(data)
    raise ValueError("media URL could not be downloaded")


def normalize_media_reference(file_ref: str, *, max_bytes: int | None = None) -> str:
    """Return a NapCat-friendly media reference.

    Hermes and NapCat may run on different hosts or in separate containers,
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
    """Fetch a public remote media URL and return a bounded base64:// reference."""
    limit = max_bytes if max_bytes is not None else _MAX_BASE64_MEDIA_BYTES
    try:
        data = await download_public_url_bytes(url, max_bytes=limit, timeout=25)
        return "base64://" + base64.b64encode(data).decode("ascii")
    except Exception as exc:
        logger.debug("NapCat URL->base64 fallback skipped for %s: %s", url, exc)
        return None


def _iter_media_file_refs(value: Any):
    if isinstance(value, list):
        for item in value:
            yield from _iter_media_file_refs(item)
        return
    if not isinstance(value, dict):
        return
    if str(value.get("type") or "").lower() in _MEDIA_SEGMENT_TYPES:
        data = value.get("data")
        if isinstance(data, dict):
            file_ref = str(data.get("file") or "")
            if file_ref:
                yield file_ref
    for nested in value.values():
        yield from _iter_media_file_refs(nested)


def _clone_message_with_replaced_files(message: Any, replacements: dict[str, str]) -> Any:
    if not replacements:
        return message
    if isinstance(message, list):
        return [_clone_message_with_replaced_files(item, replacements) for item in message]
    if not isinstance(message, dict):
        return message
    cloned = {
        key: _clone_message_with_replaced_files(value, replacements)
        for key, value in message.items()
    }
    if str(cloned.get("type") or "").lower() in _MEDIA_SEGMENT_TYPES:
        data = cloned.get("data")
        if isinstance(data, dict):
            file_ref = str(data.get("file") or "")
            if file_ref in replacements:
                new_data = dict(data)
                new_data["file"] = replacements[file_ref]
                cloned["data"] = new_data
    return cloned


async def _remote_media_url_replacements(message: Any) -> dict[str, str]:
    replacements: dict[str, str] = {}
    for file_ref in _iter_media_file_refs(message):
        if not file_ref.lower().startswith(("http://", "https://")):
            continue
        if file_ref in replacements:
            continue
        base64_ref = await _download_url_as_base64_ref(file_ref)
        if not base64_ref:
            raise ValueError(f"remote media URL could not be fetched safely: {file_ref}")
        replacements[file_ref] = base64_ref
    return replacements


def _local_media_reference_replacements(message: Any) -> dict[str, str]:
    """Validate readable local media and inline bounded files as base64."""
    replacements: dict[str, str] = {}
    for file_ref in _iter_media_file_refs(message):
        lowered = file_ref.lower()
        if not file_ref or lowered.startswith(("http://", "https://", "base64://")):
            continue
        path = _local_path_from_ref(file_ref)
        if not path.is_file() or file_ref in replacements:
            continue
        _validate_upload_local_file(path)
        normalized = normalize_media_reference(file_ref)
        if normalized != file_ref:
            replacements[file_ref] = normalized
    return replacements


def _prepare_local_media_url_replacements(message: Any, temp_dir: str) -> dict[str, str]:
    replacements: dict[str, str] = {}
    used_names: set[str] = set()
    for file_ref in _iter_media_file_refs(message):
        lowered = file_ref.lower()
        if not file_ref or lowered.startswith(("http://", "https://", "base64://")):
            continue
        path = _local_path_from_ref(file_ref)
        if not path.is_file() or file_ref in replacements:
            continue
        _validate_upload_local_file(path)
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
    def __init__(self, *args: Any, allowed_paths: set[str] | None = None, **kwargs: Any) -> None:
        self._allowed_paths = set(allowed_paths or ())
        super().__init__(*args, **kwargs)

    def _path_is_allowed(self) -> bool:
        return unquote(urlparse(self.path).path) in self._allowed_paths

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        if not self._path_is_allowed():
            self.send_error(404)
            return
        super().do_GET()

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler API
        if not self._path_is_allowed():
            self.send_error(404)
            return
        super().do_HEAD()

    def list_directory(self, path: Any) -> None:
        del path
        self.send_error(404)
        return None

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
    request_headers: dict[str, str] | None = None,
) -> int:
    current = str(url or "")
    headers = dict(request_headers or {})
    byte_limit = _remote_upload_limit()
    timeout_cfg = aiohttp.ClientTimeout(
        total=timeout,
        connect=min(10, timeout),
        sock_read=min(30, timeout),
    )
    connector = aiohttp.TCPConnector(resolver=_PublicOnlyResolver(), use_dns_cache=False)
    async with aiohttp.ClientSession(timeout=timeout_cfg, connector=connector) as session:
        for redirect_count in range(4):
            await ensure_public_http_url(current)
            async with session.get(current, headers=headers, allow_redirects=False) as resp:
                if resp.status in {301, 302, 303, 307, 308}:
                    location = resp.headers.get("Location")
                    if not location or redirect_count >= 3:
                        raise ValueError("remote file redirect limit exceeded")
                    next_url = urljoin(current, location)
                    headers = _headers_for_redirect(headers, current, next_url)
                    current = next_url
                    continue
                resp.raise_for_status()
                content_length = resp.headers.get("Content-Length")
                if content_length and int(content_length) > byte_limit:
                    raise ValueError("remote file exceeds configured download limit")
                total = 0
                with target.open("wb") as fh:
                    async for chunk in resp.content.iter_chunked(1024 * 256):
                        if not chunk:
                            continue
                        total += len(chunk)
                        if total > byte_limit:
                            raise ValueError(
                                f"remote file too large for configured download limit: "
                                f"{total} bytes > {byte_limit} bytes"
                            )
                        fh.write(chunk)
                if total <= 0:
                    raise ValueError(f"remote file is empty: {current}")
                return total
    raise ValueError("remote file could not be downloaded")


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
        token = secrets.token_urlsafe(24)
        token_dir = Path(temp_dir) / token
        token_dir.mkdir(mode=0o700)
        exposed_name = _copy_or_link_single_file(local_path, str(token_dir), served_name)
        allowed_path = f"/{token}/{exposed_name}"
        handler = partial(
            _QuietFileHandler,
            directory=temp_dir,
            allowed_paths={allowed_path},
        )
        bind_host = local_bind_host_for(base_url)
        advertise_host = public_media_host_for(base_url)
        server = ThreadingHTTPServer((bind_host, 0), handler)
        thread = threading.Thread(target=server.serve_forever, name="napcat-file-upload", daemon=True)
        thread.start()
        try:
            retry_params = dict(params)
            retry_params[file_key] = (
                f"http://{advertise_host}:{server.server_port}/{token}/"
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


def _route_local_address(remote_url: str) -> str:
    """Return the local interface address used to reach a remote HTTP API."""
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


def _local_interface_host_for(remote_url: str) -> str:
    """Backward-compatible local bind-address helper."""
    return _route_local_address(remote_url)


def public_media_host_for(remote_url: str) -> str:
    """Return the hostname advertised to the remote NapCat process."""
    override = os.getenv("NAPCAT_PUBLIC_MEDIA_HOST") or os.getenv("HERMES_PUBLIC_MEDIA_HOST")
    return override.strip() if override else _route_local_address(remote_url)


def local_bind_host_for(remote_url: str) -> str:
    """Return a local interface address suitable for ThreadingHTTPServer.bind."""
    return _route_local_address(remote_url)


def local_route_host_for(remote_url: str) -> str:
    """Backward-compatible alias for the advertised media hostname."""
    return public_media_host_for(remote_url)


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
    download_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Call a file-upload API with safer cross-host fallbacks.

    For remote NapCat HTTP APIs, readable Hermes-local files go straight through
    a short-lived single-file HTTP URL instead of first waiting for a guaranteed
    path-read failure.  Remote URLs are first downloaded by Hermes with public-
    address and size checks, then exposed as one short-lived exact-path URL.
    Timeout-like errors are never retried because QQ/NapCat may already have
    accepted the upload.
    """
    file_ref = str((params or {}).get(file_key) or "")
    display_name = _safe_served_name(str((params or {}).get("name") or ""), "file")
    lowered = file_ref.lower()

    if not file_ref:
        raise ValueError(f"missing upload parameter: {file_key}")

    if lowered.startswith(("http://", "https://")):
        with tempfile.TemporaryDirectory(prefix="napcat-url-upload-") as temp_dir:
            parsed_name = _safe_served_name(unquote(urlparse(file_ref).path), display_name)
            local_path = Path(temp_dir) / parsed_name
            await _download_url_to_file(
                file_ref,
                local_path,
                timeout=timeout,
                request_headers=download_headers,
            )
            safe_params = dict(params)
            safe_params.pop("headers", None)
            return await _call_upload_with_temp_url(
                base_url,
                action,
                safe_params,
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
    *,
    message_key: str = "message",
) -> dict[str, Any]:
    """Call a message API without exposing untrusted URLs or local paths.

    Remote URLs are downloaded through the public-only resolver and replaced by
    bounded base64 refs. Readable local files are validated against the upload
    limit; small files become base64 refs and larger allowed files are exposed
    through one short-lived exact-path URL before the first NapCat call.
    """
    message = (params or {}).get(message_key)
    remote_replacements = await _remote_media_url_replacements(message)
    safe_message = _clone_message_with_replaced_files(message, remote_replacements)
    local_replacements = _local_media_reference_replacements(safe_message)
    safe_message = _clone_message_with_replaced_files(safe_message, local_replacements)
    safe_params = dict(params)
    safe_params[message_key] = safe_message

    with tempfile.TemporaryDirectory(prefix="napcat-media-") as temp_dir:
        token = secrets.token_urlsafe(24)
        token_dir = Path(temp_dir) / token
        token_dir.mkdir(mode=0o700)
        local_names = _prepare_local_media_url_replacements(safe_message, str(token_dir))
        if not local_names:
            return await call_onebot_api(
                base_url,
                action,
                safe_params,
                access_token,
                timeout=timeout,
            )

        allowed_paths = {f"/{token}/{name}" for name in local_names.values()}
        handler = partial(
            _QuietFileHandler,
            directory=temp_dir,
            allowed_paths=allowed_paths,
        )
        bind_host = local_bind_host_for(base_url)
        advertise_host = public_media_host_for(base_url)
        server = ThreadingHTTPServer((bind_host, 0), handler)
        thread = threading.Thread(target=server.serve_forever, name="napcat-media-send", daemon=True)
        thread.start()
        try:
            replacements = {
                original: f"http://{advertise_host}:{server.server_port}/{token}/{quote(name)}"
                for original, name in local_names.items()
            }
            url_params = dict(safe_params)
            url_params[message_key] = _clone_message_with_replaced_files(
                safe_message,
                replacements,
            )
            return await call_onebot_api(
                base_url,
                action,
                url_params,
                access_token,
                timeout=timeout,
            )
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
