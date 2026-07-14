import asyncio
import aiohttp
import aiohttp.web
from aiohttp.test_utils import TestClient, TestServer
import importlib.util
import ipaddress
import os
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from functools import partial
from unittest.mock import AsyncMock, patch


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_NAME = "hermes_napcat_testpkg"


def _load_module(name: str):
    if PACKAGE_NAME not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            PACKAGE_NAME,
            PLUGIN_ROOT / "__init__.py",
            submodule_search_locations=[str(PLUGIN_ROOT)],
        )
        assert spec is not None and spec.loader is not None
        package = importlib.util.module_from_spec(spec)
        sys.modules[PACKAGE_NAME] = package
        spec.loader.exec_module(package)
    return __import__(f"{PACKAGE_NAME}.{name}", fromlist=[name])


class ReverseWebSocketSecurityTests(unittest.TestCase):
    def test_accepts_matching_bearer_token(self):
        adapter = _load_module("adapter")
        self.assertTrue(
            adapter._ws_token_is_valid(
                "secret-token",
                "Bearer secret-token",
                None,
            )
        )

    def test_rejects_missing_or_wrong_token(self):
        adapter = _load_module("adapter")
        self.assertFalse(adapter._ws_token_is_valid("secret-token", "", None))
        self.assertFalse(
            adapter._ws_token_is_valid(
                "secret-token",
                "Bearer wrong-token",
                None,
            )
        )

    def test_accepts_query_token_for_onebot_compatibility(self):
        adapter = _load_module("adapter")
        self.assertTrue(
            adapter._ws_token_is_valid(
                "secret-token",
                "",
                "secret-token",
            )
        )

    def test_rejects_event_for_another_bot_self_id(self):
        adapter = _load_module("adapter")
        self.assertFalse(adapter._event_matches_self_id({"self_id": 999}, "123"))
        self.assertTrue(adapter._event_matches_self_id({"self_id": 123}, "123"))


class ReverseWebSocketIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        adapter = _load_module("adapter")
        self.adapter = adapter
        self.instance = object.__new__(adapter.NapCatAdapter)
        self.instance._ws_allowed_ips = set()
        self.instance._ws_access_token = "secret-token"
        self.instance._active_ws = set()
        self.instance._raw_tasks = set()
        self.instance._ws_max_message_bytes = 1024 * 1024
        self.instance._ws_heartbeat_seconds = 30
        self.instance._ws_max_inflight = 4
        self.instance._handle_raw = AsyncMock()
        app = aiohttp.web.Application()
        app.router.add_get("/", self.instance._ws_handler)
        self.client = TestClient(TestServer(app))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_rejects_unauthenticated_websocket_handshake(self):
        with self.assertLogs(self.adapter.logger, level="WARNING"):
            with self.assertRaises(aiohttp.WSServerHandshakeError) as ctx:
                await self.client.ws_connect("/")
        self.assertEqual(ctx.exception.status, 401)

    async def test_accepts_authenticated_websocket_handshake(self):
        ws = await self.client.ws_connect(
            "/",
            headers={"Authorization": "Bearer secret-token"},
        )
        self.assertFalse(ws.closed)
        await ws.close()

    async def test_connect_rejects_empty_token_even_on_loopback(self):
        instance = object.__new__(self.adapter.NapCatAdapter)
        instance._http_api = "http://napcat.test"
        instance._ws_access_token = ""
        instance._ws_host = "127.0.0.1"
        self.assertFalse(await instance.connect())


class PermissionContextTests(unittest.TestCase):
    def test_empty_context_blocks_qq_tools_but_not_unrelated_cli_tools(self):
        adapter = _load_module("adapter")
        with patch("gateway.session_context.get_session_env", return_value=""):
            blocked = adapter._napcat_acl_pre_tool_call("qq_send_message")
            unrelated = adapter._napcat_acl_pre_tool_call("read_file")

        self.assertEqual(blocked["action"], "block")
        self.assertIsNone(unrelated)

    def test_pre_tool_hook_fails_closed_when_session_context_breaks(self):
        adapter = _load_module("adapter")
        with patch(
            "gateway.session_context.get_session_env",
            side_effect=RuntimeError("context unavailable"),
        ):
            blocked = adapter._napcat_acl_pre_tool_call("qq_send_message")
            unrelated = adapter._napcat_acl_pre_tool_call("read_file")

        self.assertEqual(blocked["action"], "block")
        self.assertIsNone(unrelated)

    def test_other_platform_blocks_qq_tools_but_not_unrelated_tools(self):
        adapter = _load_module("adapter")

        def session_env(name, default=""):
            return {
                "HERMES_SESSION_PLATFORM": "cli",
                "HERMES_SESSION_USER_ID": "123",
            }.get(name, default)

        with patch("gateway.session_context.get_session_env", side_effect=session_env):
            blocked = adapter._napcat_acl_pre_tool_call("qq_send_message")
            unrelated = adapter._napcat_acl_pre_tool_call("read_file")

        self.assertEqual(blocked["action"], "block")
        self.assertIsNone(unrelated)

    def test_missing_session_context_fails_closed_even_after_owner_message(self):
        qq_tool = _load_module("qq_tool")
        qq_tool._init("http://napcat.test", "token", owners=["123"], admins=[])
        if hasattr(qq_tool, "_current_sender"):
            qq_tool._current_sender = "123"
            qq_tool._current_role = "owner"

        with patch("gateway.session_context.get_session_env", return_value=""):
            sender, role = qq_tool._current_identity()

        self.assertEqual(sender, "")
        self.assertEqual(role, "user")
        self.assertIsNotNone(qq_tool._require_admin())


class ToolEndpointTests(unittest.IsolatedAsyncioTestCase):
    def test_bounded_int_clamps_tool_parameters(self):
        qq_tool = _load_module("qq_tool")
        self.assertEqual(qq_tool._bounded_int("500", default=20, minimum=1, maximum=100), 100)
        self.assertEqual(qq_tool._bounded_int("bad", default=20, minimum=1, maximum=100), 20)

    async def test_set_friend_remark_uses_correct_endpoint(self):
        qq_tool = _load_module("qq_tool")
        qq_tool._init("http://napcat.test", "token", owners=["123"], admins=[])
        call = AsyncMock(return_value={})
        with (
            patch.object(qq_tool, "_call", call),
            patch.object(qq_tool, "_require_admin", return_value=None),
        ):
            await qq_tool._qq_set_friend_remark(
                {"user_id": "456", "remark": "朋友"}
            )

        call.assert_awaited_once_with(
            "set_friend_remark",
            user_id=456,
            remark="朋友",
        )

    async def test_translation_uses_current_napcat_action_and_parameter(self):
        qq_tool = _load_module("qq_tool")
        qq_tool._init("http://napcat.test", "token", owners=["123"], admins=[])
        call = AsyncMock(return_value={"data": {}})
        with patch.object(qq_tool, "_call", call):
            await qq_tool._qq_translate_en2zh({"content": "hello"})
        call.assert_awaited_once_with("translate_en2zh", words=["hello"])

    async def test_history_uses_message_seq_for_pagination(self):
        qq_tool = _load_module("qq_tool")
        qq_tool._init("http://napcat.test", "token", owners=["123"], admins=[])
        call = AsyncMock(return_value={"data": {}})
        with patch.object(qq_tool, "_call", call):
            await qq_tool._qq_get_group_msg_history(
                {"group_id": "789", "message_seq": "456", "count": 20}
            )
        call.assert_awaited_once_with(
            "get_group_msg_history",
            group_id=789,
            message_seq=456,
            count=20,
        )

        call.reset_mock()
        with patch.object(qq_tool, "_call", call):
            await qq_tool._qq_get_friend_msg_history(
                {"user_id": "123", "message_seq": "456", "count": 20}
            )
        call.assert_awaited_once_with(
            "get_friend_msg_history",
            user_id=123,
            message_seq=456,
            count=20,
        )

    async def test_download_file_uses_safe_hermes_fetch_not_raw_napcat_url(self):
        qq_tool = _load_module("qq_tool")
        qq_tool._init("http://napcat.test", "token", owners=["123"], admins=[])
        safe_call = AsyncMock(return_value={"data": {"file": "/tmp/downloaded"}})
        raw_call = AsyncMock()
        with (
            patch.object(qq_tool, "call_onebot_api_with_local_file_url_fallback", safe_call),
            patch.object(qq_tool, "_call", raw_call),
        ):
            result = await qq_tool._qq_download_file(
                {
                    "url": "https://files.example/archive.zip",
                    "thread_count": 8,
                    "headers": ["Authorization: Bearer secret", "X-Test=ok"],
                }
            )

        self.assertFalse(getattr(result, "is_error", False))
        raw_call.assert_not_awaited()
        safe_call.assert_awaited_once_with(
            "http://napcat.test",
            "download_file",
            {"url": "https://files.example/archive.zip", "thread_count": 1},
            "token",
            file_key="url",
            download_headers={"Authorization": "Bearer secret", "X-Test": "ok"},
        )

    async def test_download_file_rejects_unsafe_custom_header(self):
        qq_tool = _load_module("qq_tool")
        qq_tool._init("http://napcat.test", "token", owners=["123"], admins=[])
        safe_call = AsyncMock()
        with patch.object(qq_tool, "call_onebot_api_with_local_file_url_fallback", safe_call):
            result = await qq_tool._qq_download_file(
                {"url": "https://files.example/a", "headers": ["Host: internal"]}
            )
        self.assertIn("download header is not allowed: Host", str(result))
        safe_call.assert_not_awaited()

    async def test_forward_tools_use_recursive_safe_media_helper(self):
        qq_tool = _load_module("qq_tool")
        qq_tool._init("http://napcat.test", "token", owners=["123"], admins=[])
        messages = [
            {
                "type": "node",
                "data": {
                    "name": "tester",
                    "uin": "123",
                    "content": [
                        {"type": "image", "data": {"file": "https://files.example/a.png"}}
                    ],
                },
            }
        ]
        safe_call = AsyncMock(return_value={"data": {"message_id": 1}})
        raw_call = AsyncMock()
        with (
            patch.object(qq_tool, "call_onebot_api_with_media_fallback", safe_call),
            patch.object(qq_tool, "_call", raw_call),
        ):
            await qq_tool._qq_send_group_forward_msg({"group_id": "789", "messages": messages})
            await qq_tool._qq_send_private_forward_msg({"user_id": "456", "messages": messages})

        raw_call.assert_not_awaited()
        self.assertEqual(safe_call.await_count, 2)
        group_call, private_call = safe_call.await_args_list
        self.assertEqual(group_call.args[1], "send_group_forward_msg")
        self.assertEqual(group_call.args[2]["group_id"], 789)
        self.assertEqual(group_call.kwargs["message_key"], "messages")
        self.assertEqual(private_call.args[1], "send_private_forward_msg")
        self.assertEqual(private_call.args[2]["user_id"], 456)
        self.assertEqual(private_call.kwargs["message_key"], "messages")

    async def test_group_portrait_uses_cross_host_file_fallback(self):
        qq_tool = _load_module("qq_tool")
        qq_tool._init("http://napcat.test", "token", owners=["123"], admins=[])
        fallback = AsyncMock(return_value={"data": {}})
        with (
            patch.object(qq_tool, "call_onebot_api_with_local_file_url_fallback", fallback),
            patch.object(qq_tool, "_require_admin", return_value=None),
        ):
            await qq_tool._qq_set_group_portrait(
                {"group_id": "789", "file": "/tmp/avatar.png"}
            )
        fallback.assert_awaited_once_with(
            "http://napcat.test",
            "set_group_portrait",
            {"group_id": 789, "file": "/tmp/avatar.png"},
            "token",
            file_key="file",
        )

    async def test_group_notice_image_uses_cross_host_file_fallback(self):
        qq_tool = _load_module("qq_tool")
        qq_tool._init("http://napcat.test", "token", owners=["123"], admins=[])
        fallback = AsyncMock(return_value={"data": {}})
        with (
            patch.object(qq_tool, "call_onebot_api_with_local_file_url_fallback", fallback),
            patch.object(qq_tool, "_require_admin", return_value=None),
        ):
            await qq_tool._qq_send_group_notice(
                {"group_id": "789", "content": "公告", "image": "/tmp/notice.png"}
            )
        fallback.assert_awaited_once_with(
            "http://napcat.test",
            "_send_group_notice",
            {"group_id": 789, "content": "公告", "image": "/tmp/notice.png"},
            "token",
            file_key="image",
        )

    async def test_ocr_uses_cross_host_file_fallback(self):
        qq_tool = _load_module("qq_tool")
        qq_tool._init("http://napcat.test", "token", owners=["123"], admins=[])
        fallback = AsyncMock(return_value={"data": {"texts": []}})
        with patch.object(qq_tool, "call_onebot_api_with_local_file_url_fallback", fallback):
            await qq_tool._qq_ocr_image({"image": "/tmp/ocr.png"})
        fallback.assert_awaited_once_with(
            "http://napcat.test",
            "ocr_image",
            {"image": "/tmp/ocr.png"},
            "token",
            file_key="image",
        )


class VoiceLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_ffmpeg_conversion_runs_off_event_loop(self):
        adapter = _load_module("adapter")
        completed = SimpleNamespace(returncode=0)

        async def fake_offload(_func, command, **_kwargs):
            Path(command[-1]).write_bytes(b"RIFF")
            return completed

        offload = AsyncMock(side_effect=fake_offload)
        with (
            patch.object(adapter, "download_public_url_bytes", AsyncMock(return_value=b"voice")),
            patch.object(adapter.asyncio, "to_thread", offload),
        ):
            wav = await adapter._download_and_convert_wav("https://cdn.example/voice", 1024)

        self.assertIsNotNone(wav)
        awaited = offload.await_args
        self.assertIsNotNone(awaited)
        assert awaited is not None
        self.assertIs(awaited.args[0], adapter.subprocess.run)
        self.assertIn("-t", awaited.args[1])
        if wav:
            Path(wav).unlink(missing_ok=True)

    async def test_oversized_converted_wav_is_rejected_and_cleaned(self):
        adapter = _load_module("adapter")
        with tempfile.TemporaryDirectory() as tmp:
            silk = Path(tmp) / "voice.silk"

            async def fake_to_thread(_func, command, **_kwargs):
                Path(command[-1]).write_bytes(b"x" * 8192)
                return SimpleNamespace(returncode=0)

            with (
                patch.object(adapter, "download_public_url_bytes", AsyncMock(return_value=b"voice")),
                patch.object(
                    adapter.tempfile,
                    "mkstemp",
                    side_effect=lambda **_kwargs: (
                        os.open(silk, os.O_CREAT | os.O_RDWR),
                        str(silk),
                    ),
                ),
                patch.object(adapter.asyncio, "to_thread", side_effect=fake_to_thread),
            ):
                wav = await adapter._download_and_convert_wav("https://cdn.example/voice", 1024)

            self.assertIsNone(wav)
            self.assertFalse(silk.exists())
            self.assertFalse(silk.with_suffix(".wav").exists())

    async def test_processing_completion_cleans_voice_temp_on_failure(self):
        adapter = _load_module("adapter")

        fd, wav = tempfile.mkstemp(suffix=".wav")
        os.close(fd)
        Path(wav).write_bytes(b"wave")
        instance = object.__new__(adapter.NapCatAdapter)
        instance._pending_completion_reply_anchors = {}
        instance._active_reply_anchors = {}
        event = SimpleNamespace(
            message_id="123",
            source=SimpleNamespace(chat_id="chat"),
            metadata={"_napcat_temp_media_paths": [wav]},
        )

        await instance.on_processing_complete(event, object())

        self.assertFalse(Path(wav).exists())

    async def test_inline_command_success_cleans_voice_temp(self):
        adapter = _load_module("adapter")
        fd, wav = tempfile.mkstemp(suffix=".wav")
        os.close(fd)
        Path(wav).write_bytes(b"wave")
        instance = object.__new__(adapter.NapCatAdapter)
        instance._clear_processing = AsyncMock()
        instance._mark_post_response = AsyncMock()
        event = SimpleNamespace(
            message_id="123",
            metadata={"_napcat_temp_media_paths": [wav]},
        )

        await instance._complete_inline_command(event)

        instance._clear_processing.assert_awaited_once_with("123")
        instance._mark_post_response.assert_awaited_once_with("123")
        self.assertFalse(Path(wav).exists())

    async def test_busy_inline_command_does_not_pollute_next_reply_anchor(self):
        adapter = _load_module("adapter")
        instance = object.__new__(adapter.NapCatAdapter)
        instance._busy_followup_reply_anchors = {}
        instance._next_final_reply_anchors = {}
        instance._inline_completion_reply_anchors = {}
        instance._pending_completion_reply_anchors = {}
        instance._active_sessions = {"session"}
        instance._session_key_for_event = lambda _event: "session"
        instance._needs_inline_command_completion_reaction = lambda _event: True

        event = SimpleNamespace(
            message_id="1884618342",
            source=SimpleNamespace(chat_id="group:1041762935"),
            get_command=lambda: "status",
        )

        with patch.object(adapter.BasePlatformAdapter, "handle_message", AsyncMock()):
            await instance.handle_message(event)

        self.assertEqual(instance._busy_followup_reply_anchors, {})

    def test_self_improvement_review_is_progress_only(self):
        adapter = _load_module("adapter")
        text = (
            "💾 Self-improvement review: Patched SKILL.md in skill "
            "'hermes-ops-troubleshooting' (1 replacement). · "
            "Patched SKILL.md in skill 'source-backed-tech-answers' (1 replacement)."
        )
        self.assertTrue(adapter._is_non_final_progress_message(text))
        self.assertTrue(adapter._is_non_final_progress_message("💾 Memory updated"))
        self.assertTrue(
            adapter._is_non_final_progress_message(
                "⏳ Working — 10 min — iteration 18/150, waiting for non-streaming API response"
            )
        )
        self.assertFalse(
            adapter._is_non_final_progress_message(
                "关好了。目标：乌托邦探险之旅"
            )
        )

    async def test_self_improvement_send_does_not_consume_reply_anchors(self):
        adapter = _load_module("adapter")
        instance = object.__new__(adapter.NapCatAdapter)
        instance._http_api = "http://127.0.0.1:18801"
        instance._access_token = "token"
        instance._quote_reply_enabled = True
        instance._inline_completion_reply_anchors = {
            "group:1041762935": "111"
        }
        instance._active_reply_anchors = {"group:1041762935": "111"}
        instance._next_final_reply_anchors = {}
        instance._busy_followup_reply_anchors = {}
        instance._pending_completion_reply_anchors = {}
        instance._post_reply_pokes = {}
        instance._poke_after_reply = AsyncMock()
        instance._clear_processing = AsyncMock()
        instance._mark_post_response = AsyncMock()

        captured = {}

        async def fake_send_group_msg(api, group_id, segs, token=None):
            captured["segs"] = segs
            return {"message_id": 999}

        with patch.object(adapter, "send_group_msg", fake_send_group_msg):
            result = await instance.send(
                "group:1041762935",
                "💾 Self-improvement review: Patched SKILL.md in skill 'x' (1 replacement).",
            )

        self.assertTrue(result.success)
        # No QQ quote-reply segment for status bubbles.
        self.assertEqual(
            [seg.get("type") for seg in captured["segs"]],
            ["text"],
        )
        # Must not consume completion bookkeeping meant for the real answer.
        self.assertEqual(
            instance._inline_completion_reply_anchors,
            {"group:1041762935": "111"},
        )
        instance._clear_processing.assert_not_awaited()
        instance._mark_post_response.assert_not_awaited()
        instance._poke_after_reply.assert_not_awaited()

    def _voice_test_instance(self, adapter):
        instance = object.__new__(adapter.NapCatAdapter)
        instance._owners = {"123"}
        instance._admins = set()
        instance._group_allow_chats = set()
        instance._require_mention = False
        instance._self_id = "999"
        instance._source_platform = "napcat"
        instance._media_max_mb = 10
        instance._is_sender_authorized = lambda *_args: True
        instance._remember_post_reply_reaction = lambda *_args: None
        instance._remember_post_reply_poke = lambda *_args: None
        instance._mark_processing = AsyncMock()
        return instance

    @staticmethod
    def _voice_event(timestamp):
        return {
            "message_type": "private",
            "user_id": 123,
            "sender": {"nickname": "tester"},
            "message_id": 456,
            "message": [{"type": "record", "data": {"file": "https://cdn.example/a.silk"}}],
            "time": timestamp,
        }

    async def test_event_construction_failure_happens_before_voice_conversion(self):
        adapter = _load_module("adapter")
        instance = self._voice_test_instance(adapter)
        convert = AsyncMock(return_value="/tmp/should-not-exist.wav")
        with patch.object(adapter, "_download_and_convert_wav", convert):
            with self.assertRaises((OverflowError, OSError, ValueError)):
                await instance._process_message(self._voice_event(10**100))
        convert.assert_not_awaited()

    async def test_pre_dispatch_failure_cleans_converted_voice_temp(self):
        adapter = _load_module("adapter")
        fd, wav = tempfile.mkstemp(suffix=".wav")
        os.close(fd)
        Path(wav).write_bytes(b"wave")
        instance = self._voice_test_instance(adapter)
        instance._mark_processing = AsyncMock(side_effect=RuntimeError("reaction failed"))
        with patch.object(adapter, "_download_and_convert_wav", AsyncMock(return_value=wav)):
            with self.assertRaises(RuntimeError):
                await instance._process_message(self._voice_event(1_700_000_000))
        self.assertFalse(Path(wav).exists())


class MediaSecurityTests(unittest.IsolatedAsyncioTestCase):
    def test_cross_origin_redirect_strips_sensitive_download_headers(self):
        api = _load_module("napcat_api")
        headers = {
            "Authorization": "Bearer secret",
            "Cookie": "session=secret",
            "Proxy-Authorization": "proxy-secret",
            "X-Api-Key": "api-secret",
        }
        redirected = api._headers_for_redirect(
            headers,
            "https://files.example/a",
            "https://cdn.example/b",
        )
        self.assertEqual(redirected, {})
        self.assertEqual(
            api._headers_for_redirect(
                headers,
                "https://files.example/a",
                "https://files.example/b",
            ),
            headers,
        )

    def test_remote_download_stays_bounded_when_local_precheck_is_disabled(self):
        api = _load_module("napcat_api")
        with (
            patch.object(api, "_MAX_UPLOAD_BYTES", 0),
            patch.object(api, "_MAX_REMOTE_DOWNLOAD_BYTES", 2048),
        ):
            self.assertEqual(api._remote_upload_limit(), 2048)

    async def test_nested_forward_media_is_replaced_before_first_napcat_call(self):
        api = _load_module("napcat_api")
        original = "https://files.example/private.png"
        messages = [
            {
                "type": "node",
                "data": {
                    "content": [
                        {"type": "image", "data": {"file": original}}
                    ]
                },
            }
        ]
        call = AsyncMock(return_value={"retcode": 0, "data": {"message_id": 1}})
        download = AsyncMock(return_value="base64://safe")
        with (
            patch.object(api, "call_onebot_api", call),
            patch.object(api, "_download_url_as_base64_ref", download),
        ):
            await api.call_onebot_api_with_media_fallback(
                "http://napcat.test",
                "send_group_forward_msg",
                {"group_id": 789, "messages": messages},
                message_key="messages",
            )

        awaited = call.await_args
        self.assertIsNotNone(awaited)
        assert awaited is not None
        first_params = awaited.args[2]
        nested_file = first_params["messages"][0]["data"]["content"][0]["data"]["file"]
        self.assertEqual(nested_file, "base64://safe")
        self.assertNotIn(original, str(first_params))

    async def test_oversized_local_media_is_rejected_before_napcat_call(self):
        api = _load_module("napcat_api")
        with tempfile.TemporaryDirectory() as temp_dir:
            media = Path(temp_dir) / "media.bin"
            media.write_bytes(b"xx")
            call = AsyncMock(return_value={"data": {"message_id": 1}})
            with (
                patch.object(api, "_MAX_BASE64_MEDIA_BYTES", 1),
                patch.object(api, "_MAX_UPLOAD_BYTES", 1),
                patch.object(api, "call_onebot_api", call),
            ):
                with self.assertRaises(ValueError):
                    await api.call_onebot_api_with_media_fallback(
                        "http://napcat.test",
                        "send_private_msg",
                        {
                            "user_id": 123,
                            "message": [{"type": "image", "data": {"file": str(media)}}],
                        },
                    )

            call.assert_not_awaited()

    async def test_local_media_path_is_replaced_before_first_napcat_call(self):
        api = _load_module("napcat_api")
        with tempfile.TemporaryDirectory() as temp_dir:
            media = Path(temp_dir) / "media.bin"
            media.write_bytes(b"xx")
            call = AsyncMock(return_value={"data": {"message_id": 1}})
            with (
                patch.object(api, "_MAX_BASE64_MEDIA_BYTES", 1),
                patch.object(api, "_MAX_UPLOAD_BYTES", 1024),
                patch.object(api, "local_bind_host_for", return_value="127.0.0.1"),
                patch.object(api, "public_media_host_for", return_value="127.0.0.1"),
                patch.object(api, "call_onebot_api", call),
            ):
                await api.call_onebot_api_with_media_fallback(
                    "http://napcat.test",
                    "send_private_msg",
                    {
                        "user_id": 123,
                        "message": [{"type": "image", "data": {"file": str(media)}}],
                    },
                )

            call.assert_awaited_once()
            awaited = call.await_args
            self.assertIsNotNone(awaited)
            assert awaited is not None
            sent_params = awaited.args[2]
            sent = sent_params["message"][0]["data"]["file"]
            self.assertTrue(sent.startswith("http://127.0.0.1:"))
            self.assertNotIn(str(media), str(sent_params))

    async def test_temp_file_handler_exposes_only_exact_allowed_path(self):
        api = _load_module("napcat_api")
        with tempfile.TemporaryDirectory() as temp_dir:
            token = "unguessable-token"
            token_dir = Path(temp_dir) / token
            token_dir.mkdir()
            (token_dir / "media.txt").write_text("ok", encoding="utf-8")
            handler = partial(
                api._QuietFileHandler,
                directory=temp_dir,
                allowed_paths={f"/{token}/media.txt"},
            )
            server = api.ThreadingHTTPServer(("127.0.0.1", 0), handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                async with aiohttp.ClientSession() as session:
                    base = f"http://127.0.0.1:{server.server_port}"
                    for path in ("/", f"/{token}/", "/wrong/media.txt"):
                        async with session.get(base + path) as response:
                            self.assertEqual(response.status, 404, path)
                    async with session.get(base + f"/{token}/media.txt") as response:
                        self.assertEqual(response.status, 200)
                        self.assertEqual(await response.text(), "ok")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    async def test_connection_resolver_rejects_private_rebinding_result(self):
        api = _load_module("napcat_api")
        loop = asyncio.get_running_loop()
        with patch.object(
            loop,
            "getaddrinfo",
            AsyncMock(return_value=[(2, 1, 6, "", ("127.0.0.1", 80))]),
        ):
            with self.assertRaises(ValueError):
                await api._PublicOnlyResolver().resolve("rebind.example", 80)

    async def test_remote_upload_url_is_downloaded_before_napcat_call(self):
        api = _load_module("napcat_api")

        async def fake_download(url, target, **kwargs):
            del url, kwargs
            target.write_bytes(b"safe")
            return 4

        raw_call = AsyncMock(return_value={"data": {}})
        safe_call = AsyncMock(return_value={"data": {}})
        with (
            patch.object(api, "_download_url_to_file", side_effect=fake_download),
            patch.object(api, "call_onebot_api", raw_call),
            patch.object(api, "_call_upload_with_temp_url", safe_call),
        ):
            await api.call_onebot_api_with_local_file_url_fallback(
                "http://napcat.test",
                "ocr_image",
                {"image": "https://cdn.example/media.png"},
                "token",
                file_key="image",
            )

        raw_call.assert_not_awaited()
        safe_call.assert_awaited_once()

    async def test_remote_message_url_is_replaced_before_first_napcat_call(self):
        api = _load_module("napcat_api")
        call = AsyncMock(return_value={"data": {"message_id": 1}})
        with (
            patch.object(api, "download_public_url_bytes", AsyncMock(return_value=b"image")),
            patch.object(api, "call_onebot_api", call),
        ):
            await api.call_onebot_api_with_media_fallback(
                "http://napcat.test",
                "send_private_msg",
                {
                    "user_id": 123,
                    "message": [{"type": "image", "data": {"file": "https://cdn.example/a.png"}}],
                },
                "token",
            )

        sent = call.await_args.args[2]["message"][0]["data"]["file"]
        self.assertTrue(sent.startswith("base64://"))

    def test_public_media_override_is_not_used_as_local_bind_address(self):
        api = _load_module("napcat_api")
        with (
            patch.dict("os.environ", {"NAPCAT_PUBLIC_MEDIA_HOST": "files.example.test"}),
            patch.object(api, "_route_local_address", return_value="192.168.50.10"),
        ):
            self.assertEqual(
                api.public_media_host_for("http://192.168.50.20:18801"),
                "files.example.test",
            )
            self.assertEqual(
                api.local_bind_host_for("http://192.168.50.20:18801"),
                "192.168.50.10",
            )

    def test_rejects_non_global_destination_addresses(self):
        api = _load_module("napcat_api")
        for value in ("127.0.0.1", "10.0.0.1", "192.168.50.20", "169.254.169.254", "::1"):
            with self.subTest(value=value):
                self.assertFalse(api._is_public_ip(ipaddress.ip_address(value)))

    def test_accepts_public_destination_address(self):
        api = _load_module("napcat_api")
        self.assertTrue(api._is_public_ip(ipaddress.ip_address("1.1.1.1")))

    async def test_rejects_private_and_non_http_media_urls(self):
        api = _load_module("napcat_api")
        for url in (
            "http://127.0.0.1/file",
            "http://192.168.50.20/file",
            "file:///etc/passwd",
            "https://user:pass@1.1.1.1/file",
        ):
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    await api.ensure_public_http_url(url)

    async def test_accepts_public_literal_media_url(self):
        api = _load_module("napcat_api")
        await api.ensure_public_http_url("https://1.1.1.1/image.png")

    async def test_limited_download_stops_when_stream_exceeds_cap(self):
        api = _load_module("napcat_api")
        from aiohttp import web
        from aiohttp.test_utils import TestClient, TestServer

        async def large_response(request):
            del request
            return web.Response(body=b"x" * 2048)

        app = web.Application()
        app.router.add_get("/large", large_response)
        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            with self.assertRaises(ValueError):
                with patch.object(api, "ensure_public_http_url", AsyncMock(return_value=None)):
                    await api.download_public_url_bytes(
                        str(client.make_url("/large")),
                        max_bytes=1024,
                    )
        finally:
            await client.close()

    async def test_url_validation_rejects_private_dns_result(self):
        api = _load_module("napcat_api")
        loop = asyncio.get_running_loop()
        with patch.object(
            loop,
            "getaddrinfo",
            AsyncMock(return_value=[(2, 1, 6, "", ("192.168.50.20", 80))]),
        ):
            with self.assertRaises(ValueError):
                await api.ensure_public_http_url("http://example.test/image.png")

    async def test_url_validation_accepts_public_dns_result(self):
        api = _load_module("napcat_api")
        loop = asyncio.get_running_loop()
        with patch.object(
            loop,
            "getaddrinfo",
            AsyncMock(return_value=[(2, 1, 6, "", ("1.1.1.1", 443))]),
        ):
            await api.ensure_public_http_url("https://example.test/image.png")


if __name__ == "__main__":
    unittest.main()
