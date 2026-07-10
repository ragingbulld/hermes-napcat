# hermes-napcat

Out-of-tree Hermes Agent platform plugin for QQ via [NapCat](https://github.com/NapNeko/NapCatQQ) / OneBot 11.

这个插件让 Hermes Agent 通过 NapCat 接入 QQ 群聊和私聊，不需要修改 Hermes Agent 核心源码。

## Features

- QQ group and private-message adapter for Hermes Gateway
- Reverse WebSocket listener for NapCat events
- Authenticated reverse WebSocket with bot-ID validation and bounded event handling
- OneBot 11 HTTP API client for sending text, images, voice, video, and files
- `qq_*` toolset for QQ messaging, group management, OCR, translation, reactions, notices, and files
- QQ-number based owner/admin/user ACL for NapCat tool calls
- Optional group mention requirement
- Processing and post-response QQ emoji reactions
- Private-chat typing indicator support
- Plain-text formatting for NapCat QQ, because ordinary QQ does not render Markdown

## Requirements

- Linux host running Hermes Agent
- Hermes Agent with plugin support
- NapCat configured with:
  - OneBot 11 HTTP API enabled
  - reverse WebSocket target pointing to the Hermes host and `ws_port`
  - a non-empty reverse WebSocket token; it reuses the HTTP API token by default,
    or may be configured separately with `ws_access_token`
- Python dependency: `aiohttp`（通常 Hermes gateway 环境里已经有）

## Installation

Clone this repository into your Hermes plugin directory:

```bash
git clone https://github.com/ragingbulld/hermes-napcat.git ~/.hermes/plugins/hermes-napcat
```

Enable the plugin in `~/.hermes/config.yaml`:

```yaml
plugins:
  enabled:
    - hermes-napcat
```

Add a NapCat platform config:

```yaml
platforms:
  napcat:
    enabled: true
    extra:
      http_api: "http://127.0.0.1:18801"
      access_token: "<required-high-entropy-token>"
      self_id: "<BOT_QQ_ID>"
      ws_host: "0.0.0.0"
      ws_port: 18800
      # Defaults to access_token. Set the same token in NapCat's WS client.
      # ws_access_token: "a-separate-high-entropy-token"
      ws_allowed_ips:
        - "<NAPCAT_HOST_IP>"
      ws_max_message_bytes: 2097152
      ws_max_inflight: 32
      ws_heartbeat_seconds: 30

      owners:
        - "<OWNER_QQ_ID>"
      admins: []

      # Required for group replies. An empty list rejects every group.
      group_allow_chats: []

      # Optional: require @bot in groups.
      require_mention: true
      group_require_mention: true

      processing_emoji: true
      processing_emoji_id: "307"
      post_response_emoji: true
      post_response_emoji_id: "478"

      private_typing_status: true
      private_typing_event_type: 1
      private_typing_interval: 5
      private_typing_max_seconds: 120

      poke_after_response: false
      media_max_mb: 5
```

Then validate and restart the gateway:

```bash
hermes config check
hermes gateway restart
```

NapCat should connect to:

```text
ws://<hermes-host>:18800
```

The plugin rejects unauthenticated reverse-WebSocket connections. Standard
Bearer authentication is supported; the OneBot `access_token` query parameter
is also accepted for compatibility. The listener refuses to start when neither
`ws_access_token` nor `access_token` is configured, including loopback-only
listeners. `ws_allowed_ips` is optional defense in depth and must contain the
NapCat host address when set.

## Access control

The plugin has two layers:

1. Reply/session access, configured by your Hermes Gateway/NapCat platform settings.
2. Tool-call ACL enforced by this plugin.

Tool roles:

- `owners`: full access, including memory/profile-sensitive tools.
- `admins`: may use admin/dangerous QQ tools, but not owner-only memory/profile tools.
- ordinary users: may chat when allowed by the reply ACL, but cannot call any tools by default.

Do not put real tokens, QQ IDs, group IDs, OpenIDs, or deployment IPs in this repository. Keep secrets in `~/.hermes/config.yaml` or `~/.hermes/.env` on your own machine.

## Repository layout

```text
adapter.py      # Hermes Gateway platform adapter and plugin registration
napcat_api.py   # Minimal async OneBot 11 HTTP client
qq_tool.py      # Hermes qq_* tool registrations
plugin.yaml     # Hermes plugin metadata
```

## Development

Basic validation:

```bash
# Use the same Python environment that runs Hermes Agent.
HERMES_PYTHON=/path/to/hermes/venv/bin/python
"$HERMES_PYTHON" -m py_compile *.py
```

Recommended pre-publish checks:

```bash
git status --short
"$HERMES_PYTHON" -m py_compile *.py tests/*.py
"$HERMES_PYTHON" -m unittest discover -s tests -v
```

## License

MIT
