# hermes-napcat

Out-of-tree Hermes Agent platform plugin for QQ via [NapCat](https://github.com/NapNeko/NapCatQQ) / OneBot 11.

这个插件让 Hermes Agent 通过 NapCat 接入 QQ 群聊和私聊，不需要修改 Hermes Agent 核心源码。

## Features

- QQ group and private-message adapter for Hermes Gateway
- Reverse WebSocket listener for NapCat events
- OneBot 11 HTTP API client for sending text, images, voice, video, and files
- `qq_*` toolset for QQ messaging, group management, OCR, translation, reactions, notices, and files
- QQ-number based owner/admin/user ACL for NapCat tool calls
- Optional group mention requirement
- Processing and post-response QQ emoji reactions
- Private-chat typing indicator support
- Plain-text formatting for NapCat QQ, because ordinary QQ does not render Markdown
- Optional plugin-local `qqbot_native` platform for official QQBot access with native Markdown output

## Requirements

- Linux host running Hermes Agent
- Hermes Agent with plugin support
- NapCat configured with:
  - OneBot 11 HTTP API enabled
  - reverse WebSocket target pointing to the Hermes host and `ws_port`
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
      access_token: ""
      self_id: "123456789"
      ws_port: 18800

      owners:
        - "123456789"
      admins: []

      # Optional: restrict which QQ groups can talk to Hermes.
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
python -m py_compile ~/.hermes/plugins/hermes-napcat/*.py
hermes config check
hermes gateway restart
```

NapCat should connect to:

```text
ws://<hermes-host>:18800
```

## Optional official QQBot native platform

The plugin can also register a separate `qqbot_native` platform. This is not
Hermes' built-in `qqbot` adapter; it lives inside this plugin and keeps official
QQBot support out of Hermes core. Use it when you want official QQBot native
Markdown rendering. Official QQBot uses OpenID identities, not normal QQ
numbers.

```yaml
platforms:
  qqbot_native:
    enabled: false   # flip to true only after app credentials are ready
    extra:
      app_id: ""
      client_secret: ""
      markdown_support: true

      # OpenIDs, not QQ numbers.
      owners: []
      admins: []

      dm_policy: allowlist
      allow_from: []

      group_policy: allowlist
      group_allow_chats: []
```

## Access control

The plugin has two layers:

1. Reply/session access, configured by your Hermes Gateway/NapCat or QQBot Native platform settings.
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
qqbot_native.py # Plugin-local official QQBot adapter with native Markdown
qq_tool.py      # Hermes qq_* tool registrations
plugin.yaml     # Hermes plugin metadata
```

## Development

Basic validation:

```bash
python -m py_compile *.py
```

Recommended pre-publish checks:

```bash
git status --short
python -m py_compile *.py
```

## License

MIT
