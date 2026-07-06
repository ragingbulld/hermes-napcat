# Security Policy

This plugin can bridge Hermes Agent to a live QQ account through NapCat, so treat configuration as sensitive.

## Do not commit

- NapCat `access_token`
- QQ owner/admin/group IDs from private deployments
- LAN IPs or reverse-WebSocket URLs that identify your network
- Hermes `.env`, `config.yaml`, SQLite state, logs, or session dumps

## Reporting issues

Please open a GitHub issue with reproduction steps. Redact tokens, QQ numbers, group IDs, private messages, and internal IPs before posting logs.
