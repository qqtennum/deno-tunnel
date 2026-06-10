# Deno WebSocket Relay

Minimal WebSocket relay server with a plain profile endpoint.

## Features

- WebSocket TCP forwarding
- Plain profile link at `/<UUID>`
- Optional Base64 subscription with `?base64` or `?b64`
- 0-RTT early data from `sec-websocket-protocol`

This minimal version does not include UDP, SOCKS5 relay, KV editing, Telegram notification, dynamic UUID, Clash conversion, or preferred IP pools.

## Run Locally

```bash
deno run --allow-net --allow-env server.js
```

Required environment variable:

```bash
UUID=xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
```

Optional environment variables:

```bash
PORT=8000
NAME=edge-link
HOST=example.com
WS_PATH=/link
```

PowerShell example:

```powershell
$env:UUID="xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx"; deno run --allow-net --allow-env server.js
```

## Endpoints

- `/` shows a basic health message.
- `/<UUID>` returns a client profile link.
- `/<UUID>?base64` returns the same link encoded with Base64.
- WebSocket upgrade requests on `WS_PATH` are handled as relay connections.

## Client Settings

- Protocol: use the generated profile value
- Transport: `ws`
- Path: `/link` by default, or your configured `WS_PATH`
- TLS: enabled when deployed behind HTTPS
- UUID: same as the `UUID` environment variable

Subscription URL:

```text
https://your-domain.example/<UUID>
```

Manual node format:

```text
<generated-profile>://<UUID>@your-domain.example:443?encryption=none&security=tls&type=ws&host=your-domain.example&path=%2Flink#edge-link
```

## Deploy To Deno Deploy

This project is intended for Deno Deploy / Deno Subhosting style deployments such as `console.deno.com`.

1. Create a new project in Deno Deploy.
2. Upload or connect this repository with `server.js` as the entrypoint.
3. Add environment variables in the project settings:

```text
UUID=xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
NAME=edge-link
```

4. Deploy and open:

```text
https://your-project.deno.dev/<UUID>
```

Deno Deploy automatically provides the HTTPS port, so the code does not bind `PORT` there. Local runs still use `PORT` or `8000`.

Important: relay forwarding requires outbound TCP socket support. If the Deno Deploy project/runtime does not expose `Deno.connect`, only the profile endpoint will work and WebSocket requests will return `501`.
