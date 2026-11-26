---
sidebar_position: 2
title: kubemcp
---

MCP server the agent uses to read and write the cluster. It uses your `~/.kube/config`.

```bash
cd kubemcp
uv sync
uv run kubemcp                 # serves http://0.0.0.0:9700/mcp
curl localhost:9700/healthz
```

To use another port, set `KUBEMCP_PORT`. Every setting is listed in `kubemcp/README.md`.
