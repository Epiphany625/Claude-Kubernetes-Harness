---
sidebar_position: 4
title: harness
---

The agent. It consumes events from RabbitMQ, fixes the cluster through kubemcp and asks a human before it writes.

Needs:

- the RabbitMQ port-forward;
- [kubemcp](./kubemcp.md);
- [alertprocessor](./alertprocessor.md), started first because it creates the queue and tables;
- Claude auth: `ANTHROPIC_API_KEY`, or a logged-in `claude` CLI.

```bash
cd harness
source .env                                 # AMQP username/password, Gmail app password
export HARNESS_POSTGRES_DSN='postgresql://...'  # same database as alertprocessor
uv sync
uv run python main.py                       # logs "[mq] consuming from agent.events"
```

Any key in `config/config.json` can be overridden with `HARNESS_` plus the key path in upper case, e.g. `HARNESS_AMQP_PORT=5673`.

## Try it

From the repository root:

```bash
kubectl create ns ckh-test
kubectl -n ckh-test create deployment nginx-deploy --image=nginxm:latest   # bad image
curl -i -X POST http://localhost:8080/api/v1/alerts \
  -H 'Content-Type: application/json' \
  --data-binary @alertprocessor/test/testdata/image-pull-error.json
```

What happens:

1. The agent finds the typo and proposes a patch.
2. It emails you an approval link.
3. Once you approve, the same session resumes, applies the patch and checks the fix.

The event's `status` column moves through `solving`, `waiting_human_approval` and `solving` again, ending at `resolved`.

To skip approval, start the harness with `HARNESS_POLICIES_REQUIREAPPROVAL=false`.

## Troubleshooting

| Symptom                                     | Cause                                                    |
| ------------------------------------------- | -------------------------------------------------------- |
| `address already in use`                    | Port taken: `lsof -nP -iTCP:<port> -sTCP:LISTEN`         |
| Fails on `get_queue`                        | alertprocessor hasn't created `agent.events` yet         |
| `postgres.dsn or postgres.host must be set` | `HARNESS_POSTGRES_DSN` not exported                      |
| RabbitMQ `ACCESS_REFUSED`                   | `.env` not sourced                                       |
| No `mcp__kubemcp__*` tools                  | kubemcp not running on 9700                              |
