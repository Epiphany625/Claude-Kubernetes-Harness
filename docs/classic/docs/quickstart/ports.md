---
sidebar_position: 5
title: Ports
---

| Service        | Port  | Paths                                                        | Override                              |
| -------------- | ----- | ------------------------------------------------------------ | ------------------------------------- |
| alertprocessor | 8080  | `/api/v1/alerts`, `/api/v1/approvals`, `/healthz`, `/readyz` | `ALERTPROCESSOR_HTTP_ADDR`            |
| kubemcp        | 9700  | `/mcp`, `/healthz`, `/readyz`                                | `KUBEMCP_PORT`                        |
| harness        | —     | listens on nothing                                           | —                                     |
| RabbitMQ       | 5672  | AMQP (15672: dashboard)                                      | `HARNESS_AMQP_PORT`                   |
| PostgreSQL     | 5432  | —                                                            | `HARNESS_POSTGRES_DSN`                |
| SMTP (Gmail)   | 587   | outbound, from harness                                       | `HARNESS_NOTIFIERS_EMAIL_SMTPPORT`    |
| Docs site      | 3000  | `npm start` in `docs/classic`                                | `npm start -- --port <port>`          |

## Who calls whom

| Caller       | Target                   | Local                                    | In cluster                                         |
| ------------ | ------------------------ | ---------------------------------------- | -------------------------------------------------- |
| Alertmanager | alertprocessor           | `http://localhost:8080/api/v1/alerts`    | `http://alertprocessor.ckh.svc:8080/api/v1/alerts` |
| harness      | kubemcp                  | `http://127.0.0.1:9700/mcp`              | `http://kubemcp.ckh.svc.cluster.local:9700/mcp`    |
| Approval email | alertprocessor         | `http://localhost:8080/api/v1/approvals` | —                                                  |
| harness, alertprocessor | RabbitMQ      | `localhost:5672` (port-forward)          | `agent-rabbitmq.ckh.svc.cluster.local:5672`        |

## e2e approval demo

`e2e/approval-demo` uses uncommon host ports so it doesn't clash with the ports above: Postgres 55432, RabbitMQ 55672, Mailpit SMTP 11025, Mailpit UI 18025, alertprocessor 18080.
