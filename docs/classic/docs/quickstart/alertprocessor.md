---
sidebar_position: 3
title: alertprocessor
---

Receives Alertmanager webhooks, stores events in Postgres and publishes them to RabbitMQ. It also serves the approval page.

Needs the RabbitMQ port-forward from [Cluster setup](./cluster.md).

```bash
cd alertprocessor
source .env                                # Postgres DSN, AMQP username/password
export ALERTPROCESSOR_AMQP_HOST=127.0.0.1  # use the port-forward
go run ./cmd/alertprocessor                # serves :8080
```

On startup it creates the `event` and `approval` tables and the `agent.events` queue.

To test it, send a fake image-pull alert:

```bash
curl -i -X POST http://localhost:8080/api/v1/alerts \
  -H 'Content-Type: application/json' \
  --data-binary @test/testdata/image-pull-error.json
```
