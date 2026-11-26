---
sidebar_position: 1
title: Cluster setup
---

One-time setup: cert-manager, the RabbitMQ operator, and a RabbitMQ cluster in namespace `ckh`.

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.3/cert-manager.yaml
kubectl apply -f https://github.com/rabbitmq/cluster-operator/releases/latest/download/cluster-operator.yml
kubectl create ns ckh
kubectl apply -f - <<EOF
apiVersion: rabbitmq.com/v1beta1
kind: RabbitmqCluster
metadata:
  name: agent-rabbitmq
  namespace: ckh
EOF
```

RabbitMQ credentials, for the services' `.env` files:

```bash
kubectl -n ckh get secret agent-rabbitmq-default-user -o jsonpath='{.data.username}' | base64 --decode; echo
kubectl -n ckh get secret agent-rabbitmq-default-user -o jsonpath='{.data.password}' | base64 --decode; echo
```

Reach RabbitMQ from your machine. Each command blocks, so give each its own terminal:

```bash
kubectl -n ckh port-forward svc/agent-rabbitmq 5672:5672     # AMQP, needed by every service
kubectl -n ckh port-forward svc/agent-rabbitmq 15672:15672   # optional dashboard: http://localhost:15672
```
