Core microservice for debugging, diagnosis, and the agent loop.
Language & framework: Python, Claude Agent SDK.

`docker build -t xinyangxu/harness:latest .` from this directory produces the
image: uv resolves dependencies into a virtualenv in the build stage, and the
runtime stage carries that virtualenv plus the source on `python:3.12-slim`.
There is no Node.js layer. `claude-agent-sdk` ships platform-specific wheels
that bundle the Claude Code CLI, and the SDK spawns that binary rather than
calling the API, so the linux wheel supplies it; the build fails outright if
that binary is ever missing. Build for the architecture of the cluster's nodes
-- a native `docker build` on Apple silicon produces an arm64 image, which will
not run on an amd64 node.

Deployment manifests are in `ops/harness`: a single-replica Deployment and a
Secret template. The pod gets its RabbitMQ credentials and `ANTHROPIC_API_KEY`
from that Secret and its kubemcp address from `HARNESS_MCPSERVERS`, since the
`127.0.0.1:8080` in `config/config.json` is a port-forward default that does
not resolve in a pod. There is no Service: the harness listens on nothing.

Iterations:

1. Agent can receive messages from rabbitmq, and based off RabbitMQ, solves Kubernetes problems. (this one is basically done)
2. Write a Dockerfile to build the image, then a Kubernetes manifest (deployment with replica = 1, imagepull = always, and a service layer, if needed) in the ops/ folder.
3. Add Langfuse for LLM performance evaluation
4. add around 500+ test cases to benchmark LLM capabilities
5. Add a bug causer (similar to the tool invented by netflix) to intentionally cause problems in the code and evaluate performance.
6. Add postgres handler to report final solution result to postgres for each stage (waiting human approval, not solved, solved, etc. )

Other Considerations:

1. We might need a validator agent to confirm the alert sent by AlertManger is actually a problem (for example, it is very likely that the problem is already solved. )
2. Maybe for a production-level Notifier, we need a dedicated database to store the interactions and use a webhook callback upon human approval. Shit. That means I need to support resuming from a break point? That would be very challenging.
