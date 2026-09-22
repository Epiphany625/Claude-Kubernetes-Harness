Core microservice for debugging, diagnosis, and the agent loop.
Language & framework: Python, Claude Agent SDK.

Iterations:

1. Agent can receive messages from rabbitmq, and based off RabbitMQ, solves Kubernetes problems. (this one is basically done)
2. Write a Dockerfile to build the image, then a Kubernetes manifest (deployment with replica = 1, imagepull = always, and a service layer, if needed) in the ops/ folder.
3. Add Langfuse for LLM performance evaluation
4. add around 500+ test cases to benchmark LLM capabilities
5. Add a bug causer (similar to the tool invented by netflix) to intentionally cause problems in the code and evaluate performance.
6. Add postgres handler to report final solution result to postgres for each stage (waiting human approval, not solved, solved, etc. )
