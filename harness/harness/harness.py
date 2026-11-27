from agent.agent import AgentService
from mq.mq import RabbitMQService
from config.config import *
from mq.event import Event
from postgres.postgres import PostgresClient
from notifier.notifier import Notifier, NotifierSet
from notifier.email_notifier import EmailNotifier
from langfuse import Langfuse

import asyncio
from logging import Logger
import sys

class Harness:
    logger: Logger
    def __init__(self, harnessConfig: HarnessConfig, langfuseConfig: LangfuseConfig, notifiersConfig: NotifiersConfig):
        # a task queue for agentService to consume and rabbitmq service to produce.
        self.taskQueue = asyncio.Queue[Event](maxsize=100)

        agentOptionsConfig = harnessConfig.agentOptionsConfig
        rabbitMQConfig = harnessConfig.rabbitMQConfig
        postgresConfig = harnessConfig.postgresConfig

        self.pgClient = PostgresClient(postgresConfig)

        notifiers: list[Notifier] = []
        
        # edit here if more notifiers are supported. 
        emailConfig = notifiersConfig.email
        if emailConfig is not None:
            notifiers.append(EmailNotifier(emailConfig))
        notifierSet = NotifierSet(notifiers)
        
        # initialize langfuse
        langfuse = Langfuse(public_key=langfuseConfig.publicKey, 
            secret_key=langfuseConfig.privateKey, 
            base_url=langfuseConfig.baseUrl, tracing_enabled=langfuseConfig.enabled
        )
        if langfuseConfig.enabled:
            assert langfuse.auth_check()

        # initialize two services
        self.rabbitmqService = RabbitMQService(rabbitMQConfig, self.taskQueue, rabbitMQConfig.logger)
        self.agentService = AgentService(agentOptionsConfig, self.taskQueue, self.pgClient, notifierSet, langfuse)

    async def start(self):
        if self.rabbitmqService is None or self.agentService is None:
            sys.exit(1)

        await self.pgClient.start() # wait for postgres client to start and connect first. 
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.rabbitmqService.start())
                tg.create_task(self.agentService.start())
        finally:
            await self.pgClient.close()

