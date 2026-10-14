from agent.agent import AgentService
from mq.mq import RabbitMQService
from config.config import HarnessConfig
from mq.event import Event
from postgres.postgres import PostgresClient

import asyncio
from logging import Logger
import sys

class Harness:
    # TODO: make the logger variable typed.
    def __init__(self, harnessConfig: HarnessConfig, sanity_check: bool = False):
        # a task queue for agentService to consume and rabbitmq service to produce.
        self.taskQueue = asyncio.Queue[Event](maxsize=100)

        agentOptionsConfig = harnessConfig.agentOptionsConfig
        rabbitMQConfig = harnessConfig.rabbitMQConfig
        postgresConfig = harnessConfig.postgresConfig

        self.pgClient = PostgresClient(postgresConfig)

        # initialize two services
        self.rabbitmqService = RabbitMQService(rabbitMQConfig, self.taskQueue, rabbitMQConfig.logger, sanity_check=sanity_check)
        self.agentService = AgentService(agentOptionsConfig, self.taskQueue, self.pgClient, sanity_check)

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

