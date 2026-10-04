from agent.agent import AgentService
from mq.mq import RabbitMQService
from config.config import HarnessConfig
from mq.event import Event

import asyncio
from logging import Logger
import sys

class Harness:
    # TODO: make the logger variable typed. 
    def __init__(self, harnessConfig: HarnessConfig):
        # a task queue for agentService to consume and rabbitmq service to produce. 
        self.taskQueue = asyncio.Queue[Event](maxsize=100) 

        agentOptionsConfig = harnessConfig.agentOptionsConfig
        rabbitMQConfig = harnessConfig.rabbitMQConfig

        # initialize two services
        self.rabbitmqService = RabbitMQService(rabbitMQConfig, self.taskQueue, rabbitMQConfig.logger)
        self.agentService = AgentService(agentOptionsConfig, self.taskQueue)

    async def start(self):
        if self.rabbitmqService is None or self.agentService is None:
            # TODO. exit on error. 
            sys.exit(1)
        
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.rabbitmqService.start())
            tg.create_task(self.agentService.start())

