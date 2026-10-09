from agent.agent import AgentService
from mq.mq import RabbitMQService
from config.config import HarnessConfig
from mq.event import Event

import asyncio
import sys

class Harness:
    def __init__(self, harnessConfig: HarnessConfig, agentSanityRun: bool = False):
        # a task queue for agentService to consume and rabbitmq service to produce. 
        self.taskQueue = asyncio.Queue[Event](maxsize=100) 

        agentOptionsConfig = harnessConfig.agentOptionsConfig
        rabbitMQConfig = harnessConfig.rabbitMQConfig

        # initialize two services
        self.rabbitmqService = RabbitMQService(rabbitMQConfig, self.taskQueue, rabbitMQConfig.logger)
        self.agentService = AgentService(agentOptionsConfig, self.taskQueue)
        
        self.agentSanityRun = agentSanityRun

    async def start(self):
        if self.rabbitmqService is None or self.agentService is None:
            sys.exit(1)
        
        # if this is just a test run
        if self.agentSanityRun:
            async with asyncio.TaskGroup() as tg:
                        tg.create_task(self.agentService.start(sanityRun = True))
        else:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.rabbitmqService.start())
                tg.create_task(self.agentService.start())

