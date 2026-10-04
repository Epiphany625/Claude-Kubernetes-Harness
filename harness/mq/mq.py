
import aio_pika
from aio_pika.abc import AbstractIncomingMessage

import asyncio
from logging import Logger
from config.config import RabbitMQConfig

from .event import Event



class RabbitMQService:
    def __init__(self, config: RabbitMQConfig, taskQueue: asyncio.Queue[Event], logger: Logger, prefetch: int = 10):
        self.url = config.url
        self.connectTimeout = config.connectTimeout
        self.taskQueue = taskQueue
        self.logger = logger
        self.prefetch = prefetch 
        self.queueName = config.queue

    async def start(self):

        connection = await aio_pika.connect_robust(self.url, timeout=self.connectTimeout)

        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=self.prefetch)
            mq_queue = await channel.get_queue(self.queueName)
            self.logger.info("[mq] consuming from %s", self.queueName)
            async with mq_queue.iterator() as messages:
                async for message in messages:
                    await self.handleMessage(message)

    async def handleMessage(self, message: AbstractIncomingMessage) -> None:
        try:
            event = Event.fromMessage(message.body)
        except ValueError as err:
            # A body that does not parse now will not parse on redelivery either,
            # so requeueing it would spin forever. Drop it, loudly: the row is
            # still in the producer's event table and can be replayed by hand.
            self.logger.warning("[mq] dropping unparseable message %s: %s", message.message_id, err)
            self.logger.warning("[mq] body: %r", message.body)
            await message.nack(requeue=False)
            return
        
        redelivered = " (redelivered)" if message.redelivered else ""
        self.logger.info("[mq] %s%s", message.routing_key, redelivered)
        self.logger.info(event.describe())
 
        # Waits here if the asyncio.Queue is full (backpressure). While we wait,
        # this message stays unacked, so RabbitMQ stops sending after `prefetch`.
        await self.taskQueue.put(event)
 
        await message.ack()