import pika

from typing import Tuple
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from .event import Event


def start(url: str, connectTimeout: float) -> Tuple[pika.BlockingConnection, BlockingChannel]:
    parameters = pika.URLParameters(url)
    parameters.stack_timeout = connectTimeout
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    return connection, channel

def handleMessage(channel: BlockingChannel, method: Basic.Deliver, property: BasicProperties, body: bytes) -> None:
    try:
        event = Event.fromMessage(body)
    except ValueError as err:
        # A body that does not parse now will not parse on redelivery either,
        # so requeueing it would spin forever. Drop it, loudly: the row is
        # still in the producer's event table and can be replayed by hand.
        print(f"[mq] dropping unparseable message {property.message_id}: {err}", flush=True)
        print(f"[mq] body: {body!r}", flush=True)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    redelivered = " (redelivered)" if method.redelivered else ""
    print(f"[mq] {method.routing_key}{redelivered}", flush=True)
    print(event.describe(), flush=True)

    channel.basic_ack(
        delivery_tag=method.delivery_tag
    )
