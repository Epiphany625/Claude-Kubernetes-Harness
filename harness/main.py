
import config.config as config
from agent.agent import Harness
import asyncio
import mq.mq as mq

from functools import partial

async def main() -> None:

    harnessConfig = config.load_config()

    harness = Harness(harnessConfig.agentOptionsConfig)

    # set up and start rabbitmq service
    connection, channel = mq.start(harnessConfig.rabbitMQConfig.url, harnessConfig.rabbitMQConfig.connectTimeout)
    channel.basic_consume(
        queue=harnessConfig.rabbitMQConfig.queue,
        on_message_callback=partial(mq.handleMessage, harness=harness),
        auto_ack=False
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == "__main__":
    asyncio.run(main())

