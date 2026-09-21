
import config.config as config
import mq.mq as mq

def main() -> None:
    # first, set up and start rabbitmq service
    mqConfig = config.build_rabbitmq()
    connection, channel = mq.start(mqConfig.url, mqConfig.connectTimeout)
    channel.basic_consume(
        queue=mqConfig.queue,
        on_message_callback=mq.handleMessage,
        auto_ack=False
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

