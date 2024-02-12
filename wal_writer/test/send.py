import pika
import ssl
from wal_writer.walwriter.config import config
def send(test_data_dir: str):

    if config.rabbitmq['encrypt']:

        ssl_context = ssl.create_default_context(cafile=config.rabbitmq['certificate_path'])
        ssl_options = pika.SSLOptions(ssl_context, config.rabbitmq['host'])

        credentials = pika.PlainCredentials(config.rabbitmq['username'], config.rabbitmq['password'])
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=config.rabbitmq['host'],
                                      port=config.rabbitmq['port'],
                                      credentials=credentials,
                                      ssl_options=ssl_options,
                                      heartbeat=5))
    else:
        credentials = pika.PlainCredentials(config.rabbitmq['username'], config.rabbitmq['password'])
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(config.rabbitmq['host'],
                                      config.rabbitmq['port'],
                                      '/',
                                      credentials))

    channel = connection.channel()

    #make queue
    channel.queue_declare(queue=config.svc_wal_writer['inbound_queue'], durable=True)

    with open(test_data_dir, 'r') as f:
        for line in f:
            channel.basic_publish(exchange='',
                                  routing_key=config.svc_wal_writer['inbound_queue'],
                                  body=line.strip(),
                                  # this is needed when queue_declare uses dureable=true (makes things persistant in the event of
                                  # an unexpected shutdown)
                                  properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
    print("Messages Queued")

    connection.close()


if __name__ == '__main__':
    send("./data/cmf_test_data.txt")