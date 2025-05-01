#
# AtriumDB is a timeseries database software designed to best handle the unique
# features and challenges that arise from clinical waveform data.
#
# Copyright (c) 2025 The Hospital for Sick Children.
#
# This file is part of AtriumDB 
# (see atriumdb.io).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
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