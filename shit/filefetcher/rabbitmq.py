import json
import pika


class Rabbit:
    def __init__(self, url):
        self.conn = pika.BlockingConnection(pika.URLParameters(url))
        self.ch = self.conn.channel()
        self.ch.queue_declare(queue="scanner_queue")
        self.ch.queue_declare(queue="downloader_verify_queue")

    def publish(self, queue, message):
        self.ch.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(message),
        )

    def consume_one(self, queue):
        method, _, body = self.ch.basic_get(queue)
        if not method:
            return None
        self.ch.basic_ack(method.delivery_tag)
        return json.loads(body)


# OLD
# import pika
# import json
#
# import json
# from typing import Any, Dict, Optional
#
# import pika
#
#
# class RabbitMQ:
#     def __init__(self, host: str, queue: str):
#         self.host = host
#         self.queue = queue
#         self.connection = None
#         self.channel = None
#
#     def connect(self):
#         self.connection = pika.BlockingConnection(
#             pika.ConnectionParameters(host=self.host)
#         )
#         self.channel = self.connection.channel()
#         self.channel.queue_declare(queue=self.queue, durable=True)
#
#     def publish(self, message: dict):
#         self.channel.basic_publish(
#             exchange="",
#             routing_key=self.queue,
#             body=json.dumps(message),
#             properties=pika.BasicProperties(
#                 delivery_mode=2  # persistent
#             )
#         )
#
#     def consume(self, callback):
#         self.channel.basic_qos(prefetch_count=1)
#         self.channel.basic_consume(
#             queue=self.queue,
#             on_message_callback=callback
#         )
#         self.channel.start_consuming()
#
#     def ack(self, tag):
#         self.channel.basic_ack(tag)
#
#     def close(self):
#         if self.connection:
#             self.connection.close()
#
