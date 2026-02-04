from abc import ABC
from pho_rabbitmq.rabbitmq_exceptions import *
from phoenix.microservice_interface import AdaptersInterface
import threading
from threading import Lock
import time
from typing import List
import pika
import pika.exceptions
from pho_rabbitmq.rabbitmq_message import RabbitMQMessage
import json
import traceback


class RabbitMQHandler(AdaptersInterface, ABC):
    ERROR_THRESHOLD = 3

    def __init__(self, logger, config):
        self.logger = logger
        self.channel = {}
        self.connection = {}
        self.last_error_queue_name = None
        self.rabbitmq_config = config
        self._use_process_data_events = True
        self.lock = Lock()
        self.virtual_host = None

    def initialize(self):
        self.__initialize()
        self.virtual_host = self.microservice_adapter_config.get('virtual_host')
        self.__setup_connection()
        self.__setup_queue_exchange_binding()
        self.channel["normal"].basic_qos(prefetch_count=self.prefetch_count)
        self.channel["error"].basic_qos(prefetch_count=self.prefetch_count)
        self.thread_data_events.start()

    def __process_data_events_thread(self) -> None:
        """
        The function handles heartbeats and data events in a long-lived publisher connection, ensuring continuous
        And stable data flow.
        For example: if execution takes too long, the message won't return to the queue because it unacknowledged
        as long as it do not exceeded the ack timeout configured int the rabbit cluster.
        Exceptions are ignored because send_data and get_data reinitializes connections if needed.
        """
        while self._use_process_data_events:
            with self.lock:
                try:
                    self.connection[self.virtual_host].process_data_events(time_limit=5)
                except Exception as e:
                    pass

            time.sleep(1)

    def __validate_config(self):
        if not isinstance(self.rabbitmq_config, dict):
            raise ValueError("rabbitmq_config should be a dictionary")
        required_fields = ['username', 'password', 'host', 'port']
        connections_config = self.rabbitmq_config.get('connections')
        if not connections_config:
            raise ValueError("Missing 'Connections' configuration")
        if not self.microservice_adapter_config:
            raise ValueError("Missing 'microservice_adapter_config' configuration")
        for field in required_fields:
            if connections_config.get(field) is None:
                raise ValueError(f"Missing '{field}' in 'connections' configuration.")
        if self.microservice_adapter_config.get('virtual_host') is None:
            raise ValueError(f"Missing 'virtual host' field in microservice configuration.")

    def __initialize(self):
        self.microservice_adapter_config = self.rabbitmq_config.get('microservice_adapter_config')
        self.connection_config = self.rabbitmq_config.get('connections')
        self.queue_name = self.microservice_adapter_config.get('queue_name')
        self.exchange_type = self.microservice_adapter_config.get('exchange_type', 'direct')
        self.exchange = self.microservice_adapter_config.get('exchange', self.queue_name + "." + self.exchange_type)
        self.routing_key = self.microservice_adapter_config.get('routing_key')
        self.prefetch_count = self.microservice_adapter_config.get('prefetch_count', 1)
        self.mandatory = self.microservice_adapter_config.get('mandatory', True)
        self.queue_max_priority = self.microservice_adapter_config.get('queue_max_priority', None)
        self.retry_counter = self.microservice_adapter_config.get('retry_counter', 0)
        self.consume_timeout = self.connection_config.get('consume_timeout')
        self.unroutable_error_counter = 0
        self.__validate_config()
        self.thread_data_events = threading.Thread(target=self.__process_data_events_thread, daemon=True)
        self.args = self.__create_args()

    def __create_args(self):
        """
        Create args dictionary that is being used when declaring queue.
        :return: args dictionary
        """

        args = None
        if self.queue_max_priority:
            args = {'x-max-priority': self.queue_max_priority}
        return args

    def __setup_queue_exchange_binding(self):
        self.__declare_queue()
        self.__declare_exchange()
        self.__declare_bind()

    def __setup_connection(self, channel_type=None, vhost=None):
        username = self.connection_config.get('username')
        password = self.connection_config.get('password')
        host = self.connection_config.get('host')
        port = self.connection_config.get('port')
        virtual_host = vhost or self.virtual_host
        credentials = pika.PlainCredentials(username=username, password=password, erase_on_connect=True)
        self.__start_connection(credentials, host, port, virtual_host, channel_type=channel_type)

    def __start_connection(self, credentials: pika.PlainCredentials, host: str, port: int, virtual_host: str, channel_type=None):
        if not self.connection.get(virtual_host) or self.connection.get(virtual_host).is_closed or self.channel.get(channel_type).is_closed:
            parameters = pika.ConnectionParameters(
                host=host,
                port=port,
                virtual_host=virtual_host,
                credentials=credentials
            )
            self.connection[virtual_host] = pika.BlockingConnection(parameters)
            if channel_type is None:
                self.channel["normal"] = self.channel[channel_type] = self.connection[virtual_host].channel()
                self.channel["error"] = self.connection[virtual_host].channel()
            else:
                self.channel[channel_type] = self.connection[virtual_host].channel()

    def __clear_connection(self):
        # create a shallow copy of the dictionary just by kees (not a reference and not deep copy)
        close_connections = {}
        for virtual_host in self.connection:
            close_connections[virtual_host] = self.connection[virtual_host]
        # close connections and remove them from the self.connection
        for virtual_host in close_connections:
            if self.connection[virtual_host].is_open:
                self._use_process_data_events = False
                self.thread_data_events.join()
                with self.lock:
                    self.channel = {}
                    self.connection[virtual_host].close()
                    self.connection.pop(virtual_host, "")
                    self.logger.debug("Rabbitmq connection closed.")

    def __recover_channel(self, channel_type="normal", virtual_host=None):
        with self.lock:
            self.channel[channel_type] = self.connection[virtual_host or self.virtual_host].channel()

    def __declare_queue(self, queue_name=None, channel_type="normal", virtual_host=None):
        try:
            with self.lock:
                self.channel[channel_type].queue_declare(queue=queue_name or self.queue_name, passive=True)
            queue_exists = True
        except pika.exceptions.ChannelClosed as e:
            if e.reply_code == 404:
                queue_exists = False
                self.__recover_channel(channel_type=channel_type, virtual_host=virtual_host)
            else:
                raise
        if not queue_exists:
            with self.lock:
                self.channel[channel_type].queue_declare(queue=queue_name or self.queue_name, durable=True,
                                                         arguments=self.args)

    def __declare_exchange(self, exchange=None, exchange_type=None, channel_type="normal", virtual_host=None):
        try:
            with self.lock:
                self.channel[channel_type].exchange_declare(
                    exchange=exchange or self.exchange, exchange_type=exchange_type or self.exchange_type, durable=True,
                    arguments=self.args
                )
            self.logger.info(f"Creating exchange {exchange or self.exchange} as {exchange_type or self.exchange_type}")
        except pika.exceptions.AMQPChannelError:
            self.__recover_channel(channel_type=channel_type, virtual_host=self.virtual_host)
            self.__declare_exchange(exchange=exchange or self.exchange, channel_type=channel_type, virtual_host=virtual_host)

    def __declare_bind(self, queue_name=None, exchange=None, routing_key=None, channel_type="normal"):
        with self.lock:
            self.channel[channel_type].queue_bind(
                queue=queue_name or self.queue_name,
                exchange=exchange or self.exchange,
                routing_key=routing_key or self.routing_key
            )

    def send_data(self, data: List[RabbitMQMessage], channel_type="normal", virtual_host=None):
        if virtual_host:
            channel_type = self.__declare_vhost(data, virtual_host)
        for message in data:
            exchange = message.exchange or self.exchange
            routing_key = message.routing_key or self.routing_key
            queue = message.queue_name or self.queue_name
            self.__declare_queue(queue_name=queue, channel_type=channel_type, virtual_host=virtual_host)
            self.__declare_exchange(exchange=exchange, channel_type=channel_type, virtual_host=virtual_host)
            self.__declare_bind(queue_name=queue, exchange=exchange, routing_key=routing_key, channel_type=channel_type)
            payload = message.payload
            basic_properties = message.properties or {}
            try:
                with self.lock:
                    self.channel[channel_type].confirm_delivery()
                    self.channel[channel_type].basic_publish(
                        exchange=exchange,
                        routing_key=routing_key,
                        body=payload,
                        # With kwargs we can pass different properties values from the microservice like
                        # priority, delivery_mode etc...
                        # delivery_mode: This parameter is used to specify the delivery mode for messages.
                        # It is set to 2 to ensure that messages are persisted to disk.
                        properties=pika.BasicProperties(**basic_properties, delivery_mode=2),
                        # If mandatory not exists in the configuration we set True automatic
                        mandatory=self.mandatory or True,
                    )
                    # Initialized retry counter to zero if we got none errors
                self.retry_counter = 0
            except pika.exceptions.UnroutableError as e:
                if self.retry_counter >= self.ERROR_THRESHOLD:
                    raise MaxThresholdError(self.retry_counter)
                self.retry_counter += 1
                self.logger.warning(f"Failed to send message: {e}")
                self.logger.warning("Trying to declare queue and bind exchange to queue")
                self.__declare_queue(queue_name=queue, channel_type=channel_type, virtual_host=virtual_host)
                self.__declare_bind(queue_name=queue, routing_key=routing_key, exchange=exchange,
                                    channel_type=channel_type)
                self.__recover_channel(channel_type=channel_type, virtual_host=virtual_host)
                self.send_data(data=data, channel_type=channel_type, virtual_host=virtual_host)
            except (pika.exceptions.ChannelClosed, pika.exceptions.ChannelWrongStateError,
                    pika.exceptions.StreamLostError) as e:
                self.initialize()
                if self.retry_counter >= self.ERROR_THRESHOLD:
                    raise MaxThresholdError(self.retry_counter)
                self.retry_counter += 1
                self.logger.warning(e)
                if "NOT_FOUND - no exchange" in str(e):
                    self.logger.warning(e)
                    self.__declare_exchange(exchange=exchange, channel_type=channel_type, virtual_host=virtual_host)
                self.__recover_channel(channel_type=channel_type, virtual_host=virtual_host)
                self.send_data(data=data, channel_type=channel_type, virtual_host=virtual_host)
        return True

    def __declare_vhost(self, data, virtual_host):
        if not self.connection.get(virtual_host):
            channel_type = virtual_host
            self.__setup_connection(channel_type=channel_type, vhost=virtual_host)
            declare_set = set()
            for message in data:
                declare_set.add((message.exchange, message.queue_name, message.routing_key))
            for tup in declare_set:
                exchange, queue_name, routing_key = tup
                self.__declare_exchange(exchange=exchange or self.exchange,
                                        channel_type=channel_type, virtual_host=virtual_host)
                self.__declare_queue(queue_name=queue_name or self.queue_name,
                                     channel_type=channel_type, virtual_host=virtual_host)
                self.__declare_bind(queue_name=queue_name or self.queue_name,
                                    routing_key=routing_key or self.routing_key,
                                    exchange=exchange or self.exchange,
                                    channel_type=channel_type)
        return channel_type

    def get_data(self, acknowledge=False, channel_type="normal"):
        try:
            with self.lock:
                method_frame, header_frame, body = \
                    next(self.channel[channel_type].consume(self.queue_name, inactivity_timeout=self.consume_timeout))
            if method_frame:
                if acknowledge:
                    with self.lock:
                        self.channel[channel_type].basic_ack(method_frame.delivery_tag)
                    return {"data": body}
                else:
                    return {"delivery_tag": method_frame.delivery_tag, "data": body}
            else:
                return None
        except pika.exceptions.ChannelClosed as e:
            if self.queue_name is None:
                raise
            if "NOT_FOUND - no queue" in str(e):
                self.logger.warning(e)
                self.__declare_queue(queue_name=self.queue_name, channel_type=channel_type)
                self.get_data(channel_type=channel_type)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPError) as e:
            self.logger.warning(f"Failed to get data from RabbitMQ: {e}")
            self.initialize()
            if self.retry_counter >= self.ERROR_THRESHOLD:
                raise MaxThresholdError(self.retry_counter,
                                        "Reached the maximum threshold counter error while attempting to get data.")
            self.retry_counter += 1

    def acknowledge_message(self, delivery_tag):
        if not self.channel["normal"].is_open:
            self.__recover_channel()
            delivery_tag = self.get_data()["delivery_tag"]

        with self.lock:
            self.channel["normal"].basic_ack(delivery_tag)

    def nack_message(self, delivery_tag):
        if not self.channel["normal"].is_open:
            self.__recover_channel()
            delivery_tag = self.get_data()["delivery_tag"]
            
        with self.lock:
            self.channel["normal"].basic_nack(delivery_tag)

    def success_action(self, **kwargs):
        delivery_tag = kwargs['delivery_tag']
        if delivery_tag:
            self.acknowledge_message(delivery_tag)

    def failure_action(self, **kwargs):
        delivery_tag = kwargs['delivery_tag']
        if delivery_tag:
            self.nack_message(delivery_tag)

    def stop(self):
        if not self.connection:
            return
        self.logger.info("The connection to the RabbitMQ server has been closed")
        self.__clear_connection()

    def send_error_message(self, result, exception: Exception):
        queue_name = f'{self.queue_name}.error.{exception.__class__.__name__}'
        exchange_name = f'{self.exchange}.error'
        if self.last_error_queue_name != queue_name:
            self.last_error_queue_name = queue_name
            self.__declare_queue(queue_name=queue_name, channel_type="error")
            self.__declare_exchange(exchange=exchange_name, exchange_type='direct', channel_type="error")
            self.__declare_bind(queue_name=queue_name, exchange=exchange_name, routing_key=queue_name,
                                channel_type="error")

        headers = {'headers':
                       {'exception': str(exception),
                        'traceback': "".join(traceback.format_tb(exception.__traceback__))}}

        self.send_data(data=[
            RabbitMQMessage(json.dumps(result), queue_name=queue_name, routing_key=queue_name, exchange=exchange_name,
                            properties=headers)], channel_type="error")

    def __del__(self):
        self.stop()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
