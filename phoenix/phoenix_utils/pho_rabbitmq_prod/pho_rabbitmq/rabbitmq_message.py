import json


class RabbitMQMessage:
    """
    Represents a message to be sent or received through RabbitMQ.

    Attributes:
        message_type (str): Type of the message.
        payload (dict): Payload of the message.
        routing_key (str): Routing key for message routing in RabbitMQ.
        exchange (str): Exchange to which the message will be sent.
        properties (dict): Additional properties for the RabbitMQ message.

    Methods:
        to_json(): Serialize the message to a JSON-formatted string.
        from_json(json_str): Create a message object from a JSON-formatted string.
    """

    def __init__(self, payload, queue_name=None, routing_key=None, exchange=None, properties=None):
        """
        Initialize a RabbitMQMessage instance.

        Args:
            message_type (str): Type of the message.
            payload (str): Payload of the message.
            queue_name (str): The name of the buffer that stores messages.
            routing_key (str): Routing key for message routing in RabbitMQ.
            exchange (str): Exchange to which the message will be sent.
            properties (dict): Additional properties for the RabbitMQ message.
            version (int): Version of the message format.
        """
        self.payload = payload
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.exchange = exchange
        self.properties = properties or {}

    def to_json(self):
        """
        Serialize the message to a JSON-formatted string.

        Returns:
            str: JSON-formatted string representing the message.
        """
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, json_str):
        """
        Create a message object from a JSON-formatted string.

        Args:
            json_str (str): JSON-formatted string representing the message.

        Returns:
            RabbitMQMessage: Message object.
        """
        data = json.loads(json_str)
        return cls(**data)
