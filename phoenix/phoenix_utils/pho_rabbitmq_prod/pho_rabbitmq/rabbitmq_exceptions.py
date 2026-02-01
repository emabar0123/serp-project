class RabbitMQConnectionError(Exception):
    pass


class RabbitMQDisconnectError(Exception):
    pass


class RabbitMQChannelError(Exception):
    pass


class RabbitMQExchangeError(Exception):
    pass


class RabbitMQQueueError(Exception):
    pass


class RabbitMQQueueBindError(Exception):
    pass


class RabbitMQSendDataError(Exception):
    pass


class RabbitMQGetDataError(Exception):
    pass


class RabbitMQAcknowledgeError(Exception):
    pass


class MaxThresholdError(Exception):
    def __init__(self, threshold,
                 message="Reached the maximum threshold counter error while attempting to send the message"):
        self.threshold = threshold
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message}: {self.threshold}"
