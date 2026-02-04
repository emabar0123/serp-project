class AdapterCreationError(Exception):
    pass


class UnsupportedTypeError(AdapterCreationError):
    pass


class ConnectionError(Exception):
    pass


class PublishingError(Exception):
    pass


class RetrievalError(Exception):
    pass


class MissingEnvironmentVariableError(Exception):
    pass


class MissingBasConfiguration(Exception):
    pass


class UnackErrorQueueException(Exception):
    pass

class MafiaException(Exception):
    pass