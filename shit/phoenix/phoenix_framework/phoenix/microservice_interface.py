from abc import ABC, abstractmethod


class Microservice(ABC):
    def __init__(self, logger, config, global_config, io_handler):
        self.io_handler = io_handler
        self.logger = logger
        self.config = config
        self.global_config = global_config

    def send_data(self, result, **data):
        self.io_handler.send_data(result, **data)

    @abstractmethod
    def after_success(self):
        pass

    @abstractmethod
    def execute(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        raise NotImplementedError

    def failure_action(self, **kwargs):
        pass


class MetricsHandler(ABC):
    @abstractmethod
    def count_input_message(self):
        pass

    @abstractmethod
    def count_output_message(self):
        pass

    @abstractmethod
    def time_execution(self, execution_time):
        pass

    @abstractmethod
    def time_execution_failure(self):
        pass


class ConfigurationHandler(ABC):
    @abstractmethod
    def get_configuration(self, configuration_name):
        raise NotImplementedError

    @abstractmethod
    def merge_configuration(self, base_config, microservice_config):
        raise NotImplementedError


class AdaptersInterface(ABC):
    @abstractmethod
    def get_data(self):
        """Retrieve data from the source"""
        pass

    @abstractmethod
    def send_data(self, result, **kwargs):
        """Send data to the destination"""
        pass

    @abstractmethod
    def success_action(self, **kwargs):
        """Define actions to be taken on successful data processing. """
        pass

    @abstractmethod
    def failure_action(self, **kwargs):
        """Define actions to be taken on failed data processing. """
        pass

    @abstractmethod
    def stop(self):
        """Define actions to be taken on stopping."""
        pass

