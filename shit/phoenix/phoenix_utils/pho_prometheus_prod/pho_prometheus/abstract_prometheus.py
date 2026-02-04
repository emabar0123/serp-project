from abc import ABC, abstractmethod


class AbstractPrometheus(ABC):

    @abstractmethod
    def __init__(self, prometheus_settings):
        pass

    @abstractmethod
    def init_prometheus(self):
        pass

    @abstractmethod
    def increment_count(self, metric_name: str, values: dict):
        pass

    @abstractmethod
    def set_time_metric(self, metric_name: str, time_count: int, values: dict):
        pass
