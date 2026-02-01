from abc import ABC, abstractmethod


class Extract(ABC):
    @abstractmethod
    def __init__(self, logger, config):
        self.logger = logger
        self.config = config

    @abstractmethod
    def extract_file(self, input_file, output_directory, already_tried=False, password=None):
        pass
