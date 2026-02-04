from abc import ABC
from phoenix.microservice_interface import AdaptersInterface
from time import time, sleep
import os


class FileWatcher(AdaptersInterface, ABC):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.directory = None
        self.before = None
        self.timeout = None
        self.copying_complete_check_timeout = None

    def initialize(self):
        self.directory = self.config.get("microservice_adapter_config").get("directory")
        if not self.directory:
            self.logger.error("Missing directory in config")
            raise

        self.timeout = self.config.get("microservice_adapter_config").get("watcher_timeout")
        if not self.timeout:
            self.logger.error("Missing watcher_timeout in config")
            raise

        self.copying_complete_check_timeout = self.config.get("microservice_adapter_config")\
            .get("copying_complete_check_timeout")

        if not self.timeout:
            self.logger.error("Missing copying_complete_check_timeout in config")
            raise

        self.before = [d for d in os.listdir(self.directory)]

    def get_data(self):

        timeout = time() + self.timeout
        self.logger.debug(f"Starting to scan directory {self.directory}")

        while True:
            current_dirs = [d for d in os.listdir(self.directory)]
            added_items = dict([(os.path.join(self.directory, d), None)
                                for d in current_dirs if d not in self.before])
            removed = [d for d in self.before if d not in current_dirs]

            if added_items or removed:
                timeout = time() + self.timeout
                self.before = current_dirs

                if added_items:
                    for item in added_items:

                        if os.path.isdir(item):
                            self.wait_until(lambda: self.wait_for_copy_completion(item)
                                            is True,  self.copying_complete_check_timeout)

                        elif os.path.isfile(item):
                            self.wait_until(lambda: self.check_if_file_copy_complete(item)
                                            is True,  self.copying_complete_check_timeout)
                        else:
                            self.logger.info(f"{item} is neither a file or directory --> SKIPPING")
                            return

                    self.logger.info(f"FileWatcher found new directories: "
                                     f"{[key for key, value in added_items.items()]}")
                    return added_items

            if time() > timeout:
                self.logger.debug("File watcher reached timeout")
                return
            sleep(1)

    @staticmethod
    def wait_until(delegate, timeout: int):
        end = time() + timeout

        while time() < end:
            if delegate():
                return True
            else:
                sleep(0.1)
        return False

    @staticmethod
    def get_directory_size(path):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)

                # Skip if it is a symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        return total_size

    def wait_for_copy_completion(self, directory, check_interval=1, stable_count=3):
        stable_size = self.get_directory_size(directory)
        stable_checks = 0

        while stable_checks < stable_count:
            sleep(check_interval)
            current_size = self.get_directory_size(directory)
            if current_size == stable_size:
                stable_checks += 1
            else:
                return False

            self.logger.info(f"Current size: {current_size}, Stable checks {stable_checks}")
        return True

    def check_if_file_copy_complete(self, filename):
        initial_size = self.get_file_size(filename)
        sleep(0.2)
        final_size = self.get_file_size(filename)

        if initial_size == final_size and initial_size >= 0:
            return True
        else:
            return False

    @staticmethod
    def get_file_size(filename):
        try:
            with open(filename, "r") as file:
                file.seek(0, 2)
                size = file.tell()
                return size

        except Exception as e:
            return -1

    def stop(self):
        pass

    def success_action(self, **kwargs):
        pass

    def failure_action(self, **kwargs):
        pass

    def send_error_message(self, result, exception: Exception):
        pass

    def send_data(self, result, **kwargs):
        pass
