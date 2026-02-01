from abc import ABC, abstractmethod
from phoenix.microservice_interface import AdaptersInterface
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time, sleep
from pathlib import Path
import os
import glob


class FileSystemMonitor(AdaptersInterface, ABC):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.directories = []  # List of directories
        self.before = {}  # Use dictionary to maintain state for each directory
        self.timeout = None
        self.adapter_config = self.config.get("microservice_adapter_config")
        self.copying_complete_check_timeout = None

    def initialize(self):
        self.directories = self.adapter_config.get("monitors")
        if not self.directories:
            self.logger.error("Missing 'file_system_monitor' in config")
            raise ValueError("Monitor 'file_system_monitor' must be configured for all monitor items.")

        self.timeout = self.adapter_config.get("watcher_timeout")
        if not self.timeout:
            self.logger.error("Missing watcher_timeout in config")
            raise ValueError("Missing watcher_timeout in config")

        self.copying_complete_check_timeout = self.adapter_config.get("copying_complete_check_timeout")
        if not self.copying_complete_check_timeout:
            self.logger.error("Missing copying_complete_check_timeout in config")
            raise ValueError("Missing copying_complete_check_timeout in config")

        # Initialize the state for each directory
        for directory in self.directories:
            file_path = directory.get("path")
            if os.path.exists(file_path):
                self.before[file_path] = [d for d in os.listdir(file_path)]
            else:
                self.logger.error(f"Directory does not exist: {file_path}")
                raise FileNotFoundError(f"Monitor directory {file_path} does not exist.")

    def get_data(self):
        timeout = time() + self.timeout
        self.logger.debug("Starting to scan directories")

        while True:
            added_items, removed_items = {}, {}

            for item in self.directories:
                dir_path = item['path']
                search_path = os.path.join(dir_path, "**", "*")
                current_dirs = glob.glob(search_path, recursive=True)
                before_dirs = self.before.get(dir_path, [])

                new_added = {os.path.join(dir_path, d): None
                             for d in current_dirs if d not in before_dirs}
                removed = [d for d in before_dirs if d not in current_dirs]

                added_items[dir_path] = new_added
                removed_items[dir_path] = removed

                if new_added or removed:
                    timeout = time() + self.timeout
                    self.before[dir_path] = current_dirs

                # Handle newly added files and directories
                for item in new_added.get(dir_path, {}):
                    if os.path.isdir(item):
                        self.wait_until(lambda: self.wait_for_copy_completion(item),
                                        self.copying_complete_check_timeout)
                    elif os.path.isfile(item):
                        self.wait_until(lambda: self.check_if_file_copy_complete(item),
                                        self.copying_complete_check_timeout)
                    else:
                        self.logger.info(f"{item} is neither a file or directory --> SKIPPING")

                if new_added.get(dir_path) and removed_items.get(dir_path) == []:
                    self.logger.info(
                        f"FileWatcher found new directories in {dir_path}: "
                        f"{[key for key, value in new_added[dir_path].items()]}"
                    )

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
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        return total_size

    def handle_item(self, item):
        """Handles a single added file or directory"""
        if os.path.isdir(item):
            self.wait_until(lambda: self.wait_for_copy_completion(item), self.copying_complete_check_timeout)
        elif os.path.isfile(item):
            self.wait_until(lambda: self.check_if_file_copy_complete(item), self.copying_complete_check_timeout)
        else:
            self.logger.info(f"{item} is neither a file nor directory --> SKIPPING")

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
            with open(filename, "rb") as file:  # Changed mode to 'rb' for binary compatibility
                file.seek(0, os.SEEK_END)
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

