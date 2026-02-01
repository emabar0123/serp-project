import os
import sys
import time
import json
import socket
import inspect
import datetime
import threading
import traceback
import logging.handlers
from pho_logger.helpers import Helpers
from pho_logger.message_status import Status
from logging.handlers import TimedRotatingFileHandler
from pho_logger.fixed_variables import LOG_FORMAT, RETRIES, SLEEP_TIME
from pho_logger.extension_log_filters import MinLogLevelFilter, MaxLogLevelFilter, ColorizingHandler, JSONFormatter


class Logger:
    def __init__(self, project_path: str, class_name: str = None, logger_name: str = None, module_name: str = None,
                 application: str = None, category: str = "module", level=logging.DEBUG):
        """
        create log
        :param project_path: the path to the project where the log is created
        :param class_name: the class under which the log is created
        :param logger_name: the name of the log
        :param module_name: the name of the module that called the log
        :param application: the name of the project that called the log
        :param category: the log's module category
        """
        if not application:
            application = os.path.basename(project_path).replace(".py", "")

        if not module_name:
            if class_name:
                module_name = class_name.replace("Cls", "").lower()
            else:
                module_name = None

        self._content = {"machine": socket.gethostname(),
                         "os": os.name,
                         "ip": socket.gethostbyname(socket.gethostname()),
                         "application": application,
                         "module_category": category,
                         "module": module_name}
        if logger_name:
            self.logger = logging.getLogger(logger_name)
        else:
            self.logger = logging.getLogger(
                f"{self._content['application']}.{self._content['module']}")
        self.logger.propagate = False
        self.logger.setLevel(level)

    def init_stdout_logger(self, min_level: int = logging.INFO, max_level: int = logging.ERROR):
        """
        create stdout handler
        :param min_level: the minimum log level that can write to the handler
        :param max_level: the maximum log level that can write to the handler
        """
        if max_level < min_level:
            print(f"The max level: {logging.getLevelName(max_level)} has to be higher than the min level: "
                  f"{logging.getLevelName(min_level)}")
            return
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.addFilter(MinLogLevelFilter(min_level))
        stdout_handler.addFilter(MaxLogLevelFilter(max_level))
        stdout_handler.setFormatter(ColorizingHandler(LOG_FORMAT))
        if Helpers.handler_exists(self.logger, logging.StreamHandler):
            print("The handler for stdout is already exists")
            return
        self.logger.addHandler(stdout_handler)

    def init_syslog_logger(self, logstash_address: str, logstash_port: int, min_level: int = logging.INFO,
                           max_level: int = logging.ERROR):
        """
        create syslog handler
        :param logstash_address: the address to the logstash
        :param logstash_port:  the port to send the log to
        :param min_level: the minimum log level that can write to the handler
        :param max_level: the maximum log level that can write to the handler
        """
        if max_level < min_level:
            print(f"The max level: {logging.getLevelName(max_level)} has to be higher than the min level: "
                  f"{logging.getLevelName(min_level)}")
            return

        for x in range(RETRIES):
            try:
                syslog_handler = logging.handlers.SysLogHandler(address=(logstash_address, logstash_port))
                break
            except Exception as e:
                print("can't set logger {logstash_address} on port {logstash_port} retry x of {RETRIES}: error: {e}."
                      " sleeping {sleep_time} seconds".format(logstash_address=logstash_address, x=x,
                                                              logstash_port=logstash_port, RETRIES=RETRIES,
                                                              e=e, sleep_time=SLEEP_TIME * x))
                time.sleep(SLEEP_TIME * x)
        else:
            print("can't set logger {logstash_address} on port {logstash_port}".
                  format(logstash_address=logstash_address, logstash_port=logstash_port))
            return

        syslog_handler.addFilter(MinLogLevelFilter(min_level))
        syslog_handler.addFilter(MaxLogLevelFilter(max_level))
        syslog_handler.setFormatter(JSONFormatter())
        # check if the handler already exists
        if Helpers.handler_exists(self.logger, logging.handlers.SysLogHandler):
            print("The handler for syslog is already exists")
            return
        self.logger.addHandler(syslog_handler)

    def init_file_logger(self, file_path: str, when: str = "midnight", interval: int = 1, backup_count: int = 7,
                         min_level: int = logging.INFO, max_level: int = logging.ERROR):
        """
        create file handler
        :param backup_count: the number of backup log files to keep
        :param interval: indicates hoe often the rotation should occur
        :param when: specifies when the rotation should occur
        :param file_path: the path to the log file
        :param min_level: the minimum log level that can write to the handler
        :param max_level: the maximum log level that can write to the handler
        """
        if max_level < min_level:
            print(f"The max level: {logging.getLevelName(max_level)} has to be higher than the min level: "
                  f"{logging.getLevelName(min_level)}")
            return
        file_handler = TimedRotatingFileHandler(file_path, when=when, interval=interval, backupCount=backup_count)
        file_handler.addFilter(MinLogLevelFilter(min_level))
        file_handler.addFilter(MaxLogLevelFilter(max_level))
        file_handler.setFormatter(JSONFormatter())
        # check if the handler already exists
        if Helpers.handler_exists(self.logger, logging.FileHandler):
            print("The handler for file logger is already exists")
            return
        self.logger.addHandler(file_handler)

    def log(self, level: int, data_object: json, exception: Exception = None):
        """
        send the log message
        :param level: the log level
        :param data_object: the additional information of the log
        :param exception: exception
        """
        self._content["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self._content["level"] = logging.getLevelName(level)
        self._content["method"] = inspect.stack()[2].function
        self._content["pid"] = os.getpid()
        self._content["tid"] = threading.current_thread().ident
        self._content["stack"] = None
        self._content["exception"] = None
        caller_frame = inspect.currentframe().f_back.f_back
        project_path = caller_frame.f_code.co_filename
        self._content["classname"] = caller_frame.f_locals.get("self").__class__.__name__
        self._content["logger"] = f"{os.path.basename(os.path.dirname(project_path))}.{os.path.basename(project_path)}" \
            .replace(".py", "")
        if exception:
            self._content["stack"] = traceback.format_exc()
            self._content["exception"] = str(sys.exc_info()[:-1])

        self._content["data_object"] = data_object
        self.logger.log(level, self._content,
                        extra={'logger_name': self._content["logger"],
                               'method': self._content["method"],
                               'log_message': Helpers.edit_log_message(
                                   data_object, self._content["exception"], self._content["stack"])})

    def info(self, message: str, status: Status = Status.InProgress.name, elapsed: float = None,
             exception: Exception = None, message_object: object = None):
        """
        send info log message
        :param message: the log message
        :param status: the status of the message
        :param elapsed: time(milliseconds)
        :param exception: exception
        :param message_object: additional object
        """
        # the function "Helpers.get_json_parameters()" receives the variables that the function "info" receives.
        self.log(logging.INFO, Helpers.get_json_parameters(), exception)

    def error(self, message: str, status: Status = Status.InProgress.name, elapsed: float = None,
              exception: Exception = None, message_object: object = None):
        """
        send error log message
        :param message: the log message
        :param status: the status of the message
        :param elapsed: time(milliseconds)
        :param exception: exception
        :param message_object: additional object
        """
        # the function "Helpers.get_json_parameters()" receives the variables that the function "error" receives.
        self.log(logging.ERROR, Helpers.get_json_parameters(), exception)

    def debug(self, message: str, status: Status = Status.InProgress.name, elapsed: float = None,
              exception: Exception = None, message_object: object = None):
        """
        send debug log message
        :param message: the log message
        :param status: the status of the message
        :param elapsed: time(milliseconds)
        :param exception: exception
        :param message_object: additional object
        """
        # the function "Helpers.get_json_parameters()" receives the variables that the function "debug" receives.
        self.log(logging.DEBUG, Helpers.get_json_parameters(), exception)

    def warning(self, message: str, status: Status = Status.InProgress.name, elapsed: float = None,
                exception: Exception = None, message_object: object = None):
        """
        send warning log message
        :param message: the log message
        :param status: the status of the message
        :param elapsed: time(milliseconds)
        :param exception: exception
        :param message_object: additional object
        """
        # the function "Helpers.get_json_parameters()" receives the variables that the function "warning" receives.
        self.log(logging.WARNING, Helpers.get_json_parameters(), exception)

    def critical(self, message: str, status: Status = Status.InProgress.name, elapsed: float = None,
                 exception: Exception = None, message_object: object = None):
        """
        send critical log message
        :param message: the log message
        :param status: the status of the message
        :param elapsed: time(milliseconds)
        :param exception: exception
        :param message_object: additional object
        """
        # the function "Helpers.get_json_parameters()" receives the variables that the function "critical" receives.
        self.log(logging.CRITICAL, Helpers.get_json_parameters(), exception)
