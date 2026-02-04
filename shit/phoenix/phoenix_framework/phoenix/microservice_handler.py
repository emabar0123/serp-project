import copy
import logging
import os
import time
import json
from .iohandler import IOHandler
from .configuration_handler import ConfigurationHandler
from .exceptions import MissingBasConfiguration, UnackErrorQueueException, MafiaException
from pho_logger.logger import Logger
from pho_prometheus.metric_factory import MetricFactory
from threading import Thread, Event
import sys
import traceback


class MicroserviceHandler:
    def __init__(self,
                 logger_project_path,
                 microservice_class=None,
                 configuration_handler=None,
                 loggerHandler=None,
                 metrics_handler=None,
                 io_handler=None):
        self.protected_key = ['_id', 'active_version', 'environment']
        self.logger = loggerHandler or Logger(project_path=logger_project_path, class_name=microservice_class.__name__)
        self.microservice_name = os.environ.get("POD_NAME", {})
        self.microservice_environment = None
        self.microservice_class = microservice_class
        self.microservice_instance = None
        self.metrics_handler = metrics_handler
        self.configuration_handler = configuration_handler or ConfigurationHandler(logger=self.logger)
        self.io_handler = io_handler
        self.base_config = None
        self.global_settings = None
        self.microservice_config = None
        self.merge_configuration = None
        self.configuration_check_stop_event = Event()
        self.configuration_check_halt = Event()
        self.configuration_check_thread = Thread(target=self.__check_for_configuration_changes)
        self.initialize_phoenix = True
        self.run_phoenix = True
        self.exception_counter = 0
        self.periodic_sleep = 0
        self.run_mode = None
        self.prometheus = None
        self.prometheus_epoch = Thread(target=self.__update_epoch_metrics)
        self.prometheus_epoch.daemon = True

    def __initialize(self) -> None:
        try:
            self.base_config = self.configuration_handler.get_configuration("base_config")
            self.global_settings = self.configuration_handler.get_configuration("global_settings")
            if not self.microservice_name:
                raise ValueError("Add POD_NAME global environment")
            self.microservice_config = self.configuration_handler.get_configuration(self.microservice_name)
            self.microservice_environment = self.configuration_handler.get_environment()
            self.merge_configuration = self.configuration_handler.merge_configuration(self.base_config,
                                                                                      self.microservice_config)

            log_config = self.merge_configuration.get('microservice_config', {}).get(
                "logger") or self.base_config.get("logger")
            if not log_config:
                raise ValueError("The logger configuration is missing.")

            items_config = copy.deepcopy(log_config)
            self.__logger_handlers(items_config.get("handlers", {}))

            prometheus_config = self.merge_configuration.get('microservice_config', {}).get(
                "prometheus") or self.base_config.get("prometheus")
            if not prometheus_config:
                raise ValueError("The prometheus configuration is missing.")

            self.prometheus = MetricFactory.create_metric(prometheus_config)
            self.prometheus.init_prometheus()

            for config_type in ["module_name", "application", "category"]:
                value = items_config.get(config_type, "")
                if value:
                    self.logger._content[config_type] = value

            logger_name = items_config.get("logger_name", "")
            if logger_name:
                self.logger.logger.name = logger_name

            level = items_config.get("level", "")
            if level:
                self.logger.logger.setLevel(getattr(logging, level))

            # Config general validation before run microservice
            if self.base_config is None:
                raise MissingBasConfiguration

            if self.global_settings is None:
                self.logger.warning("No global settings")

            if self.microservice_config is None:
                self.logger.warning("No microservice configuration found running without IO")

            if not self.configuration_check_thread.is_alive():
                self.configuration_check_thread.start()

            # Check the configuration to determine how to handle input and output
            if self.io_handler is None:
                self.logger.info("Initialized IOHandler")
                self.io_handler = IOHandler(self.merge_configuration, self.logger, self.microservice_environment)
                self.io_handler.initialize()
            # Initialize microservice class
            self.microservice_instance = self.microservice_class(self.logger,
                                                                 self.merge_configuration['microservice_config'][
                                                                     'costume_config'],
                                                                 self.global_settings,
                                                                 self.io_handler)
            self.input_type = self.merge_configuration['microservice_config'].get("input_type", {})
            self.output_type = self.merge_configuration['microservice_config'].get("output_type", {})

            # Get periodic sleep from configuration
            self.periodic_sleep = self.microservice_config.get("periodic_sleep", 0)
            self.run_mode = self.microservice_config.get("run_mode", "default")
            self.configuration_check_halt.clear()
            self.logger.info("Initialize MicroserviceHandler")
            if not self.prometheus_epoch.is_alive():
                self.prometheus_epoch.start()
        except KeyboardInterrupt:
            self.__stop()
        except Exception:
            raise

    def phoenix_main(self) -> None:
        while self.run_phoenix:
            try:
                if self.initialize_phoenix:
                    self.__initialize()
                    self.initialize_phoenix = False
                self.__execute_phoenix()
            except Exception:
                if self.configuration_check_thread.is_alive():
                    self.configuration_check_stop_event.set()
                    self.configuration_check_thread.join()
                raise

    def get_label_value(self, label: str, data: dict = None) -> str:
        """
        need to implement to return the value of a label that is not in the default labels
        :param label: str - the label not in default labels
        :return: str - the value of the label
        """
        if hasattr(self.microservice_instance, "get_label_value"):
            try:
                return self.microservice_instance.get_label_value(label, data)
            except Exception as e:
                raise NotImplementedError from e
        raise NotImplementedError

    def prometheus_counter(self, counter_name, module, got_exception, exception_type, data=None):
        """
        receives a counter and increments its corresponding labels
        :param counter_name: str - the counter to increment
        :param module: str- the module name
        :param got_exception: bool - if there was an exception
        :param exception_type: str - the exception type
        :return: dict - returns the values of the counter that were incremented if successful
        """
        default_labels = {"module": module, "status": 'Failed' if got_exception else "Finished",
                          "error_type": exception_type}
        value = {}
        if self.prometheus.counters.get(counter_name):
            for label in self.prometheus.counters[counter_name]:
                value[label] = default_labels[label] if label in default_labels else self.get_label_value(label, data)
        else:
            self.logger.warning(f"The prometheus counter {counter_name} is not defined.")
            return
        return self.prometheus.increment_count(counter_name, value)

    def prometheus_gauge(self, gauge_name, module, start_time, end_time, data=None):
        """
        receives a gauge and increments its corresponding labels
        :param gauge_name: str - the gauge to increment
        :param module: str- the module name
        :param start_time: float - start time of the process
        :param end_time: float - end time of the process
        :return: list[dict,float] - returns the values of the gauge that were incremented and time if successfully
        """
        default_labels = {"module": module}
        value = {}
        if self.prometheus.gauges.get(gauge_name):
            for label in self.prometheus.gauges[gauge_name]:
                value[label] = default_labels[label] if label in default_labels else self.get_label_value(label, data)
        else:
            self.logger.warning(f"The prometheus gauge {gauge_name} is not defined.")
            return
        return self.prometheus.set_time_metric(gauge_name, (end_time - start_time) * 1000, value)

    def __execute_phoenix(self) -> None:
        data = None
        result = None
        got_exception = False
        start_time = None
        end_time = None
        exception_type = None
        try:
            if self.input_type:
                data = self.io_handler.get_data()
                if data:
                    # We got data from io_handler so execute function with data.
                    # To allowed send data during the process.
                    try:
                        start_time = time.time()
                        result = self.microservice_instance.execute(**data)
                        end_time = time.time()
                        if self.metrics_handler is not None:
                            self.metrics_handler.count_input_message()


                    except MafiaException as me:
                        self.logger.info(f"A MafiaException has occured {me} running failure_action.")                  
                        self.microservice_instance.failure_action(exception=me)
                        self.logger.info(f"A MafiaException has occured {me} running __handle_failure.")     
                        end_time, exception_type, got_exception = self.__handle_failure(data, end_time, exception_type,
                                                                                        got_exception, me)

                        if self.run_mode == "single_message":
                            self.logger.info("A MafiaException has occured, exiting")
                            self.__stop()
                            sys.exit(0)
                    except UnackErrorQueueException as e:
                        end_time = time.time()
                        exception_type = e.__str__()
                        got_exception = True
                        json_string = data["data"].decode('utf-8').replace('\\r', '').replace('\\n', '')
                        data_object = json.loads(json_string)
                        self.logger.info(f"An UnackErrorQueueException has occured {e} running send_error_message.")   
                        self.io_handler.send_error_message(result=data_object, exception=e)
                        # Unack message
                        self.io_handler.failure_action(**data)
                    except Exception as e:
                        self.logger.info(f"An Exception has occured {e} running __handle_failure.")   
                        end_time, exception_type, got_exception = self.__handle_failure(data, end_time, exception_type,
                                                                                        got_exception, e)

            else:
                # If we didn't define "input type" in configuration file
                # the microservice will run without incoming data.
                start_time = time.time()
                result = self.microservice_instance.execute()
                end_time = time.time()
            # message routing is handled inside exceptions if one occurred, so function returns here.
            if got_exception:
                return

            # We check that we define output_type in configuration, and we got output from the microservice.
            if result and self.output_type:
                if not isinstance(result, list):
                    result = [result]
                self.io_handler.send_data(result)
            if self.metrics_handler is not None:
                self.metrics_handler.count_output_message()
            # If the "manual_action_on_success" flag is set to 'true' in the configuration file, the developer will be
            # responsible for managing the success logic after execution and exception handling process.
            if "manual_action_on_success" not in self.microservice_config and self.input_type and data:
                self.microservice_instance.after_success()
                self.io_handler.success_action(**data)
                self.exception_counter = 0
            # Check if need to perform periodic sleep between executions
            if not self.input_type and self.periodic_sleep > 0:
                self.logger.info(f"Entering periodic sleep of {self.periodic_sleep} seconds")
                time.sleep(self.periodic_sleep)
            elif not self.input_type and self.run_mode == "script":
                self.logger.info("Running service 'as a script'")
                self.__stop()
            if self.run_mode == "single_message" and data:
                self.logger.info("finished handeling single message, exiting")
                self.__stop()
                sys.exit(0)

        except Exception as e:
            end_time = time.time()
            exception_type = e.__str__()
            got_exception = True
            self.logger.error(message=str(e), exception=e)
            if self.exception_counter > 10:
                self.logger.error("The process has been shutting down due to too many errors.")
                self.__stop()
            self.exception_counter += 1

        finally:
            if start_time is None or end_time is None:
                # if process something went wrong before the exec of the code (start_time = None)
                # or if something went wrong after and the exception didn't catch that (end_time = None)
                # no metrics are added
                return
            msg = {}
            if not self.input_type.get("rabbitmq"):
                msg = data
            elif isinstance(data, (str, bytes, bytearray)):
                msg = json.loads(data)
            elif isinstance(data, (dict)):
                msg = json.loads(data.get("data", {})) if data else {}

            for metric in self.prometheus.gauges:
                self.prometheus_gauge(metric, self.microservice_name, start_time, end_time, msg)
            for metric in self.prometheus.counters:
                if (metric ==  "phoenix_errors" and got_exception) or (metric !=  "phoenix_errors"):
                    self.prometheus_counter(metric, self.microservice_name, got_exception, exception_type, msg)

    def __handle_failure(self, data, end_time, exception_type, got_exception, me):
        end_time = time.time()
        exception_type = me.__str__()
        got_exception = True
        if hasattr(self.microservice_instance, "msg"):
            data_object = self.microservice_instance.msg
        else:
            json_string = data["data"].decode('utf-8').replace('\\r', '').replace('\\n', '')
            data_object = json.loads(json_string)
        self.io_handler.send_error_message(result=data_object, exception=me)
        self.io_handler.success_action(**data)
        return end_time, exception_type, got_exception

    def __stop(self) -> None:
        self.configuration_check_stop_event.set()
        self.configuration_check_thread.join()
        self.logger.info("Stopping MicroserviceHandler")
        if self.microservice_instance is not None:
            self.microservice_instance.stop()
        self.io_handler.stop()
        self.run_phoenix = False

    def __restart(self) -> None:
        self.logger.info("Restarting MicroserviceHandler")
        self.microservice_instance.stop()
        self.configuration_check_halt.set()
        self.initialize_phoenix = True

    def __check_for_configuration_changes(self) -> None:
        self.logger.info("Starting configuration thread")
        while not self.configuration_check_stop_event.is_set():
            if self.configuration_check_halt.is_set():
                # If entered initialization routine due to configuration change, skip checking for change until done.
                time.sleep(1)
                continue
            try:
                new_base_config = self.configuration_handler.get_configuration("base_config")
                new_global_settings = self.configuration_handler.get_configuration("global_settings")
                new_microservice_config = self.configuration_handler.get_configuration(
                    configuration_name=self.microservice_name)
            except Exception as e:
                self.logger.error(f"Exception while trying to get configuration from server. {e}")

            # Restart configuration only when active_version changes
            if self.base_config.get('active_version') != new_base_config.get('active_version'):
                # Check if base_config changed
                self.logger.info("Base configuration changed. Restarting MicroserviceHandler.")
                self.__restart()

            if self.global_settings.get('active_version') != new_global_settings.get('active_version'):
                # Check if global_settings changed
                self.logger.info("Global settings changed. Restarting MicroserviceHandler.")
                self.__restart()

            if self.microservice_config.get('active_version') != new_microservice_config.get('active_version'):
                # Check if microservice_config changed
                self.logger.info("Microservice configuration changed. Restarting MicroserviceHandler.")
                self.__restart()

            self.configuration_check_stop_event.wait(10)
        self.logger.info("Stopping configuration thread")

    def __logger_handlers(self, handlers):
        if not handlers:
            self.logger.init_stdout_logger()
            return
        for handler_type, handler_values in handlers.items():
            handler_values["min_level"] = getattr(logging, handler_values["min_level"])
            handler_values["max_level"] = getattr(logging, handler_values["max_level"])
            if handler_type == "file":
                path = handler_values.get("file_path", "")
                if not path:
                    raise AttributeError("There are no path for the log file handler.")
                if os.path.sep not in path:  # check if the log file path is a full path
                    path = os.path.join(os.getcwd(), path)
                if not os.path.exists(path):
                    with open(path, 'x'):
                        pass
                handler_values["file_path"] = path
            function = getattr(self.logger, f"init_{handler_type}_logger")
            function(**handler_values)

    def __update_epoch_metrics(self):
        while True:
            epoch_time = time.time()
            # prometheus_gauge sets time as end_time - start_time so to see the pure epoch start is set to 0
            # to show epoch_time in seconds we divide it by 1000 because prometheus_gauge multiplies by 1000 (to show milliseconds)
            self.prometheus_gauge("phoenix_epoch", self.microservice_name, 0, epoch_time / 1000)
            time.sleep(60)

