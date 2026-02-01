import enum
import inspect
import json
from itertools import chain


class Helpers:

    @staticmethod
    def handler_exists(log, handler_type):
        """
        check if the handler's type already exists in the log
        :param log: the logger
        :param handler_type: the handler's type
        :return: true if it is already exists. False if it isn't
        """
        for handler in log.handlers:
            if handler.__class__ == handler_type:
                return True
        return False

    @staticmethod
    def edit_log_message(log_message: json, exception: Exception = None, stack: str = None):
        """
        edit the log message for the right format
        :param log_message: json message
        :param exception: the exception message to add
        :param stack: the traceback stack
        :return: result string with extra info for the log
        """
        result = ""
        exception_json = {}
        if exception:
            exception_json["exception"] = exception

        if stack:
            exception_json["stack"] = stack

        for key, value in chain(log_message.items(), exception_json.items()):
            result += f"{key}: {value}\n"

        return result

    @staticmethod
    def get_json_parameters():
        """
        get the parameters from the function and put them in json
        :return: json of the function's arguments
        """
        frame = inspect.currentframe().f_back
        args, _, _, values = inspect.getargvalues(frame)  # get the function parameters

        json_values = {}
        for param_name in values:
            param_value = values[param_name]
            if param_value and param_name not in ['self', 'exception']:
                if isinstance(param_value, enum.Enum):
                    param_value = param_value.name
                try:
                    json.dumps(param_value)
                except TypeError:
                    values_dict = {}
                    for attr in dir(param_value):
                        if not attr.startswith('__'):
                            values_dict[attr] = getattr(param_value, attr)
                    param_value = {key: values_dict[key] for key in values_dict if not callable(values_dict[key])}
                json_values[param_name] = param_value

        return json_values
