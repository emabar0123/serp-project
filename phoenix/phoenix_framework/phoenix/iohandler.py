import importlib
from .exceptions import UnsupportedTypeError, AdapterCreationError, MissingEnvironmentVariableError
import copy


class IOHandler:
    def __init__(self, config, logger=None, environment=None):
        # Deepcopy create new object with a new memory address.
        # The copy is a recursive copy, this means that changes made to the new object will not affect the phoenix
        # configuration and protect him from exception that resulting from user adapter.
        self.config = copy.deepcopy(config)
        self.input_environment = environment
        self.output_environment = environment
        self.logger = logger
        self.input_adapter = None
        self.output_adapter = None

    def initialize(self) -> None:
        self.__initialize_adapters()
        if self.input_adapter:
            self.input_adapter.initialize()
        if self.output_adapter:
            self.output_adapter.initialize()

    def __initialize_adapters(self) -> None:
        try:
            for adapter_type in ['input_type', 'output_type']:
                config = self.config['microservice_config'].get(adapter_type, {})
                if config:
                    self.__setup_adapter(config, adapter_type)
        except Exception as e:
            self.logger.error(f"Error initializing adapters: {e}")
            raise

    def __setup_adapter(self, config, adapter_type):
        key_name = next(iter(config))
        adapter_config = self.__get_adapter_config(config, key_name)
        adapter_class = self.__create_adapter(adapter_config, key_name)
        if adapter_type == "input_type":
            self.input_adapter = adapter_class
        else:
            self.output_adapter = adapter_class

    def __create_adapter(self, adapter_config, key_name):
        adapter_details = self.config['base_config']['supported_type'].get(key_name)
        module_name = adapter_details.get('module_name')
        class_name = adapter_details.get('class_name')

        try:
            adapter_module = importlib.import_module(module_name)
            adapter_class = getattr(adapter_module, class_name)
            self.logger.info(f"Initializing {module_name}.{class_name}")
            return adapter_class(logger=self.logger, config=adapter_config)
        except ModuleNotFoundError:
            raise UnsupportedTypeError(f"Module {module_name} not found")
        except AttributeError:
            raise UnsupportedTypeError(f"class {class_name} not found in module {module_name}")
        except Exception as e:
            raise AdapterCreationError(f"Error creating adapter: {e}")

    def __get_adapter_config(self, adapter_config, key_name) -> dict and str:
        config = {
            "connections": self.config['base_config']['connections'][key_name],
            "microservice_adapter_config": adapter_config[key_name]
        }
        return config

    def get_data(self):
        return self.input_adapter.get_data()

    def send_data(self, result, **kwargs):
        return self.output_adapter.send_data(result, **kwargs)

    def stop(self):
        try:
            if self.input_adapter:
                self.input_adapter.stop()
            if self.output_adapter:
                self.output_adapter.stop()
        except AttributeError:  # No stop function in IO handler
            pass

    def success_action(self, **kwargs):
        self.input_adapter.success_action(**kwargs)

    def failure_action(self, **kwargs):
        self.input_adapter.failure_action(**kwargs)

    def send_error_message(self, result, exception: Exception):
        return self.input_adapter.send_error_message(result, exception)