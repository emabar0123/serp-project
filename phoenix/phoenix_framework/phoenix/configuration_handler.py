import os
import requests
import re


class ConfigurationHandler:
    def __init__(self, logger=None):
        self.logger = logger
        self.restapi_url = os.environ.get('CONFIG_SERVER_PHO_URL')
        self.protected_key = ['_id', 'version', 'logger']
        self._configuration_name = None
        self._container_id = None

    def __get_base_config(self):
        url = f"{self.restapi_url}/configurations/base_config"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def __get_global_settings(self):
        url = f"{self.restapi_url}/configurations/global_settings"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def __get_microservice_configuration(self, configuration_name):
        url = f"{self.restapi_url}/configurations/microservices/{configuration_name}"
        response = requests.get(url)
        response.raise_for_status()
        if response.status_code == 404:
            return
        response = response.json()
        return response

    def merge_configuration(self, base_config, microservice_config):
        if microservice_config is None:
            merge_configuration = {
                "base_config": base_config,
                "microservice_config": {}
            }
            return merge_configuration
        for key, value in microservice_config.items():
            if key in base_config and key not in self.protected_key:
                self.logger.warning(f"Overwriting existing configuration key '{key}' with value from microservice "
                                    f"configuration")
                base_config[key] = microservice_config[key]
        merge_configuration = {
            "base_config": base_config,
            "microservice_config": microservice_config
        }
        return merge_configuration

    def get_configuration(self, configuration_name):
        try:
            # Change it to one return
            if configuration_name == "base_config":
                return self.__get_base_config()

            elif configuration_name == "global_settings":
                return self.__get_global_settings()

            # todo: cr: i know it is a bit petty by using \d+ is better than [0-9]{1,5} since wi will not be limited to 99999 containers
            pod_name_regex = re.compile(r"^(?P<config_name>([^-]+(-[^-]+){3}))(-(?P<container_id>([0-9]+)(.*)))?$")
            match = pod_name_regex.match(configuration_name)
            if match:
                self._configuration_name = match.group('config_name')
                self._container_id = match.group('container_id')
            else:
                raise Exception('POD_NAME format is invalid: {0}'.format(self._configuration_name))
            return self.__get_microservice_configuration(self._configuration_name)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error retrieving configuration: {e}")
            raise
            
    @staticmethod
    def get_environment():
        environment = os.environ.get("ENV_TYPE", {})
        return environment
