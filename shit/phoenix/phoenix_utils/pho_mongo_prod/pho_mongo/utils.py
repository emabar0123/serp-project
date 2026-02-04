from prometheus_client.core import REGISTRY


class PrometheusRegistryWrapper:

    def is_counter_registered(counter_name: str) -> REGISTRY:
        """
        Check if a counter is registered to prometheus registry
        :param counter_name: Counter name to look for
        :return: True in case the counter is registered, False otherwise
        """
        return counter_name in REGISTRY._names_to_collectors

    def get_counter_from_registry(counter_name: str):
        """
        Return a counter instance from Prometheus Registry, None in case it's not found
        :param counter_name: Counter name to look for
        :return: Prometheus counter instance in case it's found, None otherwise
        """
        if counter_name in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[counter_name]
        else:
            return None
