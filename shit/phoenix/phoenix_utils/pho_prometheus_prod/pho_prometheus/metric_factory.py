import os

if os.name == 'nt':
    from pho_prometheus.prometheus_remote_write import PrometheusRemote
else:
    from pho_prometheus.prometheus import Prometheus


class MetricFactory:
    @staticmethod
    def create_metric(prometheus_settings):

        if os.name == 'nt':
            return PrometheusRemote(prometheus_settings)
        else:
            return Prometheus(prometheus_settings)