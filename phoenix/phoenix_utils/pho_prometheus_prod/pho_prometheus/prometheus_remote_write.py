from pho_prometheus.abstract_prometheus import AbstractPrometheus
from opentelemetry import metrics
from opentelemetry.exporter.prometheus_remote_write import PrometheusRemoteWriteMetricsExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
import time


class PrometheusRemote(AbstractPrometheus):
    def __init__(self, prometheus_settings):
        exporter = PrometheusRemoteWriteMetricsExporter(endpoint=prometheus_settings['prometheus_server'])
        reader = PeriodicExportingMetricReader(exporter, 500)
        provider = MeterProvider(metric_readers=[reader])
        metrics.set_meter_provider(provider)
        self.metric_creator = metrics.get_meter("metrics_creator")
        self.prometheus_port = prometheus_settings['prometheus_port']
        self.counters = prometheus_settings['counters']
        self.gauges = prometheus_settings['gauges']
        self.prometheus_metrics = {"counters": {},
                                   "gauges": {}}

    def init_prometheus(self):
        try:
            for counter in self.counters:
                c = self.prometheus_metrics["counters"].get(counter)
                if not c:
                    c = self.metric_creator.create_counter(counter + "_total",
                                                           description="count number of processed files per module in a host")
                self.prometheus_metrics["counters"][counter] = c
            for gauge in self.gauges:
                g = self.prometheus_metrics["gauges"].get(gauge)
                if not g:
                    g = self.metric_creator.create_gauge(gauge,
                                                         description="count number of processed files per module in a host")
                self.prometheus_metrics["gauges"][gauge] = g
        except Exception as e:
            print(e)
            print("prometheus server is up skipping")

    def increment_count(self, metric_name: str, values: dict):
        """
        increments the values of a metric
        :param metric_name: str - the metric to increment its values
        :param values: dict - values of the metric
        :return: dict - returns values if run successfully
         """
        c = self.prometheus_metrics["counters"].get(metric_name)
        if not c:
            c = self.metric_creator.create_counter(metric_name,
                                                   description="count number of processed files per module in a host")
            self.prometheus_metrics["counters"][metric_name] = c
            self.counters[metric_name] = list(values.keys())
        else:
            if list(values.keys()) != self.counters[metric_name]:
                print('values does not match labels')
                return
        c.add(1, values)
        return values

    def set_time_metric(self, metric_name: str, time_count: int, values: dict):
        """
        sets time in a metric
        :param metric_name: str - the metric to set its time
        :param time_count: int - time in milliseconds
        :param values: dict - values of the metric
        :return: list[dict,float] - returns values and time if run successfully
        """

        g = self.prometheus_metrics["gauges"].get(metric_name)
        if not g:
            g = self.metric_creator.create_gauge(metric_name,
                                                 description="count number of processed files per module in a host")
            self.prometheus_metrics["gauges"][metric_name] = g
            self.gauges[metric_name] = list(values.keys())
        else:
            if list(values.keys()) != self.gauges[metric_name]:
                print('values does not match labels')
                return

        g.set(time_count, values)
        return [values, time_count]

