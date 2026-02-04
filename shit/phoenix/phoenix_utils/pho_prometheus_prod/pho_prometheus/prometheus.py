from pho_prometheus.abstract_prometheus import AbstractPrometheus
from prometheus_client import Counter, Gauge, start_http_server


class Prometheus(AbstractPrometheus):
    def __init__(self, prometheus_settings):
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
                    c = Counter(counter, "count number of processed files per module in a host",
                                self.counters[counter])
                self.prometheus_metrics["counters"][counter] = c
            for gauge in self.gauges:
                g = self.prometheus_metrics["gauges"].get(gauge)
                if not g:
                    g = Gauge(gauge, "count number of processed files per module in a host", self.gauges[gauge])
                self.prometheus_metrics["gauges"][gauge] = g

            start_http_server(int(self.prometheus_port))
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
            c = Counter(metric_name, "count number of processed files per module in a host", list(values.keys()))
            self.prometheus_metrics["counters"][metric_name] = c
            self.counters[metric_name] = list(values.keys())
        else:
            if list(values.keys()) != self.counters[metric_name]:
                print('values does not match labels')
                return

        c.labels(**values).inc()
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
            g = Gauge(metric_name, "count number of processed files per module in a host", list(values.keys()))
            self.prometheus_metrics["gauges"][metric_name] = g
            self.gauges[metric_name] = list(values.keys())
        else:
            if list(values.keys()) != self.gauges[metric_name]:
                print('values does not match labels')
                return

        g.labels(**values).set(time_count)
        return [values, time_count]

