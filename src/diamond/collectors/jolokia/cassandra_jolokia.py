# coding=utf-8

"""
Collects Cassandra JMX metrics from the Jolokia Agent.  Extends the
JolokiaCollector to interpret Histogram beans with information about the
distribution of request latencies.

#### Example Configuration
CassandraJolokiaCollector uses a regular expression to determine which
attributes represent histograms. This regex can be overridden by providing a
`histogram_regex` in your configuration.  You can also override `percentiles` to
collect specific percentiles from the histogram statistics.  The format is shown
below with the default values.

CassandraJolokiaCollector.conf

```
    percentiles '50,95,99'
    histogram_regex '.*HistogramMicros$'
```
"""

import math
import string
import re

import diamond.collector
import json
import re
import urllib
import urllib2


class CassandraJolokiaCollector(diamond.collector.Collector):

    LIST_URL = "/list"
    READ_URL = "/?ignoreErrors=true&p=read/%s:*"

    """
    These domains contain MBeans that are for management purposes,
    or otherwise do not contain useful metrics
    """
    IGNORE_DOMAINS = ['JMImplementation', 'jmx4perl', 'jolokia',
                      'com.sun.management', 'java.util.logging']

    def get_default_config_help(self):
        config_help = super(CassandraJolokiaCollector,
                            self).get_default_config_help()
        config_help.update({
            'mbeans':  "Pipe delimited list of MBeans for which to collect"
                       " stats. If not provided, all stats will"
                       " be collected.",
            'regex': "Contols if mbeans option matches with regex,"
                       " False by default.",
            'host': 'Hostname',
            'port': 'Port',
            'prefix': 'Prefix for metrics',
            'rewrite': "This sub-section of the config contains pairs of"
                       " from-to regex rewrites.",
            'path': 'Path to jolokia.  typically "jmx" or "jolokia"'
        })
        return config_help

    def get_default_config(self):
        config = super(CassandraJolokiaCollector, self).get_default_config()
        config.update({
            'mbeans': [],
            'regex': False,
            'rewrite': [],
            'path': 'jolokia',
            'host': 'localhost',
            'prefix': 'jktest',
            'port': 8778,
        })
        return config

    def __init__(self, *args, **kwargs):
        super(CassandraJolokiaCollector, self).__init__(*args, **kwargs)
        self.mbeans = []
        self.rewrite = {}
        if isinstance(self.config['mbeans'], basestring):
            for mbean in self.config['mbeans'].split('|'):
                self.mbeans.append(mbean.strip())
        elif isinstance(self.config['mbeans'], list):
            self.mbeans = self.config['mbeans']
        if isinstance(self.config['rewrite'], dict):
            self.rewrite = self.config['rewrite']

    def check_mbean(self, mbean):
        if not self.mbeans:
            return True
        mbeanfix = self.clean_up(mbean)
        if self.config['regex'] is not None:
            for chkbean in self.mbeans:
                if re.match(chkbean, mbean) is not None or \
                   re.match(chkbean, mbeanfix) is not None:
                    return True
        else:
            if mbean in self.mbeans or mbeanfix in self.mbeans:
                return True

    def collect(self):
        listing = self.list_request()
        try:
            domains = listing['value'] if listing['status'] == 200 else {}
            for domain in domains.keys():
                if domain not in self.IGNORE_DOMAINS:
                    self.emit_domain_metrics(domain)
        except KeyError:
            # The reponse was totally empty, or not an expected format
            self.log.error('Unable to retrieve MBean listing.')

    def emit_domain_metrics(self, domain):
        try:
            obj = self.read_request(domain)
            mbeans = obj['value'] if obj['status'] == 200 else {}
            for k, v in mbeans.iteritems():
                if self.check_mbean(k):
                    self.collect_bean(k, v)
        except KeyError:
            self.log.error("Unable to retrieve MBean listing")

    def read_json(self, request):
        json_str = request.read()
        return json.loads(json_str)

    def list_request(self):
        try:
            url = "http://%s:%s/%s%s" % (self.config['host'],
                                         self.config['port'],
                                         self.config['path'],
                                         self.LIST_URL)
            response = urllib2.urlopen(url)
            return self.read_json(response)
        except (urllib2.HTTPError, ValueError):
            self.log.error('Unable to read JSON response.')
            return {}

    def read_request(self, domain):
        try:
            url_path = self.READ_URL % self.escape_domain(domain)
            url = "http://%s:%s/%s%s" % (self.config['host'],
                                         self.config['port'],
                                         self.config['path'],
                                         url_path)
            response = urllib2.urlopen(url)
            return self.read_json(response)
        except (urllib2.HTTPError, ValueError):
            self.log.error('Unable to read JSON response.')
            return {}

    # escape the JMX domain per https://jolokia.org/reference/html/protocol.html
    # the Jolokia documentation suggests that, when using the p query parameter,
    # simply urlencoding should be sufficient, but in practice, the '!' appears
    # necessary (and not harmful)
    def escape_domain(self, domain):
        domain = re.sub('!', '!!', domain)
        domain = re.sub('/', '!/', domain)
        domain = re.sub('"', '!"', domain)
        domain = urllib.quote(domain)
        return domain

    def clean_up(self, text):
        text = re.sub('["\'(){}<>\[\]]', '', text)
        text = re.sub('[:,.]+', '.', text)
        text = re.sub('[^a-zA-Z0-9_.+-]+', '_', text)
        for (oldstr, newstr) in self.rewrite.items():
            text = re.sub(oldstr, newstr, text)
        return text

    def collect_bean(self, prefix, obj):
        for k, v in obj.iteritems():
            if type(v) in [int, float, long]:
                self.publish_bean_metric(prefix, k, v)


    def publish_bean_metric(self, metric_key, value_key, value):
        metric_name_prefix, y = metric_key.split(':', 2)
        d_dict = self.make_dimension(y)
        metric_name = d_dict.pop("name")
        self.dimensions = d_dict
        full_metric_name = '.'.join([self.config['prefix'], metric_name_prefix, metric_name])
        if value_key.lower() == 'count':
            self.publish_cumulative_counter(full_metric_name, value)

        else:
            self.publish(full_metric_name, value)


    def make_dimension(self, dimension_string):
        result = {}
        for d in dimension_string.split(','):
            k, v = d.split('=')
            result[k] = v
        return result

    # There's no unambiguous way to interpret list values, so
    # this hook lets subclasses handle them.
    def interpret_bean_with_list(self, prefix, values):
        pass
