#!/usr/bin/python
# coding=utf-8
################################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from mock import Mock
from mock import patch

from diamond.collector import Collector

from cassandra_jolokia import CassandraJolokiaCollector

################################################################################


class TestCassandraJolokiaCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('CassandraJolokiaCollector', {})

        self.collector = CassandraJolokiaCollector(config, None)

    def test_import(self):
        self.assertTrue(CassandraJolokiaCollector)

    @patch.object(Collector, 'publish')
    def test_should_create_dimension(self, publish_mock):
        def se(url):
            return self.getFixture("yelp_report.json")

        patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))

        patch_urlopen.start()
        self.collector.emit_domain_metrics("com.yelp")
        patch_urlopen.stop()



################################################################################
if __name__ == "__main__":
    unittest.main()
