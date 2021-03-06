#!/usr/bin/python
# coding=utf-8

from mock import patch
from mock import Mock

from test import unittest
import configobj
import logging

from diamond.collector import Collector
from diamond.error import DiamondException


class BaseCollectorTest(unittest.TestCase):

    def tearDown(self):
        log = logging.getLogger("diamond.Collector")
        log.removeHandler(log.handlers[0])
        # Ensure that we aren't printing log messages to stdout in unit tests
        log.propagate = False

    def config_object(self):
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        return config

    @patch('diamond.collector.Collector.publish_metric', autoSpec=True)
    def test_SetDimensions(self, mock_publish):
        c = Collector(self.config_object(), [])
        dimensions = {
            'dim1': 'alice',
            'dim2': 'chains',
        }
        c.dimensions = dimensions
        c.publish('metric1', 1)

        for call in mock_publish.mock_calls:
            name, args, kwargs = call
            metric = args[0]
            self.assertEquals(metric.dimensions, dimensions)
        self.assertEqual(c.dimensions, None)

    @patch('diamond.collector.Collector.publish_metric', autoSpec=True)
    def test_successful_error_metric(self, mock_publish):
        c = Collector(self.config_object(), [])
        mock_socket = Mock()
        c._socket = mock_socket
        with patch.object(c, 'log'):
            try:
                c.publish('metric', "bar")
            except DiamondException:
                pass
        for call in mock_publish.mock_calls:
            name, args, kwargs = call
            metric = args[0]
            self.assertEqual(metric.path, "servers.Collector.fullerite.collector_errors")

    @patch('diamond.collector.Collector.publish_metric', autoSpec=True)
    def test_failed_error_metric_publish(self, mock_publish):
        c = Collector(self.config_object(), [])
        self.assertFalse(c.can_publish_metric())
        with patch.object(c, 'log'):
            try:
                c.publish('metric', "baz")
            except DiamondException:
                pass
        self.assertEquals(len(mock_publish.mock_calls), 0)

    def test_can_publish_metric(self):
        c = Collector(self.config_object(), [])
        self.assertFalse(c.can_publish_metric())

        c._socket = "socket"
        self.assertTrue(c.can_publish_metric())
