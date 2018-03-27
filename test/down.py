import logging
import unittest

import practitioner
from easy_atom import helpers
import json
from collections import namedtuple


class TestDownload(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger('utest')
        config_dict = {"local": {
            "data_filename": "test/files/ExtractionMonoTable_CAT18_ToutePopulation_201802030936.csv",
            "storage": "test/files"
        }}
        self.config_1 = json.loads(config_dict,
                                   object_hook=lambda d: namedtuple('JDATA', d.keys())(*d.values()))

        config_dict["local"]["data_filename"] = 'test/files/ExtractionMonoTable_CAT18_ToutePopulation_202801010936.csv'
        self.config_2 = json.loads(config_dict,
                                   object_hook=lambda d: namedtuple('JDATA', d.keys())(*d.values()))

    def test_download(self):
        self.logger.info("Test - test_download")
        d = practitioner.RPPS(properties=self.config_1)
        newfilename = d.retrieve_current()
        self.assertIsNotNone(newfilename)

    def test_no_download(self):
        self.logger.info("Test - test_no_download")

        d = practitioner.RPPS(properties=self.config_2)

        newfilename = d.retrieve_current()
        self.assertIsNone(newfilename)


if __name__ == '__main__':
    loggers = helpers.stdout_logger(['utest', 'downloader'], logging.DEBUG)

    unittest.main()
