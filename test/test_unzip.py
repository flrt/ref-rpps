import datetime
import logging
import os.path
import unittest
import zipfile

from easy_atom import helpers


class TestUnzip(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger('utest')
        self.zipfilename = "ExtractionMonoTable_CAT18_ToutePopulation_201802031143.zip"

    def test_unzip(self):
        start_date = datetime.datetime.now()

        zf = zipfile.ZipFile(os.path.join('files', self.zipfilename))
        zf.extractall('files')
        end_date = datetime.datetime.now()

        delta = end_date - start_date
        self.logger.info("Start  : %s" % start_date.isoformat())
        self.logger.info("End    : %s" % end_date.isoformat())
        self.logger.info("Delta  : %s" % delta)

        self.assertTrue(os.path.exists(os.path.join('files',
                                                    'ExtractionMonoTable_CAT18_ToutePopulation_201802030936.csv')))


if __name__ == '__main__':
    loggers = helpers.stdout_logger(['utest'], logging.INFO)

    unittest.main()
