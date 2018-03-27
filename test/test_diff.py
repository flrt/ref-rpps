import logging
import unittest

import differentia
from easy_atom import helpers
import os.path


class TestDiff(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger('utest')

    def test_equal_data(self):
        self.logger.info("  TEST test_equal_data")

        cfg = helpers.json_to_object("files/test-config.json")
        d = differentia.Diff(cfg)
        result = d.find_diff(os.path.abspath("files/rpps_ext.csv"))

        self.assertEqual(len(result), 0)

    def test_1diff_data(self):
        self.logger.info("  TEST test_1diff_data")
        cfg = helpers.json_to_object("files/test-config.json")

        d = differentia.Diff(cfg)
        result = d.find_diff("files/rpps_ext_1.csv")

        self.logger.info(result)
        self.assertEqual(len(result), 1)


if __name__ == '__main__':
    loggers = helpers.stdout_logger(['utest', 'digester',
                                     'differentia'], logging.INFO)

    unittest.main()
