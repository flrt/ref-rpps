import datetime
import logging
import unittest

from easy_atom import helpers

import digester


class TestDigest(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger('utest')

    def test_time(self):
        d = digester.Digester()
        start_date = datetime.datetime.now()
        data = d.make_digest("files/data695941.csv")
        end_date = datetime.datetime.now()

        delta = end_date - start_date
        self.logger.info("Start : %s" % start_date.isoformat())
        self.logger.info("End   : %s" % end_date.isoformat())

        self.logger.info('Data : %s' % (delta.total_seconds()))

        self.assertEqual(len(data), 695941)
        self.assertLess(int(delta.total_seconds()), 10)


if __name__ == '__main__':
    loggers = helpers.stdout_logger(['diff', 'digester', 'utest'], logging.INFO)

    unittest.main()
