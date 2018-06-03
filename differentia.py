#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    Compare 2 digest files (hash, line) and find differences

"""
__author__ = 'Frederic Laurent'
__version__ = "1.0"
__copyright__ = 'Copyright 2017, Frederic Laurent'
__license__ = "MIT"

import logging
import os.path
import argparse
from collections import namedtuple

import digester
from easy_atom import helpers


class Diff:
    def __init__(self, properties=None):
        self.logger = logging.getLogger('differentia')
        self.properties = properties
        self.data = []

    def find_diff(self, filename):
        """
            Find delta between the new filename (arg) and the previous

        :param filename: new data filename
        :return: list of differences
        """

        old_d = digester.Digester()
        new_d = digester.Digester()
        try:
            old_data_fn = self.properties.local.data_filename
        except AttributeError:
            old_data_fn = None

        self.logger.info("Diff old = %s / new = %s" % (old_data_fn, filename))
        self.logger.info("Diff old = %s " % (os.path.abspath(old_data_fn)))
        self.logger.info("new      = %s " % (os.path.abspath(filename)))

        old_sha_data = old_d.digest(old_data_fn)
        new_sha_data = new_d.digest(filename)

        diff_data = []

        for (sha, linenum) in new_sha_data.items():
            if sha not in old_sha_data:
                diff_data.append((sha, linenum + 1, new_d.data[linenum]))
        return diff_data

    def save_diff(self, difflist, filename):
        """
            Save differences into a file
        :param difflist: list of differences
        :param filename: file name containing diff
        :return:
        """
        self.logger.info('Differences >>> %d' % len(difflist))

        try:
            storage_dir = self.properties.local.storage_dir
        except AttributeError:
            storage_dir = ""

        with open(os.path.join(storage_dir, filename), 'w') as fout:
            for s, l, t in difflist:
                fout.write('%s\n' % l)

if __name__ == "__main__":
    loggers = helpers.stdout_logger([ 'differentia', 'digester'], logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("previous", help="previous data file")
    parser.add_argument("next", help="next data file")
    parser.add_argument("output", help="output file name")
    args = parser.parse_args()

    DataFile=namedtuple('DataFile', 'data_filename')
    Properties=namedtuple('Properties','local')

    diff=Diff(properties=Properties(DataFile(data_filename=args.previous)))
    data = diff.find_diff(args.next)
    diff.save_diff(data, args.output)
    
