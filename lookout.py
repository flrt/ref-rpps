#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

    Main App
    Detects if a newer file is available, process it

"""
__author__ = 'Frederic Laurent'
__version__ = "1.0"
__copyright__ = 'Copyright 2018, Frederic Laurent'
__license__ = "MIT"

import argparse
import logging
import os.path
import sys
from datetime import datetime as dt

from easy_atom import action
from easy_atom import atom
from easy_atom import helpers

import differentia
import digester
import practitioner


class App:
    """
        Main App
    """

    def __init__(self, config_filename):
        """
            Init class, loads config file
            :param config_filename: configration file name
        """

        self.logger = logging.getLogger('app')
        self.logger.info("Config : %s" % config_filename)

        self.config_filename = config_filename
        self.properties = helpers.json_to_object(config_filename)

        self.diff_data_filename = None
        self.diff_index_filename = None
        self.rss_filename = None
        self.stats_filename = None

        self.diff = differentia.Diff(self.properties)
        self.digest = digester.Digester()
        self.feed = atom.Feed('rpps', self.properties.pub.feed_base)
        self.feed.load_config()

        self.rpps_data = practitioner.RPPS(properties=self.properties)

        self.init_properties()

    def init_properties(self):
        try:
            if self.properties.local.save_diff_data:
                self.diff_data_filename = os.path.join(self.properties.local.storage,
                                                       self.properties.local.data_filename_mask.format(
                                                           self.rpps_data.data_date))
        except AttributeError as attr_err:
            self.logger.info("[config] Don't save diff data (%s)" % attr_err)

        try:
            if self.properties.local.save_diff_index:
                self.diff_index_filename = os.path.join(
                    self.properties.local.storage,
                    self.properties.local.index_filename_mask.format(self.rpps_data.data_date))
        except AttributeError as attr_err:
            self.logger.info("[config] Don't save diff index (%s)" % attr_err)

        try:
            self.rss_filename = os.path.join(self.properties.local.storage,
                                             self.properties.local.rss_updates_filename)
        except AttributeError as attr_err:
            self.logger.info("[config] Don't produce RSS (%s)" % attr_err)

        try:
            self.stats_filename = os.path.join(self.properties.local.storage,
                                               self.properties.tracks.stats.filename)
        except AttributeError as attr_err:
            self.logger.info("[config] Don't produce Stats (%s)" % attr_err)

    def process(self, csvfile=None):
        """
            Download newer file if available
            Process data
        """

        self.logger.debug("Start...")
        try:
            if csvfile:
                if not os.path.exists(csvfile):
                    self.logger.error("File {} not found !".format(csvfile))
                    sys.exit(1)

                # use arg filename
                _, data_date = self.rpps_data.extract_data_filename(csvfile)
                self.rpps_data.data_date = dt.strptime(data_date, '%Y%m%d%H%M').strftime("%Y-%m-%d")
                self.rpps_data.last_check_date = data_date
                self.init_properties()
                new_data_file = csvfile
            else:
                # download new file if available
                new_data_file = self.rpps_data.retrieve_current()

            self.logger.debug("new data file : %s" % new_data_file)

            # New data to compute
            if new_data_file:
                # find diff
                diff_list = self.diff.find_diff(new_data_file)
                data_tracks = self.rpps_data.extract_data(new_data_file, diff_list)

                self.rpps_data.save_diff_files(self.diff_data_filename,
                                               self.diff_index_filename,
                                               diff_list)

                self.rpps_data.save_tracks(data_tracks)

                # update RSS
                if self.rss_filename:
                    self.logger.debug(
                        "Loading RSS data feed : {}".format(self.rss_filename))
                    rpps_updates = helpers.load_json(self.rss_filename)
                    if "updates" not in rpps_updates:
                        rpps_updates = {"updates": []}

                    self.logger.info(rpps_updates)
                    d = self.rpps_data.feed_info(data_tracks)
                    rpps_updates["updates"].insert(0, d)
                    helpers.save_json(self.rss_filename, rpps_updates)

                    # produce RSS/atom file
                    result = self.feed.generate(rpps_updates["updates"])
                    self.feed.save(result)
                    self.feed.rss2()

                self.logger.debug("Last Check date : %s" % self.rpps_data.last_check_date)
                # save the new config fie
                new_local_section = self.properties.local._replace(
                    data_filename=new_data_file,
                    last_check=self.rpps_data.last_check_date)
                new_props = self.properties._replace(local=new_local_section)
                helpers.object_to_json(new_props, self.config_filename)

            else:
                self.logger.info("No newer file (post %s)" % self.rpps_data.last_check_date)

        except AttributeError as attr_err:
            self.logger.info(
                "[config] Wrong/No data file name : {}".format(attr_err))

    def statistics(self, csvfile):
        """
            Give some statistics (via log)
        :param csvfile: CSV file containing data
        :return: -
        """
        data_tracks = self.rpps_data.extract_data(csvfile)

        for data in data_tracks:
            self.logger.debug("######## DATA %s  ########" % data["type"])
            self.logger.debug(" filename > {} - history {}".format(data["filename"], data['history_flag']))

            maxlen = max(list(map(lambda x: len(x["key"]), data["values"])))

            for val in data["values"]:
                # self.logger.info(f'  {val["key"]:{"<"}{maxlen}} = {val["val"]}')
                self.logger.info('  {mykey:{"<"}{width}} = {myvalue}'.format(mykey=val["key"],
                                                                             myvalue=val["val"],
                                                                             width=maxlen))


    def upload_data(self, config):
        """
            Upload data files, according to parameters in config

            :param config: Configuration used to upload data on a server
        """

        self.logger.info("Upload data updates : %s" %
                         self.rpps_data.data_files)
        act = action.UploadAction(conf_filename=config)
        act.process(self.rpps_data.data_files)

    def upload_feed(self, config):
        """
            Upload Feed file

            :param config: Configuration used to upload feed on a server
        """
        self.logger.info("Upload feeds updates")
        act = action.UploadAction(conf_filename=config)
        act.process([self.feed.feed_filename, self.feed.rss2_filename])


def main():
    """
        Main : process arguments and start App
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="fichier de parametres")
    parser.add_argument(
        "--feedftp", help="configuration FTP pour upload du flux ATOM, format JSON")
    parser.add_argument(
        "--dataftp", help="configuration FTP pour upload des données, format JSON")
    parser.add_argument("--csv", help="Fichier CSV contenant l'extraction RPPS")
    parser.add_argument("--stat", help="Affiche les stats", action="store_true")

    args = parser.parse_args()
    if args.config:
        app = App(args.config)

        if args.stat:
            app.statistics(args.csv)

        else:
            app.process(args.csv)

            if args.dataftp:
                app.upload_data(args.dataftp)
            if args.feedftp:
                app.upload_feed(args.feedftp)
    else:
        sys.stdout.write("/!\\ Aucune action définie !\n\n")
        parser.print_help(sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    loggers = helpers.stdout_logger(
        ['downloader', 'differentia', 'app', 'digester'], logging.INFO)
    main()
