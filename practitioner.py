#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    Analyze RPPS file

"""
__author__ = 'Frederic Laurent'
__version__ = "1.0"
__copyright__ = 'Copyright 2017, Frederic Laurent'
__license__ = "MIT"

import datetime
import io
import logging
import os.path
import re
import tempfile
import zipfile
from datetime import datetime as dt

import pandas
import requests
import urllib3
from bs4 import BeautifulSoup
from easy_atom import atom
from easy_atom import content
from easy_atom import helpers


class RPPS:
    """
        Handle RPPS Data
    """
    PEM = "cert/rpps.pem"
    PUBLIC_URL = 'https://annuaire.sante.fr/web/site-pro/extractions-publiques'
    KEYS_CAT18 = ["type_identifiant_pp", "identifiant_pp", "identification_nationale_pp",
                  "code_civilite_exercice", "libelle_civilite_exercice", "nom_exercice",
                  "prenom_exercice", "code_profession", "libelle_profession",
                  "code_categorie_professionnelle", "libelle_categorie_professionnelle",
                  "code_savoir_faire", "libelle_savoir_faire", "code_type_savoir_faire",
                  "libelle_type_savoir_faire", "numero_siret_site", "numero_siren_site",
                  "numero_finess_site", "numero_finess_etablissement_juridique",
                  "raison_sociale_site", "enseigne_commerciale_site", "identifiant_structure",
                  "complement_destinataire", "complement_point_geographique",
                  "numero_voie", "indice_repetition_voie", "code_type_de_voie", "libelle_type_de_voie",
                  "libelle_voie", "mention_distribution", "bureau_cedex", "code_postal",
                  "code_commune", "libelle_commune", "code_pays", "libelle_pays",
                  "telephone", "telephone_2", "telecopie", "adresse_e-mail",
                  "adresse_bal_mssante"
                  ]

    def __init__(self, properties):
        self.logger = logging.getLogger('downloader')

        self.previous = None
        self.local_storage = tempfile.gettempdir()
        self.tracks = {}
        self.pub_url = ''
        self.data_files = []
        self.last_check_date = None
        self.re_rpps_fn = re.compile(
            '.*?(ExtractionMonoTable_CAT18_ToutePopulation_(\d+).(zip|csv))')
        self.data_date = dt.now().strftime('%Y-%m-%d')

        self.init_properties(properties)

    def init_properties(self, properties):
        """
            properties bootstrap

        :param properties: properties object
        :return: -
        """
        try:
            self.previous = properties.local.data_filename
        except AttributeError as ae:
            self.logger.warning(
                '[config] No/Wrong local.data_filename properties : %s' % ae)

        try:
            self.last_check_date = properties.local.last_check
        except AttributeError as ae:
            self.logger.warning(
                '[config] No/Wrong local.last_check properties : %s' % ae)

        if not self.last_check_date and self.previous:
            _, self.last_check_date = self.extract_data_filename(self.previous)

        try:
            self.local_storage = properties.local.storage
        except AttributeError as ae:
            self.logger.warning(
                '[config] No/Wrong local.storage properties : %s' % ae)
            self.logger.info('Using temp dir : %s' % self.local_storage)

        try:
            self.tracks = properties.tracks
        except AttributeError as ae:
            self.logger.warning(
                '[config] No/Wrong tracks properties : %s' % ae)

        try:
            self.pub_url = properties.pub.url_base
            if not self.pub_url.endswith('/'):
                self.pub_url += '/'
        except AttributeError as ae:
            self.logger.warning(
                '[config] No/Wrong pub.url_base properties : %s' % ae)

    def find_link(self):
        """
            Get the HTML Web Page. Find URL to the zip file to download

            HTTPS web site. cert are stored in RPPS.PEM

        :return:
            full_url : Full URL of the zip file
            remote_fn : remote zip filename
            data_date : file date (in the filename)
        """
        pem_filename = os.path.abspath(RPPS.PEM)
        self.logger.info("Use pem file = %s" % pem_filename)

        rpps_index = requests.get(RPPS.PUBLIC_URL, verify=pem_filename)

        soup = BeautifulSoup(rpps_index.text, "html5lib")
        links = list(filter(lambda x: x.get('href'), soup.find_all('a')))
        zip_list = list(filter(lambda x: x.get(
            'href').endswith('.zip'), links))

        for z in zip_list:
            remote_fn, data_date = self.extract_data_filename(z.get('href'))
            if remote_fn:
                return z.get('href'), remote_fn, data_date

        return None, None, None

    def extract_data_filename(self, url):
        """
            Extract data, given the URL
            blabla/ExtractionMonoTable_CAT18_ToutePopulation_201802071124.zip"

        :param url: URL of the file
        :return:
            remote file name
            data date (ex: 201802071124)
        """
        self.logger.debug("Extract data from %s" % url)
        re_eval = self.re_rpps_fn.match(url)
        if re_eval:
            remote_fn = re_eval.group(1)
            data_date = re_eval.group(2)
            return remote_fn, data_date

        return None, None

    def retrieve_current(self):
        full_url, remote_fn, data_date = self.find_link()
        self.logger.debug("URL=%s, remote=%s, date=%s" % (full_url, remote_fn, data_date))

        #prev_fn, prev_date = self.extract_data_filename(self.previous)
        #self.logger.debug("prev URL = %s, fn = %s, date = %s" % (self.previous, prev_fn, prev_date))

        self.logger.debug("prev date = %s"%self.last_check_date)
        _fmt = '%Y%m%d%H%M'
        delta = dt.strptime(data_date, _fmt) - dt.strptime(self.last_check_date, _fmt)

        if delta.total_seconds() > 0:
            self.logger.info("Newer file available -> download")
            # download zip
            return self.download_zip(self.local_storage, remote_fn, full_url)

        return None

    def download_zip(self, download_dir, remote_zip, url):
        """
            Download data file (zip format)
            Extract data into storage directory

        :param download_dir: storage directory
        :param remote_zip: remote zip filename
        :param url: Full URL of the file
        :return: absolute name of CSV file
        """
        self.logger.info("Download zip: %s" % remote_zip)
        self.logger.info("Extract to : %s" % download_dir)

        if download_dir and not os.path.exists(download_dir):
            os.makedirs(download_dir)

        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',
                                   ca_certs=os.path.abspath(RPPS.PEM))

        req = http.request("GET", url)
        zipf = zipfile.ZipFile(io.BytesIO(req.data))
        testzip = zipf.testzip()
        if testzip:
            self.logger.error("Zip ERROR : %s" % testzip)
            return None
        else:
            zipf.extractall(download_dir)

        if len(zipf.namelist()) > 1:
            self.logger.error("More than 1 file in zip : Unexpected !")
            [self.logger.error("  %s" % f) for f in zipf.namelist()]
            return None
        else:
            # save the date of the last download
            _, self.last_check_date = self.extract_data_filename(remote_zip)
            return os.path.join(download_dir, zipf.namelist()[0])

    def save_diff_files(self, data_filename, diff_filename, difflist):
        """
            Save files

        :param data_filename: file containing hash, line number, values
        :param diff_filename: file containing line numbers of modified data
        :param difflist: list of differences
        :return: -
        """
        if data_filename:
            with open(data_filename, "w") as fdata:
                for _hash, num, data in difflist:
                    fdata.write("{}\n".format(data))

            self.data_files.append(data_filename)

        if diff_filename:
            # save indexes
            with open(diff_filename, "w") as findex:
                index_list = list(map(lambda d: str(d[1]), difflist))
                findex.write("{}\n".format(','.join(index_list)))

            self.data_files.append(diff_filename)

    def feed_info(self, data_tracks):
        """
        Get Information from data to populate feed
        
        :param data_tracks: data about configured tracks to follow

        """

        root = content.xmlelt(None, "div")
        content.xmlelt(root, "h1", "Modifications")

        for data in data_tracks:
            # Produce or Not RSS section
            if data['rss']:
                content.xmlelt(root, "h2", data["title"])
                ul = content.xmlelt(root, "ul")
                for val in data["values"]:
                    self.logger.debug(val)
                    content.xmlelt(
                        ul, "li", "{} : {}".format(val["key"], val["val"]))

        info = dict(html=content.xml2text(root, atom.Feed.FEED_ENCODING, xml_decl=False),
                    files=list(map(lambda x: self.pub_url +
                                             os.path.basename(x), self.data_files)),
                    title="Mise à jour RPPS du {}".format(self.data_date),
                    id="rpps{}".format(self.data_date),
                    date=dt.now(datetime.timezone.utc).isoformat(sep='T'),
                    summary="Informations sur la publication RPPS du {}".format(self.data_date))
        return info

    def extract_data(self, data_filename, difflist):
        """
            Read raws data file
            Produce data according to config in tracks section
                - title
                - list of ordered key:value wih date
            For each block
                - produce RSS section if configured
                - produce json raw data if filename is provided

        :param data_filename:
        :param difflist: list of difference

        :return:
        """
        self.logger.debug(
            "Extract data : config={}".format(str(self.tracks)))

        # Read data
        df = pandas.read_csv(data_filename,
                             delimiter=';',
                             names=RPPS.KEYS_CAT18, header=0, index_col=False)

        mss_providers = df.adresse_bal_mssante.str.split(
            '@', expand=True).get(1)

        glob_history_flag = 'stats' in self.tracks._fields and 'save_history' in self.tracks.stats._fields and self.tracks.stats.save_history

        global_infos = {"title": u"Informations générales", "type": "stats",
                        "date": self.data_date,
                        "values": [
                            {"key": "Nombre de lignes", "val": len(df)},
                            {"key": "Nombre de lignes modifiées", "val": len(difflist)},
                            {"key": "Nombre de RPPS", "val": df.identifiant_pp.nunique()},
                            {"key": "Mails MSsante",
                             "val": df.adresse_bal_mssante.nunique()},
                            {"key": "Domaines MSsante", "val": mss_providers.nunique()}
                        ],
                        "rss": True,
                        "history_flag": glob_history_flag,
                        "filename": None
                        }
        if 'stats' in self.tracks._fields and 'filename' in self.tracks.stats._fields:
            global_infos["filename"] = self.tracks.stats.filename

        extracted = [global_infos]

        # for each block
        for block in self.tracks.mssante:
            self.logger.info("Block {}".format(block.name))

            result = {"title": block.name, "type": "mssante", "values": [], "date": self.data_date,
                      "rss": 'rss' in block._fields and block.rss,
                      "history_flag": 'save_history' in block._fields and block.save_history,
                      "filename": None}

            if 'top' in block._fields:
                # generate TOP x
                mss_top = mss_providers.value_counts().head(block.top).to_dict()
                for (k, v) in mss_top.items():
                    result["values"].append(dict(key=k, val=v))

            if 'domains' in block._fields:
                # generate domain section
                if 'title' in block.domains:
                    result["title"] = block.domains.title

                for domain in block.domains.value:
                    _count = mss_providers.where(
                        mss_providers == domain).dropna().count()
                    result["values"].append(dict(key=domain, val=int(_count)))

            if "filename" in block._fields:
                result["filename"] = block.filename
            extracted.append(result)
        return extracted

    def save_tracks(self, data_tracks):
        """
            Save data in filename, if a filename is configured
        :param data_tracks: data to save
        :return: -
        """
        self.logger.info("Save %d tracks" % len(data_tracks))
        for data in data_tracks:
            self.logger.debug("##> %s" % data)
            # Save data if needed
            if data["filename"]:
                self.save_tracks_set(
                    os.path.join(self.local_storage, data["filename"]),
                    data, data['history_flag'])

    def save_tracks_set(self, filename, data, history_flag):
        """
            Save data set in filename

        :param filename: filename of the data
        :param data: data to save
        :param history_flag: handle history or not. History means a list of data sets
        :return: -
        """
        self.logger.info("Save tracks set : type=%s, file=%s, history=%s" % (data['type'], filename, history_flag))
        tracks = helpers.load_json(filename)

        if 'type' not in data:
            self.logger.warning("No data TYPE, nothing saved...")
            return

        # dtype = data type, e.g. stats, mssante, etc.
        dtype = data["type"]

        if dtype not in tracks:
            # create new set
            tracks[dtype] = {}

        for val in data["values"]:
            if val["key"] not in tracks[dtype]:
                tracks[dtype][val["key"]] = None

            # value to add or remplace (v)
            v = dict(date=data["date"], value=val["val"])

            if history_flag:
                # Manage a list of values : value = {'date': '2018-03-23', 'value': 999}
                # insert/set/remplace the new value v
                if isinstance(tracks[dtype][val["key"]], list):
                    # already a list
                    # search if these date already exists. If so remplace, else add
                    existing_index = -1
                    for index, elem in enumerate(tracks[dtype][val["key"]]):
                        if elem["date"] == v["date"]:
                            existing_index = index

                    if existing_index > -1:
                        tracks[dtype][val["key"]][existing_index]["value"] = v["value"]
                    else:
                        tracks[dtype][val["key"]].append(v)
                else:
                    # new list (replace single structure)
                    tracks[dtype][val["key"]] = [v]
            else:
                # replace values
                tracks[dtype][val["key"]] = v

        helpers.save_json(filename, tracks)
