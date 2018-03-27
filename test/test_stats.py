import logging
import unittest

import easy_atom.helpers as helpers
import pandas

"""
df=pandas.read_csv(tfile, delimiter=";", names=KEYS, header=0, index_col=False)
df.loc(df.identifiant_pp==10005831911)
mss_providers=df.adresse_bal_mssante.str.split('@',expand=True).get(1)

mss_providers.where(mss_providers=='ch-larochelle.mssante.fr').dropna().count()
mss_providers.where(mss_providers.str.contains('ch-aix')).dropna()

"""

KEYS = ["type_identifiant_pp", "identifiant_pp", "identification_nationale_pp",
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


class TestStats(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger('utest')

    @staticmethod
    def parse_csv(filename):
        data = []
        nline = 1
        with open(filename, "r") as fin:
            for line in fin.readlines():
                vals = line.replace('"', '').split(';')
                if nline > 1:
                    data.append(dict(zip(KEYS, vals[:-1])))
                nline = nline + 1
        return data

    def test_count(self):
        df = pandas.read_csv("files/data695941.csv", delimiter=';', names=KEYS, header=0, index_col=False)

        self.logger.info("nunique : %d" % df['identifiant_pp'].nunique())
        self.logger.info("MSSante : %d" % df['adresse_bal_mssante'].nunique())

        mss_providers = df.adresse_bal_mssante.str.split('@', expand=True).get(1)

        self.logger.info('Nb MSS : %d' % len(mss_providers.unique()))
        self.logger.info('Nb by provider : %d' % len(mss_providers.value_counts()))

        self.assertEqual(len(df), 695940)
        self.assertEqual(567830, df['identifiant_pp'].nunique())
        self.assertEqual(77193, df['adresse_bal_mssante'].nunique())


if __name__ == '__main__':
    loggers = helpers.stdout_logger(['utest'], logging.INFO)

    unittest.main()
