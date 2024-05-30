import logging

import pandas


logger = logging.getLogger(__name__)


GEONAMES_REMOTE_URL = "https://download.geonames.org/export/dump/cities15000.zip"


def load_geonames(url: str = GEONAMES_REMOTE_URL) -> pandas.DataFrame:
    fields = [
        "city_id",
        "city_name",
        "city_asciiname",
        "city_alternatenames",
        "latitude",
        "longitude",
        "feature_class",
        "feature_code",
        "country_a2",
        "cc2",
        "admin1",
        "admin2",
        "admin3",
        "admin4",
        "population",
        "elevation",
        "dem",
        "timezone",
        "modification_date",
    ]
    return pandas.read_csv(url, sep="\t", names=fields)
