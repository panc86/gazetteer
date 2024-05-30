import logging
import os

import geopandas
from geopandas import GeoDataFrame


logger = logging.getLogger(__name__)


GADM_REMOTE_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/gadm_410-gdb.zip"


def read_shapefile(filepath: str) -> geopandas.GeoDataFrame:
    logger.debug("loading geometries from {}".format(filepath))
    return geopandas.read_file(filepath)


def build_gadm_filepath(url: str = GADM_REMOTE_URL) -> str:
    return f"zip+{url}!/{os.path.basename(url).replace('-gdb.zip', '.gdb')}"


def remove_regions(df: GeoDataFrame) -> GeoDataFrame:
    logger.debug("remove unused GADM regions")
    to_drop = ["Antarctica", "Caspian Sea"]
    return df.loc[~df.NAME_0.isin(to_drop), :].copy()


def update_unknown_region_meta(df: GeoDataFrame) -> GeoDataFrame:
    logger.debug("update unknown region meta")
    # Ukraine
    df.loc[
        (df.NAME_1 == "?").idxmax(), ["NAME_1", "NL_NAME_1", "HASC_1", "ENGTYPE_1"]
    ] = ["Kiev City", "Київ", "UA.KC", "Independent City"]
    return df


def fill_missing_region_names(df: GeoDataFrame) -> GeoDataFrame:
    logger.debug("fill missing GADM region names with country name")
    regionless = df.NAME_1.isna()
    df.loc[regionless, "NAME_1"] = df.loc[regionless, "NAME_0"]
    return df


def rename_region_name_attributes(df: GeoDataFrame) -> GeoDataFrame:
    logger.debug("rename wrong GADM region name attributes")
    mapping = {
        "Apulia": "Puglia",
        "Sicily": "Sicilia",
    }
    df.NAME_1 = df.NAME_1.replace(mapping)
    return df


def prepare_regions(regions: GeoDataFrame) -> GeoDataFrame:
    return GeoDataFrame(
        regions.pipe(remove_regions)  # remove inhabitated polygons e.g. Antartica
        .pipe(update_unknown_region_meta)  # e.g. Ukraine has missing data
        .pipe(fill_missing_region_names)  # small countries are regionless
        .pipe(rename_region_name_attributes)  # rename regions using local knownledge
        .reset_index(drop=True)
    )


def load_gadm(url: str = GADM_REMOTE_URL) -> GeoDataFrame:
    return prepare_regions(read_shapefile(build_gadm_filepath(url)))
