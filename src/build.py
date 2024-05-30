import logging
import os

from geopandas import GeoDataFrame, points_from_xy
import pandas

from config import CRS, DATA_PATH


logger = logging.getLogger(__name__)


GAZETTEER_FILEPATH = os.path.join(DATA_PATH, "gazetteer.json.zip")


def field_to_frame(field: pandas.Series) -> pandas.DataFrame:
    return pandas.DataFrame(field.fillna("tempvalue").str.split(",").tolist())


def explode_region_names(regions: GeoDataFrame) -> pandas.DataFrame:
    fields = ["VARNAME_1", "VARNAME_2", "VARNAME_3", "VARNAME_4"]
    return pandas.concat(
        [
            field_to_frame(regions[field]).add_prefix(f"REGION_{field}_ALT")
            for field in fields
        ],
        axis=1,
    ).replace("tempvalue", None)


def build_region_gazetteer(regions: GeoDataFrame) -> GeoDataFrame:
    logger.debug("building regions gazetteer")
    return GeoDataFrame(
        pandas.concat(
            [
                regions.loc[:, ["UID", "NAME_0", "GID_0"]],
                regions.loc[
                    :,
                    [
                        "NAME_1",
                        "NL_NAME_1",
                        "NAME_2",
                        "NL_NAME_2",
                        "NAME_3",
                        "NL_NAME_3",
                        "NAME_4",
                        "NAME_5",
                    ],
                ].add_prefix("REGION_"),
                explode_region_names(regions),
            ],
            axis=1,
        ),
        crs=CRS,
        geometry=regions.geometry,
    )


def build_place_gazetteer(places: pandas.DataFrame) -> GeoDataFrame:
    logger.debug("building places gazetteer")
    features = ["latitude", "longitude", "city_name", "city_asciiname"]
    return GeoDataFrame(
        pandas.concat(
            [
                places.loc[:, features],
                field_to_frame(places.city_alternatenames)
                .replace("tempvalue", None)
                .add_prefix("city_altname"),
            ],
            axis=1,
        ),
        crs=CRS,
        geometry=points_from_xy(places.longitude, places.latitude),
    )


def join_places_in_region(places: GeoDataFrame, regions: GeoDataFrame):
    logger.debug("executing point-in-polygon spatial join")
    joined = places.sjoin(regions, how="left", predicate="within")
    missing = joined.index_right.isna()
    if missing.any():
        joined.loc[missing, :] = (
            joined.loc[missing, :]
            .drop(columns="index_right")
            .sjoin_nearest(regions, how="left")
        )
    return joined.drop(columns="index_right").reset_index(drop=True)


def build_gazetteer(geonames: pandas.DataFrame, gadm: GeoDataFrame) -> pandas.DataFrame:
    logger.info("building gazetteer")
    gazetteer = join_places_in_region(
        build_place_gazetteer(geonames),
        build_region_gazetteer(gadm),
    )
    gazetteer.columns = gazetteer.columns.str.lower()
    return gazetteer.drop(columns="geometry")
