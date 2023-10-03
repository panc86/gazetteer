import logging
import os
import pandas
import zipfile
import requests
import warnings

os.environ['USE_PYGEOS'] = '0'
import geopandas

from tqdm import tqdm

# remove user warnings
warnings.filterwarnings("ignore", category=UserWarning)

# set fiona logging level to error to reduce verbosity
logging.getLogger("fiona").setLevel(logging.ERROR)

# data path
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def download_geometries(url: str) -> str:
    """Download external data."""
    path = os.path.join(DATA_PATH, os.path.basename(url))
    logging.info("⏳ downloading from {url} to {path}".format(url=url, path=path))
    if os.path.exists(path):
        return path
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        chunk_size = 1024
        total_bytes = int(r.headers.get('content-length', 0))
        with open(path, 'wb') as f:
            with tqdm(unit='B', unit_scale=True, unit_divisor=chunk_size, miniters=1, desc=path, total=total_bytes) as p:
                for chunk in r.iter_content(chunk_size=chunk_size*10):
                    f.write(chunk)
                    p.update(len(chunk))
    return path


def unzip_gadm(filepath):
    logging.debug("unzip {}".format(filepath))
    with zipfile.ZipFile(filepath, 'r') as archive:
        archive.extractall(path=os.path.dirname(filepath))
    return filepath.replace("-gdb.zip", ".gdb")


def load_gadm(path: str) -> pandas.DataFrame:
    logging.debug("loading GADM polygons")
    return geopandas.read_file(path, driver='FileGDB')


def remove_polygons(df: pandas.DataFrame) -> pandas.DataFrame:
    """Remove polygons without social media activity"""
    logging.debug("remove unused GADM polygons")
    to_drop = ["Antartica", "Caspian Sea"]
    return df.loc[~df.NAME_0.isin(to_drop), :].copy()


def update_polygon_meta(df: pandas.DataFrame) -> pandas.DataFrame:
    """Update missing metadata"""
    logging.debug("update missing GADM polygon meta")
    # Ukraine
    df.loc[(df.NAME_1 == "?").idxmax(), ["NAME_1","NL_NAME_1","HASC_1","ENGTYPE_1"]] = ["Kiev City", "Київ", "UA.KC", "Independent City"]
    return df


def fill_missing_region_names(df: pandas.DataFrame) -> pandas.DataFrame:
    """Fill missing region names with country name"""
    logging.debug("fill missing GADM region names with country name")
    regionless = df.NAME_1.isna()
    df.loc[regionless, "NAME_1"] = df.loc[regionless, "NAME_0"]
    return df


def rename_region_name_attributes(df: pandas.DataFrame) -> pandas.DataFrame:
    """Rename wrong region name attributes"""
    logging.debug("rename wrong GADM region name attributes")
    mapping = {
        "Apulia": "Puglia",
        "Sicily": "Sicilia",
    }
    df.NAME_1 = df.NAME_1.replace(mapping)
    return df


def download_and_prepare_gadm():
    url = "https://geodata.ucdavis.edu/gadm/gadm4.1/gadm_410-gdb.zip"
    df = load_gadm(unzip_gadm(download_geometries(url)))
    return (
        df.pipe(remove_polygons)  # remove inhabitated polygons e.g. Antartica
          .pipe(update_polygon_meta)  # e.g. Ukraine has missing data
          .pipe(fill_missing_region_names)  # small countries are regionless
          .pipe(rename_region_name_attributes)  # rename regions using local knownledge
    )


def download_geonames_cities_15k():
    """
    Geonames DB @ http://download.geonames.org/export/dump/cities15000.zip
    """
    logging.debug("loading geonamnes cities15000 points")
    fields = [
        # from cities15000.txt (Geonames)
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
    path = download_geometries("http://download.geonames.org/export/dump/cities15000.zip")
    cities = pandas.read_csv(path, sep="\t", names=fields)
    return cities


def points_in_polygons_lookup(points, polygons) -> pandas.Series:
    logging.debug("⏳ executing point-in-polygon spatial join")
    # exec spatial join and return index_right i.e. the polygon ID containing the point
    polygons_geo = polygons[["geometry"]]
    # build geometry from latitude/longitude set
    points_geo = geopandas.GeoDataFrame().set_geometry(geopandas.points_from_xy(points.longitude, points.latitude)).set_crs(polygons.crs)
    # spatial join concatenate features with predicate. index_right is the index of polygons containing the points
    spatial_lookup = points_geo.sjoin(polygons_geo, how="left", predicate="within")["index_right"]
    # if NaN in data not all points were within a polygon e.g. coastal cities off-shore
    not_in_polygon = spatial_lookup.isna()
    if not_in_polygon.any():
        # nearest spatial join
        spatial_lookup[not_in_polygon] = points_geo[not_in_polygon].sjoin_nearest(polygons_geo, how="left")["index_right"]
    return spatial_lookup.astype(int)


def build_place_gazetteer(points: pandas.DataFrame) -> pandas.DataFrame:
    logging.debug("build place gazetteer")
    # extract places
    places = points.city_name.rename("place_name")
    ascii_places = points.city_asciiname.rename("place_name_ascii")
    # explode_alternate_names
    alt_places = pandas.DataFrame(
        points.city_alternatenames.fillna('tempvalue').str.split(",").tolist()
    ).replace('tempvalue', None).add_prefix("place_name_alt")
    # build gazetteer
    return pandas.concat([points.latitude.astype(float), points.longitude.astype(float), places, ascii_places, alt_places], axis=1)


def build_region_gazetteer(polygons: pandas.DataFrame) -> pandas.DataFrame:
    logging.debug("build region gazetteer")
    prefix = "region_"
    # concatenate lower level first to be stacked right to places in next step
    varnames = pandas.DataFrame()
    for lev in range(4):
        col = "VARNAME_{}".format(lev+1)
        varnames = pandas.concat([
            varnames,
            pandas.DataFrame(
                polygons[col].fillna('tempvalue').str.split("|").tolist()
            ).add_prefix("".join([prefix, col, "_alt"]))
        ], axis=1)
    # build region gazetteer
    gazetteer = pandas.concat([
        varnames.replace('tempvalue', None),
        polygons.loc[:, ["NAME_5","NAME_4","NL_NAME_3","NAME_3","NL_NAME_2","NAME_2","NL_NAME_1","NAME_1"]].add_prefix(prefix),
        polygons.NAME_0,
        polygons.GID_0,
        polygons.UID,
    ], axis=1)
    # normalize column names
    gazetteer.columns = gazetteer.columns.str.lower()
    return gazetteer


def build_gazetteer(points: pandas.DataFrame, polygons: pandas.DataFrame) -> None:
    logging.info("building gazetteer")
    places_gazetteer = build_place_gazetteer(points)
    regions_gazetteer = build_region_gazetteer(polygons)
    places_gazetteer["polygon_index"] = points_in_polygons_lookup(points, polygons)
    # stack region names right to follow the place size logic place > region_N > Country
    gazetteer = places_gazetteer.join(regions_gazetteer, on="polygon_index").drop(columns=["polygon_index"])
    gazetteer.to_json(os.path.join(DATA_PATH, "gazetteer.json.zip"), orient="records", date_format=None, lines=True, force_ascii=False)


def build_regions(polygons: pandas.DataFrame) -> None:
    """Build regions from dissolved polygons"""
    logging.info("building regions")
    features = ["geometry","UID","NL_NAME_1","NAME_1","NAME_0","GID_0"]
    # aggregate regions using GADM NAME_1 level shape(3555, 6)
    polygons.dissolve(
        by="NAME_1", aggfunc='first', as_index=False, level=None, sort=True, observed=False, dropna=False
    )[features].to_file(os.path.join(DATA_PATH, 'gadm41_regions.shp.zip'), driver='ESRI Shapefile')


def main():
    # download and prepare shapes data
    polygons = download_and_prepare_gadm()
    points = download_geonames_cities_15k()
    # build gazetteer
    build_gazetteer(points, polygons)
    # build regions
    build_regions(polygons)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog="Gazetteer",
        description="Build a region shapefile and places gazetteer using GADM and Geonames data."
    )
    parser.add_argument(
        "--debug", action="store_true", default=False, help="Enables debugging."
    )
    args = parser.parse_args()
    logging.root.setLevel(logging.DEBUG if args.debug else logging.INFO)
    main()
