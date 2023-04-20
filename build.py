import logging
import os
import typing
import numpy
import pandas
import zipfile
import requests

os.environ['USE_PYGEOS'] = '0'
import geopandas

from tqdm import tqdm
from config import LEVEL0_REGIONS, LEVEL2_REGIONS

# enable logging
logging.basicConfig(level=logging.INFO)
console = logging.getLogger("tools.places")
# paths
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
# meta
with open("./VERSION") as v:
    VERSION = v.read().strip("\n")

CLEAN = bool(int(os.getenv("CLEAN", 0)))


def clean_data_dir():
    for f in os.listdir(data_dir):
        console.info("removing {}".format(f))
        os.unlink(os.path.join(data_dir, f))


def download_data(url, filepath):
    """Download external data."""
    console.info("downloading from {}".format(url))
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        chunk_size = 1024
        total_bytes = int(r.headers.get('content-length', 0))
        with open(filepath, 'wb') as f:
            with tqdm(unit='B', unit_scale=True, unit_divisor=chunk_size, miniters=1, desc=filepath, total=total_bytes) as p:
                for chunk in r.iter_content(chunk_size=chunk_size*10):
                    f.write(chunk)
                    p.update(len(chunk))


def extract_data(filepath, fn):
    # extract
    console.info("extracting {}".format(filepath))
    with zipfile.ZipFile(filepath, 'r') as archive:
        for f in archive.infolist():
            f.filename = fn
            archive.extract(f, path=data_dir)


def load_gadm_regions(filepath):
    """
    Build polygons from GADM GeoPackage data.
    https://gadm.org/metadata.html
    """
    console.info("loading GADM regions...")
    features = ["geometry", "GID_0", "NAME_0", "NAME_1", "VARNAME_1", "NL_NAME_1", "NAME_2", "VARNAME_2", "NL_NAME_2"]
    regions = geopandas.read_file(filepath)
    return regions[features].replace(" ", numpy.nan).replace("?", numpy.nan).replace("n.a.", numpy.nan)


def load_geonames_15k_cities(filepath):
    """
    # Geonames DB @ http://download.geonames.org/export/dump/cities15000.zip

        The main 'geoname' table has the following fields :
        ---------------------------------------------------
        geonameid         : integer id of record in geonames database
        name              : name of geographical point (utf8) varchar(200)
        asciiname         : name of geographical point in plain ascii characters, varchar(200)
        alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
        latitude          : latitude in decimal degrees (wgs84)
        longitude         : longitude in decimal degrees (wgs84)
        feature class     : see http://www.geonames.org/export/codes.html, char(1)
        feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
        country a2        : ISO-3166 2-letter country code, 2 characters
        cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
        admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
        admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
        admin3 code       : code for third level administrative division, varchar(20)
        admin4 code       : code for fourth level administrative division, varchar(20)
        population        : bigint (8 byte int)
        elevation         : in meters, integer
        dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
        timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
        modification date : date of last modification in yyyy-MM-dd format
        ```

        ### NOTES

        Following field names have been modified for convenience and clarity

        #### cities15000.txt
        ```
        - geonameid       > city_id
        - name            > city_name
        - asciiname       > city_asciiname
        - alternatenames  > city_alternatenames
    """
    features = [
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
    todrop = [
        "feature_class",
        "feature_code",
        "country_a2",
        "cc2",
        "admin1",
        "admin2",
        "admin3",
        "admin4",
        "dem",
        "timezone",
        "modification_date",
    ]
    cities = pandas.read_csv(filepath, sep="\t", names=features).drop(columns=todrop)
    return cities


def dissolve_regions(regions: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    console.info("dissolving regions...")
    def merge_region_names(rows):
        names = set(rows.values.flatten())
        return "|".join([k for k in names if isinstance(k, str)])
    col_mapping = dict(NAME_0="country_name", GID_0="country_code", NAME_1="region_name", NAME_2="subregion_name")
    total = len(regions)
    # filter aggregated regions
    agg_mask = regions.NAME_0.isin(LEVEL0_REGIONS)
    agg_reg = regions[agg_mask]
    agg_reg_names = agg_reg.groupby(by="NAME_0")[["GID_0", "NAME_1", "NL_NAME_1", "VARNAME_1"]].apply(merge_region_names)
    agg_reg_names.name = "region_names"
    agg_reg_dis = agg_reg.dissolve(by="NAME_0", as_index=False).join(agg_reg_names, on="NAME_0").rename(columns=col_mapping).loc[:, ("geometry", "country_name", "country_code", "region_names")]
    agg_reg_dis["agglev"] = 0
    agg_reg_dis["region_name"] = agg_reg_dis["country_name"]
    agg_reg_dis["subregion_name"] = numpy.nan
    console.info("{:.1f}% level 0 regions dissolved".format(round(100*agg_mask.sum()/total, 1)))
    # filter decomposed regions
    dec_mask = regions.NAME_0.isin(LEVEL2_REGIONS)
    dec_reg = regions[dec_mask].fillna("")
    dec_dissolve_key = regions.NAME_0+"-"+regions.NAME_1+"-"+regions.NAME_2
    dec_reg["region_names"] = (dec_reg.NAME_1 +"|"+ dec_reg.NL_NAME_1 +"|"+ dec_reg.VARNAME_1 +"|"+ dec_reg.NAME_2 +"|"+ dec_reg.NL_NAME_2 +"|"+ dec_reg.VARNAME_2).str.strip("|")
    dec_reg_dis = dec_reg.dissolve(by=dec_dissolve_key, as_index=False).rename(columns=col_mapping).loc[:, ("geometry", "country_name", "country_code", "region_name", "subregion_name", "region_names")]
    dec_reg_dis["agglev"] = 2
    console.info("{:.1f}% level 2 regions dissolved".format(round(100*dec_mask.sum()/total, 1)))
    # filter remaining regions
    other_mask = ~(agg_mask | dec_mask)
    other_reg = regions[other_mask].fillna("")
    other_dissolve_key = regions.NAME_0+"-"+regions.NAME_1
    other_reg["region_names"] = (other_reg.NAME_1 +"|"+ other_reg.NL_NAME_1 +"|"+ other_reg.VARNAME_1).str.strip("|")
    other_reg_dis = other_reg.dissolve(by=other_dissolve_key, as_index=False).rename(columns=col_mapping).loc[:, ("geometry", "country_name", "country_code", "region_name", "region_names")]
    other_reg_dis["agglev"] = 1
    other_reg_dis["subregion_name"] = numpy.nan
    console.info("{:.1f}% level 1 regions dissolved".format(round(100*other_mask.sum()/total, 1)))
    dissolved = pandas.concat([other_reg_dis, agg_reg_dis, dec_reg_dis]).reset_index(drop=True).fillna(numpy.nan).rename(columns=col_mapping)
    return dissolved


def build_regions_index(regions: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    console.info("building regions index...")
    # build region index
    tmp = regions[["country_name", "region_name", "subregion_name"]].fillna("")
    regions["region_id"] = 10000 + pandas.Categorical(tmp.country_name+"_"+tmp.region_name+"_"+tmp.subregion_name, ordered=True).codes
    # assert region ID uniqueness
    duplicates = regions["region_id"].duplicated(keep=False)
    assert duplicates.sum() == 0, "Duplicates in region ID\n{}".format(regions[duplicates])
    return regions


def compute_regions_centroid(regions: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    console.info("computing regions centroids...")
    centroids = regions.geometry.to_crs('+proj=cea').centroid.to_crs(regions.crs)
    regions["latitude"] = round(centroids.y, 6)
    regions["longitude"] = round(centroids.x, 6)
    return regions


def drop_regions(regions):
    to_drop = [
        "Antarctica",
    ]
    indices = regions[regions.NAME_0.isin(to_drop)].index
    return regions.drop(indices)


def apply_region_names_mapping(regions):
    """Fix wrong region names"""
    region_names_mapping = {
        "Apulia": "Puglia",
        "Sicily": "Sicilia",
    }
    regions.region_name = regions.region_name.replace(region_names_mapping)
    return regions


def save_to_gpkg(data, filename):
    console.info("saving gpkg file...")
    data.to_file(os.path.join(data_dir, "{0}_{1}.gpkg".format(filename, VERSION)), driver='GPKG')


def build_regions() -> geopandas.GeoDataFrame:
    console.info("building regions...")
    # normalize name
    dwn_filepath = os.path.join(data_dir, "gadm-gpkg.zip")
    if not os.path.exists(dwn_filepath):
        # download GADM polygons
        download_data("https://geodata.ucdavis.edu/gadm/gadm4.0/gadm404-gpkg.zip", dwn_filepath)

    ext_filepath = os.path.join(data_dir, "gadm.gpkg")
    if not os.path.exists(ext_filepath):
        # extract
        extract_data(dwn_filepath, "gadm.gpkg")
    regions = load_gadm_regions(ext_filepath)
    regions_final = (
            regions.pipe(drop_regions)
                   .pipe(dissolve_regions)
                   .pipe(build_regions_index)
                   .pipe(compute_regions_centroid)
                   .pipe(apply_region_names_mapping)
            )
    save_to_gpkg(regions_final, "regions")
    # remove extracted file
    os.unlink(ext_filepath)
    return regions_final


def build_places(polygons):
    console.info("building places...")
    dwn_filepath = os.path.join(data_dir, "cities15000.zip")
    if not os.path.exists(dwn_filepath):
        # download
        download_data("http://download.geonames.org/export/dump/cities15000.zip", dwn_filepath)
    ext_filepath = os.path.join(data_dir, "cities15000.txt")
    if not os.path.exists(ext_filepath):
        # extract
        extract_data(dwn_filepath, "cities15000.txt")
    # load
    cities = load_geonames_15k_cities(ext_filepath)
    # build geometry
    geometry = geopandas.points_from_xy(cities.longitude, cities.latitude)
    # as geodatabase
    points = geopandas.GeoDataFrame(cities, geometry=geometry, crs=polygons.crs)
    # exec spatial join
    joined = points.sjoin(polygons, how="left", predicate="within")
    # points not in polygon
    not_in_polygon_mask = joined.index_right.isna()
    console.debug("Points not in polygon: {}".format(not_in_polygon_mask.sum()))
    # join points close to regions
    joined_nearest = points[not_in_polygon_mask].sjoin_nearest(polygons, how="left")
    console.debug("points outside polygons\n{}\n".format(joined_nearest))
    # update records with point not in polygon
    joined.loc[not_in_polygon_mask, :] = joined_nearest
    joined["region_id"] = joined["region_id"].astype(int)
    assert joined["region_id"].isna().sum() == 0, "Places must have a region ID assigned to it."
    # cleaned
    to_drop = ["index_right", "agglev", "city_id", "city_asciiname"]
    mapping = {"city_name": "place_name", "city_alternatenames": "place_names"}
    places = joined.drop(columns=to_drop).rename(columns=mapping)
    save_to_gpkg(places, "places")
    # remove regions file after spatial join
    os.unlink(os.path.join(data_dir, "regions_v1.gpkg"))
    return places


def build_gazetteer(places):
    # extract place names
    names = places[["country_name", "country_code", "region_id", "region_name", "subregion_name", "place_name"]].astype("category")
    coords = places[["latitude", "longitude"]]
    # extract alternative region names
    alt_region_names = pandas.DataFrame(places.region_names.fillna('').str.split('|').tolist()).astype("category")
    alt_region_names.columns = ["region_an_{n}".format(n=c) for c in alt_region_names.columns]
    # extract alternative place names
    alt_place_names = pandas.DataFrame(places.place_names.fillna('').str.split(',').tolist()).astype("category")
    alt_place_names.columns = ["place_an_{n}".format(n=c) for c in alt_place_names.columns]
    # concatenate
    return pandas.concat([names, alt_region_names, alt_place_names, coords], axis=1)


if __name__ == "__main__":
    if CLEAN:
        clean_data_dir()
    # build smdrm regions (GADM polygons)
    regions = build_regions()
    # build smdrm places (Geonames cities)
    places = build_places(regions.drop(columns=["latitude", "longitude"]))
    # build gazetteer
    gazetteer = build_gazetteer(places)
    gazetteer.to_pickle(os.path.join(data_dir, "gazetteer.pkl"), protocol=5)
