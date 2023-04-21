import os
import numpy
import pandas
import zipfile
import requests

os.environ['USE_PYGEOS'] = '0'
import geopandas

from tqdm import tqdm

# paths
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def download_data(url, filepath):
    """Download external data."""
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
    with zipfile.ZipFile(filepath, 'r') as archive:
        for f in archive.infolist():
            f.filename = fn
            archive.extract(f, path=data_dir)


def apply_polygon_names_mapping(polygons):
    """Fix wrong polygon name attributes"""
    mapping = {
        "Apulia": "Puglia",
        "Sicily": "Sicilia",
    }
    polygons.NAME_1 = polygons.NAME_1.replace(mapping)
    return polygons


def drop_polygons(polygons):
    to_drop = [
        "Antarctica",
    ]
    indices = polygons[polygons.NAME_0.isin(to_drop)].index
    return polygons.drop(indices)


def get_gadm_polygons():
    """Load polygons from GADM GeoPackage data. https://gadm.org/metadata.html"""
    print("⏳ Fetching GADM polygons...")
    dwn_filepath = os.path.join(data_dir, "gadm-gpkg.zip")
    if not os.path.exists(dwn_filepath):
        download_data("https://geodata.ucdavis.edu/gadm/gadm4.0/gadm404-gpkg.zip", dwn_filepath)
    ext_filepath = os.path.join(data_dir, "gadm.gpkg")
    if not os.path.exists(ext_filepath):
        extract_data(dwn_filepath, "gadm.gpkg")
    return geopandas.read_file(ext_filepath).replace(" ", numpy.nan).replace("?", numpy.nan).replace("n.a.", numpy.nan)


def get_geonames_cities15k():
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
    print("⏳ Fetching (geonames) cities15k points...")
    dwn_filepath = os.path.join(data_dir, "cities15000.zip")
    if not os.path.exists(dwn_filepath):
        download_data("http://download.geonames.org/export/dump/cities15000.zip", dwn_filepath)
    ext_filepath = os.path.join(data_dir, "cities15000.txt")
    if not os.path.exists(ext_filepath):
        extract_data(dwn_filepath, "cities15000.txt")
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
    return pandas.read_csv(ext_filepath, sep="\t", names=fields)


def build_places_gazetteer(points):
    features = [
        "latitude",
        "longitude",
        "city_name",
        "city_alternatenames",
    ]
    gazetteer = points.loc[:, features].rename(columns={"city_name": "place_name"})
    altname = pandas.DataFrame(gazetteer.pop("city_alternatenames").fillna('').str.split(",").tolist())
    altname.columns = ["place_name_alt{}".format(index) for index in range(altname.shape[1])]
    return pandas.concat([gazetteer, altname], axis=1).convert_dtypes()


def build_regions_gazetteer(polygons):
    gazetteer = pandas.DataFrame(index=polygons.index)
    gazetteer["country_code"] = polygons.GID_0
    gazetteer["country_name"] = polygons.NAME_0
    prefix = "region_"
    gazetteer = pandas.concat([gazetteer, polygons.loc[:, ["NAME_1", "NL_NAME_1", "NAME_2", "NL_NAME_2", "NAME_3", "NL_NAME_3", "NAME_4", "NAME_5"]].add_prefix(prefix)], axis=1)

    for lev in range(4):
        col = "VARNAME_{}".format(lev+1)
        altname = pandas.DataFrame(polygons[col].fillna('').str.split("|").tolist())
        altname.columns = [prefix+"{col}_alt{index}".format(col=col, index=index) for index in range(altname.shape[1])]
        gazetteer = pandas.concat([gazetteer, altname], axis=1)

    gazetteer.columns = gazetteer.columns.str.lower()
    return gazetteer.convert_dtypes()


def point_in_polygon(points, polygons):
    print("⏳ Executing point-in-polygon spatial join...")
    # exec spatial join and return index_right i.e. the polygon ID containing the point
    joined = points.sjoin(polygons, how="left", predicate="within")
    # spatially join points outside polygon with nearest polygon and update missing records
    not_in_polygon_mask = joined.index_right.isna()
    joined.loc[not_in_polygon_mask, :] = points[not_in_polygon_mask].sjoin_nearest(polygons, how="left")
    return joined


def main():
    # GADM polygons
    polygons = get_gadm_polygons().pipe(apply_polygon_names_mapping).pipe(drop_polygons)
    polygons_geom = geopandas.GeoDataFrame(polygons.pop("geometry"))
    # geonames (cities) points
    cities = get_geonames_cities15k()
    cities_geom = geopandas.GeoDataFrame(index=cities.index).set_geometry(geopandas.points_from_xy(cities.longitude, cities.latitude)).set_crs(polygons_geom.crs)
    # spatial join
    joined = point_in_polygon(cities_geom, polygons_geom)
    # build gazetteer
    regions_g = build_regions_gazetteer(polygons)
    places_g = build_places_gazetteer(cities)
    # add polygon lookup index i.e. the id of the polygon containing the point
    fn = os.path.join(data_dir, "gazetteer.csv")
    places_g["polygon_index"] = joined.index_right.astype(int)
    gazetteer = places_g.join(regions_g, on="polygon_index").drop(columns=["polygon_index"])
    gazetteer.to_csv(fn, index=False)
    print("⌛ {fn} built!".format(fn=fn))


if __name__ == "__main__":
    main()
