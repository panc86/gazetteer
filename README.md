# Regions Builder

Build a *regions.gpkg*, and a *places.gpkg* geopackage files to serve as the input of the Geocoder plugin.
The files are saved in ~/plugins/geocoder/data.

## Build

![Python](https://img.shields.io/badge/Python-3.8-information)&nbsp;&nbsp;![Geopandas](https://img.shields.io/badge/Geopandas-~=0.10-information)&nbsp;&nbsp;![Pandas](https://img.shields.io/badge/Pandas-~=1.4-information)&nbsp;&nbsp;![requests](https://img.shields.io/badge/Requests-2.27.1-information)&nbsp;&nbsp;![rtree](https://img.shields.io/badge/rtree-1.0.0-information)&nbsp;&nbsp;![tqdm](https://img.shields.io/badge/tqdm-~=0.46-information)

> :bangbang: Execute all bash commands from project root directory

> :coffee: This may take several minutes depending on you connection.

```shell
# export `http_proxy`, and `https_proxy` environment variables if behind a proxy
bash build.sh
```

## Info

The *region-builder* Docker image downloads the GADM [gadm404-gpkg](https://biogeo.ucdavis.edu/data/gadm4.0/gadm404-gpkg.zip) geopackage,
and the Geonames [cities15000](https://download.geonames.org/export/dump/cities15000.zip) to create the *regions.gpkg*, and *places.gpkg* geopackages.

### Regions.gpkg

Derivate product of gadm404-gpkg.

It supports the conversion of standard inputs such as latitude/longitude coordinates, and bounding box bounds into regions metadata.

We combine several GADM levels to create a set of polygons (with metadata) to represents a customary geopolitical world division.
For example, we dissolve small countries like archipelagos using the level 0, or big countries like the US, or China using level 2.

Each region contains:
* `agglev` / GADM level
* `geometry` / region geometry
* `region_id` / unique region identifier
* `country_name` / GADM NAME_0 attribute
* `region_name` / GADM NAME_1 attribute
* `subregion_name` / GADM NAME_2 attribute
* `region_names` / GADM VARNAME + NL_NAME concatenation of level 1 to 2

> :bulb: You can create your own regions geopackage file to use with out Geocoder.
> Ensure that `region_id`, `names`, and `geometry` fields are present.

### Places.gpkg

Derivate product of cities15000.

It support *regions.gpkg* `names` field metadata enrichment.

For each polygon in *regions.gpkg*, it contains a number of places (i.e. cities, or facilities) expressed in WGS-84 longitude/latitude coordinates system.
We execute a *Point In Polygon* spatial join between cities with population above 15.000 inhabitants, and *regions.gpkg* `geometry`.

Each place contains:
* `geometry` / region geometry
* `region_id` / id of the region spatially connected
* `place_name` / the place name (i.e. name of a city, or facility)
* `latitude` / latitude float
* `longitude` / longitude float

> :bulb: You can create your own places geopackage file to use with out Geocoder.
> Ensure that `region_id`[^1], `place_name`, `latitude`, and `longitude` fields are present.

## GADM

```shell
# regions.gpkg data point example
{'GID_0': 'AUS', 'NAME_0': 'Australia', 'GID_1': 'AUS.1_1', 'NAME_1': 'Ashmore and Cartier Islands', 'NL_NAME_1': None, 'GID_2': 'AUS.1.1_1', 'NAME_2': 'Ashmore and Cartier Islands', 'VARNAME_2': None, 'NL_NAME_2': None, 'TYPE_2': 'Territory', 'ENGTYPE_2': 'Territory', 'CC_2': None, 'HASC_2': None}
```

[^1]: *places.gpkg* should be already spatially joined with *regions.gpkg*
