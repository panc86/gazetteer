# Gazetteer

Build gazetteer using GADM regions and Geonames cities above 15k pop

## Build

> :coffee: It may take several minutes on first built due to GADM database download

Install dependencies

```shell
poetry install
```

Build gazetteer

```shell
poetry run python -B src/main.py
```
