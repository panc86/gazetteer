import logging.config
import os
import warnings

import geopandas
import pyogrio


# remove user warnings
warnings.filterwarnings("ignore")

# set default map projections
CRS = "EPSG:4326"

# set default shapes IO engine
geopandas.options.io_engine = "pyogrio"
# do not preprocess polygons to save time
pyogrio.set_gdal_config_options({"OGR_ORGANIZE_POLYGONS": "SKIP"})


DATA_PATH = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_PATH, exist_ok=True)


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s %(levelname)-8s %(name)s.%(funcName)s %(message)s"
        },
    },
    "handlers": {
        "consolehandler": {
            "level": "DEBUG",
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "gazetteer": {"handlers": ["consolehandler"], "level": "INFO", "propagate": False},
        "pyproj": {"level": "WARNING"}
    },
    "root": {
        "level": "DEBUG",
        "handlers": ["consolehandler"]
    }
}
logging.config.dictConfig(LOGGING_CONFIG)
