from argparse import ArgumentParser
import logging

from build import build_gazetteer, GAZETTEER_FILEPATH
from gadm import GADM_REMOTE_URL, load_gadm
from geonames import GEONAMES_REMOTE_URL, load_geonames

 
logger = logging.getLogger(__name__)


def main():
    parser = ArgumentParser(
        description="Build gazetteer using GADM and Geonames data"
    )
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--gadm-url", default=GADM_REMOTE_URL, help="%(default)s")
    parser.add_argument("--geonames-url", default=GEONAMES_REMOTE_URL, help="%(default)s")
    args = parser.parse_args()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    build_gazetteer(
        load_geonames(args.geonames_url),
        load_gadm(args.gadm_url),
    ).to_json(
        GAZETTEER_FILEPATH,
        force_ascii=False,
        lines=True,
        orient="records",
    )
    logger.info("done")


if __name__ == "__main__":
    main()
