
"""
Run the NRT production pipeline
"""

import argparse
import os
import sys
from chimera.logger import logger
from chimera.commons.sciflo_util import run_sciflo

# Set up logging
LOGGER = logger


BASE_PATH = os.path.dirname(__file__)


def main(sfl_file, context_file, output_folder):
    """Main."""

    sfl_file = os.path.abspath(sfl_file)
    context_file = os.path.abspath(context_file)
    output_file = os.path.abspath(output_folder)
    LOGGER.info("sfl_file: %s" % sfl_file)
    LOGGER.info("context_file: %s" % context_file)
    return run_sciflo(sfl_file, ["sf_context=%s" % context_file], output_folder)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sfl_file", help="SciFlo workflow")
    parser.add_argument("context_file", help="HySDS context file")
    parser.add_argument("output_folder", help="Sciflo output file")
    args = parser.parse_args()
    sys.exit(main(args.sfl_file, args.context_file, args.output_folder))
