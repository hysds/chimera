
"""
Run the NRT production pipeline
"""

import argparse
import os
import json
import sys
from importlib import import_module

from chimera.logger import logger
from chimera.commons.accountability import Accountability
from chimera.commons.sciflo_util import run_sciflo

# Set up logging
LOGGER = logger


BASE_PATH = os.path.dirname(__file__)


# grabs accountability class if implemented and set in the sciflo jobspecs
def get_accountability_class(context_file):
    work_dir = None
    context = None
    if isinstance(context_file, str):
        work_dir = os.path.dirname(context_file)
        with open(context_file, "r") as f:
            context = json.load(f)
    path = context.get("module_path")
    if "accountability_module_path" in context:
        path = context.get("accountability_module_path")
    accountability_class_name = context.get("accountability_class", None)
    accountability_module = import_module(path, "nisar-pcm")
    if accountability_class_name is None:
        LOGGER.error(
            "No accountability class specified"
        )
        return Accountability(context, work_dir)
    cls = getattr(accountability_module, accountability_class_name)
    if not issubclass(cls, Accountability):
        LOGGER.error(
            "accountability class does not extend Accountability"
        )
        return Accountability(context, work_dir)
    cls_object = cls(context, work_dir)
    return cls_object


# sets the accountability status as failed or doesn't do anything at all
# def set_status_failed(context_file):
#     context = {}
#     with open(context_file, "r") as f:
#         import json
#         context = json.load(f)
#     try:
#         #from nisar_chimera.commons.accountability import NisarAccountability
#         accountability = get_accountability_class(context)
#         accountability.set_status("job-failed")
#     except Exception as e:
#         LOGGER.info("could not get accountability object")
#         LOGGER.info("path: {}".format(sys.path))
#         LOGGER.error(e)


def main(sfl_file, context_file, output_folder):
    """Main."""

    sfl_file = os.path.abspath(sfl_file)
    context_file = os.path.abspath(context_file)
    output_file = os.path.abspath(output_folder)
    LOGGER.info("sfl_file: %s" % sfl_file)
    LOGGER.info("context_file: %s" % context_file)
    accountability = get_accountability_class(context_file)
    accountability.create_job_entry()
    result = run_sciflo(sfl_file, ["sf_context=%s" % context_file], output_folder)
    # if result != 0:
    #     # sets status as failed if accountability implemented in chimera, otherwise, does nothing
    #     set_status_failed(context_file)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sfl_file", help="SciFlo workflow")
    parser.add_argument("context_file", help="HySDS context file")
    parser.add_argument("output_folder", help="Sciflo output file")
    args = parser.parse_args()
    sys.exit(main(args.sfl_file, args.context_file, args.output_folder))
