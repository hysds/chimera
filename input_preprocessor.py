#!/usr/bin/env python
from collections import OrderedDict
import logging
import traceback
import sys
import simplejson
from commons import constants, preconditions
import yaml

# Set up logging
LOGGER = logging.getLogger("inputpp")
LOGGER.setLevel(logging.DEBUG)
stdout = logging.StreamHandler(sys.stdout)
stdout.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout.setFormatter(formatter)
LOGGER.addHandler(stdout)

# This is the output generated
job_params = {}
# Used to identify fields to be filled within the runconfig context.json of PGE
EMPTY_FIELD_IDENTIFIER = None


def _validate_pge_config(pge_config):
    """
    To validate if the PGE config json is valid
    :param pge_config:
    :return: Boolean
    """
    # Validate metadata
    # Check if runconfig key is present
    if pge_config.get(constants.RUNCONFIG_KEY) is None:
        raise Exception("{} not found in pge config".format(constants.RUNCONFIG_KEY))

    runconfig = pge_config.get(constants.RUNCONFIG_KEY)
    if "SeriesID" not in runconfig:
        raise Exception("SeriesID missing from {}".format(constants.RUNCONFIG_KEY))


def prepare_runconfig(pge_config):
    """
    To prepare the final completed runconfig context.json which will be fed in to the pge
    :param pge_config:
    :return: dict
    """
    LOGGER.debug("Preparing runconfig for {}".format(pge_config.get('pge_name')))
    output_context = OrderedDict()

    if pge_config.get(constants.RUNCONFIG_KEY):
        for group in pge_config.get(constants.RUNCONFIG_KEY):
            if isinstance(pge_config.get(constants.RUNCONFIG_KEY).get(group), str):
                output_context[group] = pge_config.get(constants.RUNCONFIG_KEY).get(group)
                continue
            elif pge_config.get(constants.RUNCONFIG_KEY).get(group) == EMPTY_FIELD_IDENTIFIER:
                output_context[group] = job_params.get(group)
                continue
            output_context[group] = OrderedDict()  # Create group

            for item in pge_config.get(constants.RUNCONFIG_KEY).get(group):  # Populate group
                item_value = pge_config.get(constants.RUNCONFIG_KEY).get(group).get(item)

                if item_value == EMPTY_FIELD_IDENTIFIER:
                    # Search job params for the item
                    if job_params.get(item) is not None:
                        item_value = job_params.get(item)
                    else:
                        raise ValueError("{} value has not been evaluated by the preprocessor".format(item))

                output_context[group][item] = item_value
    else:
        raise KeyError("Key runconfig not found in PGE config file")

    # Add localized urls
    output_context[constants.LOCALIZE_KEY] = preconditions.localize_paths(pge_config, output_context)
    return output_context


def process(sf_context, ipp_def_filepath, pge_config_filepath, sys_config_file=None, testmode=False):
    """
    Process the inputs to check if the preconditions for the provided PGE are satisfied.
    :param sf_context: Input context (sciflow context or post processor output)
    :param ipp_def_filepath: File of preconditions definition
    :param pge_config_filepath: path to the pge config json file
    :param sys_config_file: path to the system config file
    :param testmode: set test mode to True or False
    :return: python dict containing context for the PGE to run
    """
    try:
        LOGGER.info("Processing example preconditions for {}".format(pge_config_filepath))
        LOGGER.info("Current value of job params {}".format(simplejson.dumps(job_params)))

        # load context file
        context = None
        if isinstance(sf_context, dict):
            context = sf_context
        elif isinstance(sf_context, str):
            context = simplejson.load(open(sf_context, 'r'))
        logging.debug("Loaded context file: {}".format(simplejson.dumps(context)))

        # load pge config file
        pge_config = None
        try:
            pge_config = simplejson.load(open(pge_config_filepath, 'r'), object_pairs_hook=OrderedDict)
        except Exception as e:
            print("Could not load PGE Config : {}".format(e))
        logging.debug("Loaded PGE config file: {}".format(simplejson.dumps(pge_config)))

        # load system config file
        sys_config = None
        try:
            sys_config = simplejson.load(open(sys_config_file, 'r'))
        except Exception as e:
            print("Could not read system config file : {}".format(e))
        logging.debug("Loaded context file: {}".format(simplejson.dumps(sys_config)))

        # load IPP config file
        ipp_config = None
        try:
            ipp_config = yaml.load(open(ipp_def_filepath).read())
        except Exception as e:
            print("Could not read preconditions definition file : {}".format(e))

        # Three variables at all times are available - context, job_params, pge_config, sys_config
        for precond in pge_config.get(constants.PRECONDITIONS_KEY):
            logging.info("Preprocessing condition: {}".format(precond))
            try:
                function_to_exec = "job_params.update( preconditions." + precond + "("
                for param in ipp_config.get(precond):
                    function_to_exec += param + ","
                function_to_exec = function_to_exec[:-1] + "))"
                exec(function_to_exec)
            except TypeError:
                raise NotImplementedError("Precondition {} not evaluated".format(precond))
        preconditions.set_product_time()
        output_context = prepare_runconfig(pge_config)
    except Exception as e:
        LOGGER.error("Input precondition failure: {}".format(traceback.format_exc()))
        raise RuntimeError("Input precondition failure: {}".format(e))

    return output_context


def _debug_log(method_name, msg):
    LOGGER.debug("{}: {}".format(method_name, msg))


if __name__ == '__main__':
    pass
