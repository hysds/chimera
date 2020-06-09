#!/usr/bin/env python
"""
Contributors:
- Sujen Shah
- Michael Cayanan
- Namrata Malarout

This is the first step of Chimera called Input Preprocessor (IPP)
The Input preprocessor runs all the preconditions and constructs the configuration required to run an algorithm (PGE)
"""

from chimera.logger import logger
from chimera.precondition_evaluator import PreConditionEvaluator


def process(sf_context, chimera_config_file, pge_config_filepath, settings_file):
    """
    Process the inputs to check if the preconditions for the provided PGE are satisfied.
    :param sf_context: Input context (sciflow context or post processor output)
    :param chimera_config_file: Chimera config file.
    :param pge_config_filepath: path to the pge config json file
    :param settings_file: Settings file.

    :return: python dict containing context for the PGE to run
    """
    logger.info("Starting input_preprocessor step.")
    pre_cond_evaluator = PreConditionEvaluator(sf_context, chimera_config_file, pge_config_filepath, settings_file)
    output_context = pre_cond_evaluator.evaluate()
    logger.info("Finished input_preprocessor step.")
    return output_context


if __name__ == '__main__':
    pass
