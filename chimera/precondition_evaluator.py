import json
import os
import copy
import traceback

from importlib import import_module

from chimera.logger import logger
from chimera.commons.conf_util import YamlConf, load_config
from chimera.commons.constants import ChimeraConstants
from chimera.precondition_functions import PreConditionFunctions

from urllib.parse import urlparse

# Used to identify fields to be filled within the runconfig context.json of PGE
EMPTY_FIELD_IDENTIFIER = "__CHIMERA_VAL__"


class PreConditionEvaluator(object):

    def __init__(self, sf_context, chimera_config_filepath, pge_config_filepath, settings_file):
        # load context file
        if isinstance(sf_context, dict):
            self._sf_context = sf_context
        elif isinstance(sf_context, str):
            self._sf_context = json.load(open(sf_context, 'r'))
        logger.debug("Loaded context file: {}".format(json.dumps(self._sf_context)))

        # load pge config file
        self._pge_config = load_config(pge_config_filepath)
        logger.debug("Loaded PGE config file: {}".format(json.dumps(self._pge_config)))

        # load IPP config file
        try:
            self._chimera_config = YamlConf(chimera_config_filepath).cfg
            self._module_path = self._chimera_config.get("preprocessor", {}).get("module_path", None)
            if not self._module_path:
                raise RuntimeError("'module_path' must be defined in the 'preprocessor' section of the "
                                   "Chimera Config file '{}'".format(chimera_config_filepath))
            self._class_name = self._chimera_config.get("preprocessor", {}).get("class_name", None)
            if not self._class_name:
                raise RuntimeError("'class_name' must be defined in the 'preprocessor' section of the "
                                   "Chimera Config file '{}'".format(chimera_config_filepath))
        except Exception as e:
            raise RuntimeError("Could not read preconditions definition file : {}".format(e))

        # load Settings file
        try:
            if settings_file:
                settings_file = os.path.abspath(os.path.normpath(settings_file))
                self._settings = YamlConf(settings_file).cfg
        except Exception as e:
            if settings_file:
                file_name = settings_file
            else:
                file_name = '~/verdi/etc/settings.yaml'
            raise RuntimeError("Could not read settings file '{}': {}".format(file_name, e))

    def repl_val_in_dict(self, d, val, job_params, root=None):
        """
        Recursive function to replace occurences of val in a dict with values from the job_params.
        """

        if root is None: root = []
        matched_keys = []
        for k, v in d.items():
            rt = copy.copy(root)
            rt.append(k)
            if isinstance(v, dict):
                matched_keys.extend(self.repl_val_in_dict(v, val, job_params, rt))
            if v == val:
                jp_key = '.'.join(rt)
                # use job_params with explicit dot notation
                if jp_key in job_params:
                    d[k] = job_params[jp_key]
                    matched_keys.append(jp_key)
                # maintain backwards-compatibility of using job_param values without dot notation
                elif k in job_params:
                    d[k] = job_params[k]
                    matched_keys.append(k)
                else:
                    logger.error("job_params: {}".format(json.dumps(job_params, indent=2, sort_keys=True)))
                    raise(ValueError("{} has not been evaluated by the preprocessor.".format(jp_key)))
        return matched_keys

    def localize_paths(self, output_context):
        """
        To set file to localize in the docker
        :param output_context:
        """
        logger.debug("Preparing to localize file paths")

        # Deprecated function since not all values in localize_groups are on s3
        # for example SPS config files
        def is_url(val):
            parse_result = urlparse(val)
            schemes = ["s3", "s3s", "http", "https",
                       "ftp", "sftp", "azure", "azures", "rsync"]
            return parse_result.scheme in schemes

        localize_paths_list = []
        for group in self._pge_config.get(ChimeraConstants.LOCALIZE_GROUPS, []):
            for elem in output_context.get(group, []):
                value = output_context.get(group).get(elem)

                # If the value is a list, example some InputFilGroups could be
                # scalars or vectors
                if isinstance(value, list):
                    for v in value:
                        if is_url(v):
                            localize_paths_list.append(v)

                elif isinstance(value, str):
                    if is_url(value):
                        localize_paths_list.append(value)

                else:
                    continue

        return localize_paths_list

    def prepare_runconfig(self, job_params):
        """
        To prepare the final completed runconfig context.json which will be fed in
        to the pge
        :return: dict
        """
        logger.debug("Preparing runconfig for {}".format(self._pge_config.get('pge_name')))
        empty_field_identifier = self._pge_config.get(ChimeraConstants.EMPTY_FIELD_IDENTIFIER,
                                                      EMPTY_FIELD_IDENTIFIER)
        logger.debug("Empty field identifier: {}".format(empty_field_identifier))
        output_context = dict()
        #TODO: how to incorporate optional_fields in recursive function
        #optional_fields = self._pge_config.get(ChimeraConstants.OPTIONAL_FIELDS, [])
        if self._pge_config.get(ChimeraConstants.RUNCONFIG):
            output_context = copy.deepcopy(self._pge_config.get(ChimeraConstants.RUNCONFIG))
            matched_keys = self.repl_val_in_dict(output_context, empty_field_identifier, job_params)
        else:
            raise KeyError("Key runconfig not found in PGE config file")

        # Add localized urls
        output_context[ChimeraConstants.LOCALIZE] = self.localize_paths(output_context)
        output_context[ChimeraConstants.SIMULATE_OUTPUTS] = self._settings[ChimeraConstants.PGE_SIM_MODE]

        return output_context

    def evaluate(self):
        job_params = dict()
        try:
            module = import_module(self._module_path)
            cls = getattr(module, self._class_name)
            if not issubclass(cls, PreConditionFunctions):
                raise RuntimeError("Class must be a subclass of {}: {}".format(PreConditionFunctions.__name__,
                                                                               cls.__name__))
            cls_object = cls(self._sf_context, self._pge_config, self._settings, job_params)
            job_params.update(cls_object.run(self._pge_config.get(ChimeraConstants.PRECONDITIONS, list())))
            output_context = self.prepare_runconfig(job_params)
            return output_context
        except Exception as e:
            logger.error("Input precondition failure: {}. {}".format(e, traceback.format_exc()))
            raise RuntimeError("Input precondition failure: {}".format(e))
