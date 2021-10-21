import json
import os
import traceback

from importlib import import_module

from chimera.logger import logger
from chimera.commons.conf_util import YamlConf, load_config
from chimera.commons.constants import ChimeraConstants
from chimera.postprocess_functions import PostProcessFunctions


class PostProcessor(object):
    def __init__(
        self,
        sf_context,
        chimera_config_filepath,
        pge_config_filepath,
        settings_file,
        job_result
    ):
        # load context file
        if isinstance(sf_context, dict):
            self._sf_context = sf_context
        elif isinstance(sf_context, str):
            self._sf_context = json.load(open(sf_context, "r"))
        logger.debug("Loaded context file: {}".format(json.dumps(self._sf_context)))

        # load pge config file
        self._pge_config = load_config(pge_config_filepath)
        logger.debug("Loaded PGE config file: {}".format(json.dumps(self._pge_config)))

        # load PP config file
        try:
            self._chimera_config = YamlConf(chimera_config_filepath).cfg
            self._module_path = self._chimera_config.get("postprocessor", {}).get(
                "module_path", None
            )
            if not self._module_path:
                raise RuntimeError(
                    "'module_path' must be defined in the 'preprocessor' section of the "
                    "Chimera Config file '{}'".format(chimera_config_filepath)
                )
            self._class_name = self._chimera_config.get("postprocessor", {}).get(
                "class_name", None
            )
            if not self._class_name:
                raise RuntimeError(
                    "'class_name' must be defined in the 'preprocessor' section of the "
                    "Chimera Config file '{}'".format(chimera_config_filepath)
                )
        except Exception as e:
            raise RuntimeError(
                "Could not read preconditions definition file : {}".format(e)
            )

        # load Settings file
        try:
            if settings_file:
                settings_file = os.path.abspath(os.path.normpath(settings_file))
                self._settings = YamlConf(settings_file).cfg
        except Exception as e:
            if settings_file:
                file_name = settings_file
            else:
                file_name = "~/verdi/etc/settings.yaml"
            raise RuntimeError(
                "Could not read settings file '{}': {}".format(file_name, e)
            )

        # load PGE job result
        if isinstance(job_result, dict):
            self._job_result = job_result
        elif isinstance(job_result, str):
            self._job_result = json.load(open(job_result, "r"))
        self._job_result["work_dir"] = os.path.dirname(sf_context)
        logger.debug("Loaded job result: {}".format(json.dumps(self._job_result)))

    def prepare_psuedo_context(self, psuedo_context):
        """
        Write the gathered job and product metadata information to the psuedo context file.
        :return: dict
        """
        logger.debug(
            "Preparing psuedo_context file after {} run".format(
                self._pge_config.get("pge_name")
            )
        )
        # write out job context
        psu_context = open(
            "{}_context.json".format(self._pge_config.get("pge_name")), "w"
        )
        psu_context.write(json.dumps(psuedo_context))
        psu_context.close()
        return "{}_context.json".format(self._pge_config.get("pge_name"))

    def process(self):
        new_context = dict()
        try:
            module = import_module(self._module_path)
            cls = getattr(module, self._class_name)
            if not issubclass(cls, PostProcessFunctions):
                raise RuntimeError(
                    "Class must be a subclass of {}: {}".format(
                        PostProcessFunctions.__name__, cls.__name__
                    )
                )
            # run mandatory post process funtions
            # new_context.update(self.required_post_process_steps())
            # run custom post processing steps and update the psuedo context content
            cls_object = cls(
                self._sf_context, self._pge_config, self._settings, self._job_result
            )
            new_context.update(
                cls_object.run(
                    self._pge_config.get(ChimeraConstants.POSTPROCESS, list())
                )
            )
            # write to output context file
            new_context_file = self.prepare_psuedo_context(new_context)
            return new_context_file
        except Exception as e:
            logger.error(
                "Post processor failure: {}. {}".format(e, traceback.format_exc())
            )
            raise RuntimeError("Post processor failure: {}".format(e))
