"""
Class that submits a PGE job to HySDS. This class can be used as-is, which will rely on HySDS Core's feature
of performing the hash calculation to determine dedup.

"""

import json
import os

from chimera.commons.constants import ChimeraConstants as chimera_const
from chimera.commons.conf_util import load_config, YamlConf
from chimera.logger import logger

from hysds_commons.job_utils import resolve_hysds_job


class PgeJobSubmitter(object):
    def __init__(self, context, run_config, pge_config_file, settings_file, wuid=None, job_num=None):
        # load context file
        if isinstance(context, dict):
            self._context = context
        elif isinstance(context, str):
            self._context = json.load(open(context, 'r'))
        logger.debug("Loaded context file: {}".format(json.dumps(self._context)))

        # This is intended to represent the top level working directory of the job. It's assumed to be at the same
        # level as the given context file.
        self._base_work_dir = os.path.dirname(os.path.abspath(context))

        # load pge config file
        self._pge_config = load_config(pge_config_file)
        logger.debug("Loaded PGE config file: {}".format(json.dumps(self._pge_config)))

        self._wuid = wuid
        self._job_num = job_num

        # load Settings file
        try:
            if settings_file:
                settings_file = os.path.abspath(os.path.normpath(settings_file))
                self._settings = YamlConf(settings_file).cfg
                self._chimera_config = self._settings.get("CHIMERA", None)
                if self._wuid and self._job_num:
                    if not self._chimera_config:
                        raise RuntimeError("Must specify a CHIMERA area in {}".format(settings_file))
        except Exception as e:
            if settings_file:
                file_name = settings_file
            else:
                file_name = '~/verdi/etc/settings.yaml'
            raise RuntimeError("Could not read settings file '{}': {}".format(file_name, e))

        self._run_config = run_config

    def get_input_file_name(self, input_file_key=None):
        """
        Function to grab the primary input file name out of the run config
        :param input_file_key: JSON key in runconfig containing the primary input
        value
        :return:
        """

        input_products = self._run_config.get(chimera_const.RC_INPUT).get(input_file_key, None)
        if input_products is None:
            return None
        if isinstance(input_products, list):
            input_file = [os.path.basename(path) for path in input_products
                          if not path.endswith(".XFR")]
            files = "-".join(input_file)
        else:
            input_file = os.path.basename(input_products)
            files = input_file
        return files

    def get_localize_urls(self, localize):
        """
        create the list of products to be localized within the docker for the PGE
        run
        :param localize: list of urls to be localized
        :return: localize list that osaka understands
        """
        localize_list = []

        for url in localize:
            element = {"url": url, "path": "input/"}
            localize_list.append(element)

        return localize_list

    def construct_params(self):
        """
        Construct the params for the PGE job submission

        :return:
        """
        try:
            localize_urls = self.get_localize_urls(self._run_config.get(chimera_const.LOCALIZE))
        except Exception:
            raise ValueError(
                "Couldn't find {} in runconfig from input preprocessor".format(
                    chimera_const.LOCALIZE))

        job_params = {
            "run_config": self._run_config,
            "pge_config": self._pge_config,
            "localize_urls": localize_urls,
            "simulate_outputs": self._run_config[chimera_const.SIMULATE_OUTPUTS]
        }

        return job_params

    def get_payload_hash(self, job_type):
        """
        Can be overwritten to calculate the payload hash to determine dedup. By returning None, we will use HySDS
        Core's hash calculation to determine dedup.

        :param job_type:

        :return:
        """
        return None

    def perform_adaptation_tasks(self, job_json):
        """
        Can be used to perform additional tasks prior to job submission.

        :param job_json:
        :return:
        """
        return job_json

    def construct_job_payload(self, params=None, dataset_id=None, pge_config=None, job_type=None, job_queue=None,
                              payload_hash=None):
        """
        Uses resolve hysds job to get the job json
        :param params:
        :param dataset_id:
        :param pge_config:
        :param job_type:
        :param job_queue:
        :param payload_hash:
        :return:
        """

        if dataset_id is not None:
            job_name = job_type + "_" + pge_config["pge_name"] + "_" + dataset_id
        else:
            job_name = job_type + "_" + pge_config["pge_name"]

        try:
            if dataset_id is not None:
                tags = [pge_config["pge_name"], dataset_id]
            else:
                tags = [pge_config["pge_name"]]
            job = resolve_hysds_job(job_type, job_queue,
                                    params=params, job_name=job_name, enable_dedup=True, tags=tags,
                                    payload_hash=payload_hash)
        except Exception as e:
            raise Exception(e)
        except:
            raise RuntimeError("Wasn't able to get Job JSON from resolve_hysds_job.")

        print(json.dumps(job, sort_keys=True, indent=4, separators=(',', ': ')))
        return job

    def submit_job(self):
        if not isinstance(self._run_config, dict):
            raise RuntimeError("The output from input preprocessor is not a dictionary")

        params, localize_hash = self.construct_params()

        # If wuid and job_num are not null, it is implied that we need to do job submission. In that case, we need to
        # construct the job payload.
        if self._wuid and self._job_num:
            # get HySDS job type and queue information
            job_name = self._chimera_config.get(chimera_const.JOB_TYPES).get(
                self._pge_config.get(chimera_const.PGE_NAME))
            job_queue = self._chimera_config.get(chimera_const.JOB_QUEUES).get(
                self._pge_config.get(chimera_const.PGE_NAME))

            if chimera_const.RELEASE_VERSION in self._context:
                release_version = self._context[chimera_const.RELEASE_VERSION]
            else:
                release_version = self._context.get('container_specification').get('version')

            job_type = job_name + ":" + release_version

            localize_hash = self.get_payload_hash(job_type)

            # Find what the primary input is to the job
            # input_file_key = self._pge_config.get(chimera_const.PRIMARY_INPUT, None)
            # dataset_id = self.get_input_file_name(input_file_key)

            # Nominally, the primary input is used as part of the job name. If we wanted to set something else in the
            # job
            # name, look to see if the pge_job_name field is specified in the run_config
            dataset_id = self._run_config.get("pge_job_name", None)

            if dataset_id:
                logger.info("dataset_id is set to {}".format(dataset_id))

            job_json = self.construct_job_payload(params, dataset_id=dataset_id, pge_config=self._pge_config,
                                                  job_type=job_type, job_queue=job_queue, payload_hash=localize_hash)
            # Set the sciflo fields wuid and job num
            # these are internally passed context information available in sciflo processes
            job_json['payload']['_sciflo_wuid'] = self._wuid
            job_json['payload']['_sciflo_job_num'] = self._job_num

            logger.debug("Resolved Job JSON: {}".format(json.dumps(job_json)))
        else:
            # If we're running inline, we will set the params as the job_json
            job_json = params
            # We also need to get the job_specification from _context.json as that contains dependency image
            # information, if specified
            if "job_specification" in self._context:
                job_json["job_specification"] = self._context["job_specification"]

        job_json = self.perform_adaptation_tasks(job_json)

        return job_json
