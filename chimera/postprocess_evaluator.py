import json
import os
import traceback

from importlib import import_module

from chimera.logger import logger
from chimera.commons.conf_util import YamlConf, load_config
from chimera.commons.constants import ChimeraConstants
from chimera.postprocess_functions import PostProcessFunctions
from hysds.celery import app
import elasticsearch
import time

from urllib.parse import urlparse

# Used to identify fields to be filled within the runconfig context.json of PGE
EMPTY_FIELD_IDENTIFIER = None
MOZART_ES_ENDPOINT = "MOZART"
GRQ_ES_ENDPOINT = "GRQ"
STATUS_INDEX = None
STATUS_TYPE = None

JOBS_ES_URL = app.conf["JOBS_ES_URL"]
JOBS_ES = elasticsearch.Elasticsearch(JOBS_ES_URL)
GRQ_ES_URL = app.conf["GRQ_ES_URL"]
GRQ_ES = elasticsearch.Elasticsearch(GRQ_ES_URL)

REDIS_URL = app.conf.REDIS_INSTANCE_METRICS_URL
REDIS_KEY = app.conf.REDIS_INSTANCE_METRICS_KEY


class PostProcessor(object):
    def __init__(
        self,
        sf_context,
        chimera_config_filepath,
        pge_config_filepath,
        settings_file,
        job_result,
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

    def query_es(
        self,
        endpoint,
        doc_id=None,
        query=None,
        request_timeout=30,
        retried=False,
        size=1,
    ):
        """
        This function queries ES. Not using the query util because the ES
        connection is set
        for the GRQ ES.
        :param endpoint: the value specifies which ES endpoint to send query
         can be MOZART or GRQ
        :param doc_id: id of product or job
        :param query: query to run
        :param request_timeout: how long to wait for ES request
        :param retried: flag to specify if the query has already been retried
        :param size: number of results to be returned
        :return: result of query
        """
        result = None
        if query is None and doc_id is None:
            raise ValueError("Both doc_id and query cannot be None")

        es, es_url, es_index = None, None, None
        if endpoint == GRQ_ES_ENDPOINT:
            es_index = "grq"
            es = GRQ_ES
        if endpoint == MOZART_ES_ENDPOINT:
            es_index = "job_status-current"
            es = JOBS_ES

        if doc_id is not None:
            query = {"query": {"bool": {"must": [{"term": {"_id": doc_id}}]}}}

        try:
            result = es.search(
                index=es_index, body=query, size=size, request_timeout=request_timeout
            )
            # retry in case of time out
            if "timed_out" in result and result.get("timed_out"):
                logger.warning(
                    "ES responded with a timed out result, "
                    "retrying....: {}".format(json.dumps(result))
                )
                raise RuntimeWarning(
                    "ES responded with a timed out result, retrying...."
                )
        except Exception as e:
            logger.warning(
                "Caught exception from elasticsearch "
                "retrying: {}".format(traceback.format_exc())
            )
            # Retry querying, this is incase ES takes too long to respond
            if not retried:
                self.query_es(
                    endpoint=endpoint,
                    doc_id=doc_id,
                    size=size,
                    request_timeout=int(request_timeout + 30),
                    retried=True,
                )
            else:
                raise Exception(str(e))

        return result

    def product_in_grq(self, doc_id):
        """
        Checks if the product has been indexed in ES
        :param doc_id:
        :return: True if product found else throw suitable exception
        """
        query = {
            "_source": ["id"],
            "query": {"bool": {"must": [{"term": {"_id": doc_id}}]}},
        }

        try:
            if self.wait_for_doc(endpoint=GRQ_ES_ENDPOINT, query=query, timeout=120):
                return True
        except Exception as ex:
            logger.error(
                "Error querying GRQ for product {}. {}. {}".format(
                    doc_id, str(ex), traceback.format_exc()
                )
            )
            raise Exception(
                "Error querying GRQ for product {}. {}".format(doc_id, str(ex))
            )

    def wait_for_doc(self, endpoint, query, timeout):
        """
        This function executes the search query for specified wait time until
        document is found
        :param endpoint: GRQ or MOZART
        :param query: search query
        :param timeout: time to wait in seconds
        :return: True if document found else raise suitable Exception
        """
        try:
            result = self.query_es(
                endpoint=endpoint, query=query, request_timeout=30, size=1
            )
            slept_seconds = 0
            sleep_seconds = 2

            while self.wait_condition(endpoint=endpoint, result=result):
                if result.get("timed_out", True):
                    slept_seconds += 30

                if slept_seconds + sleep_seconds < timeout:
                    logger.debug("Slept for {} seconds".format(slept_seconds))
                    logger.debug("Sleeping for {} seconds".format(sleep_seconds))
                else:
                    sleep_seconds = timeout - slept_seconds
                    logger.debug("Slept for {} seconds".format(slept_seconds))
                    logger.debug(
                        "Sleeping for {} seconds to conform to timeout "
                        "of {} seconds".format(sleep_seconds, timeout)
                    )

                if slept_seconds >= timeout:
                    if len(result.get("hits").get("hits")) == 0:
                        raise Exception(
                            "{} ES taking too long to index document".format(endpoint)
                        )
                    if endpoint == MOZART_ES_ENDPOINT:
                        if (
                            str(result["hits"]["hits"][0]["_source"]["status"])
                            == "job-started"
                        ):
                            raise Exception(
                                "{} ES taking too long to update status of "
                                "job".format(endpoint)
                            )

                time.sleep(sleep_seconds)
                result = self.query_es(
                    endpoint=endpoint, query=query, request_timeout=30, size=1
                )
                slept_seconds += sleep_seconds
                sleep_seconds *= 2
            return True
        except Exception as e:
            raise Exception("ElasticSearch Operation failed due to : {}".format(str(e)))

    def get_product_info(self, product_id):
        """
            This function gets the product's URL and associated metadata from Elastic
            Search
            :param product_id: id of product
            :return: tuple(product_url, metadata)
            """
        response = None
        try:
            if self.product_in_grq(doc_id=product_id):
                try:
                    response = self.query_es(
                        endpoint=GRQ_ES_ENDPOINT, doc_id=product_id
                    )
                    if len(response.get("hits").get("hits")) == 0:
                        raise Exception(
                            "ES taking too long to index product with id "
                            "%s." % product_id
                        )
                except Exception as ex:
                    raise Exception(
                        "ElasticSearch Operation failed due to : {}".format(str(ex))
                    )
        except Exception as ex:
            raise Exception(
                "Failed to find product in GRQ. {}. {}".format(
                    str(ex), traceback.format_exc()
                )
            )

        try:
            result = response.get("hits").get("hits")[0]
            product_urls = result.get("_source").get("urls")
            product_url = None
            for url in product_urls:
                if url.startswith("s3://"):
                    product_url = url
            metadata = result.get("_source").get("metadata")
        except Exception as ex:
            raise Exception(
                "Failed to get product info. {}. {}".format(
                    str(ex), traceback.format_exc()
                )
            )

        return product_url, metadata

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
                self._sf_context, self._pge_config, self._settings, new_context
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
