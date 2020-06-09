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

JOBS_ES_URL = app.conf['JOBS_ES_URL']
JOBS_ES = elasticsearch.Elasticsearch(JOBS_ES_URL)
GRQ_ES_URL = app.conf["GRQ_ES_URL"]
GRQ_ES = elasticsearch.Elasticsearch(GRQ_ES_URL)

REDIS_URL = app.conf.REDIS_INSTANCE_METRICS_URL
REDIS_KEY = app.conf.REDIS_INSTANCE_METRICS_KEY


class PostProcessor(object):

    def __init__(self, sf_context, chimera_config_filepath, pge_config_filepath,  settings_file, job_result):
        # load context file
        if isinstance(sf_context, dict):
            self._sf_context = sf_context
        elif isinstance(sf_context, str):
            self._sf_context = json.load(open(sf_context, 'r'))
        logger.debug("Loaded context file: {}".format(json.dumps(self._sf_context)))

        # load pge config file
        self._pge_config = load_config(pge_config_filepath)
        logger.debug("Loaded PGE config file: {}".format(json.dumps(self._pge_config)))

        # load PP config file
        try:
            self._chimera_config = YamlConf(chimera_config_filepath).cfg
            self._module_path = self._chimera_config.get("postprocessor", {}).get("module_path", None)
            if not self._module_path:
                raise RuntimeError("'module_path' must be defined in the 'preprocessor' section of the "
                                   "Chimera Config file '{}'".format(chimera_config_filepath))
            self._class_name = self._chimera_config.get("postprocessor", {}).get("class_name", None)
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

        # load PGE job result
        if isinstance(job_result, dict):
            self._job_result = job_result
        elif isinstance(job_result, str):
            self._job_result = json.load(open(job_result, 'r'))
        logger.debug("Loaded job result: {}".format(json.dumps(self._job_result)))

    def prepare_psuedo_context(self, psuedo_context):
        """
        Write the gathered job and product metadata information to the psuedo context file.
        :return: dict
        """
        logger.debug("Preparing psuedo_context file after {} run".format(self._pge_config.get('pge_name')))
        # write out job context
        psu_context = open("{}_context.json".format(self._pge_config.get('pge_name'), "w"))
        psu_context.write(json.dumps(psuedo_context))
        psu_context.close()
        return "{}_context.json".format(self._pge_config.get('pge_name'), "w")

    def check_job_status(self):
        """
        Check if job is completed or deduped. If any other status then raise an error.
        :return:
        """
        # getting the job paylooad and status
        job_id = str(self.job_result["payload_id"])
        job_status = str(self.job_result["status"])

        logger.info("Recieved JOB ID: {} with status: {}".format(
            job_id, job_status))

        if job_status != "job-completed" and job_status != "job-deduped":
            logger.info("Job with job_id: {} was not completed. Status: {}".format(
                job_id, job_status))
            raise ValueError(
                "Job with job_id: {} was not completed. Status: {}".format(
                    job_id, job_status))
        return job_status

    def query_es(self, endpoint, doc_id=None, query=None, request_timeout=30,
                 retried=False, size=1):
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
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"_id": doc_id}}
                        ]
                    }
                }
            }

        try:
            result = es.search(index=es_index, body=query,
                               size=size, request_timeout=request_timeout)
            # retry in case of time out
            if "timed_out" in result and result.get("timed_out"):
                logger.warning("ES responded with a timed out result, "
                               "retrying....: {}".format(json.dumps(result)))
                raise RuntimeWarning(
                    "ES responded with a timed out result, retrying....")
        except Exception as e:
            logger.warning("Caught exception from elasticsearch "
                           "retrying: {}".format(traceback.format_exc()))
            # Retry querying, this is incase ES takes too long to respond
            if not retried:
                self.query_es(endpoint=endpoint, doc_id=doc_id, size=size,
                         request_timeout=int(request_timeout + 30), retried=True)
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
            "_source": [
                "id"
            ],
            "query": {
                "bool": {
                    "must": [
                        {"term": {"_id": doc_id}}
                    ]
                }
            }
        }

        try:
            if self.wait_for_doc(endpoint=GRQ_ES_ENDPOINT, query=query, timeout=120):
                return True
        except Exception as ex:
            logger.error("Error querying GRQ for product {}. {}. {}".format(
                doc_id, str(ex), traceback.format_exc()))
            raise Exception(
                "Error querying GRQ for product {}. {}".format(doc_id, str(ex)))

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
            result = self.query_es(endpoint=endpoint, query=query,
                              request_timeout=30, size=1)
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
                    logger.debug("Sleeping for {} seconds to conform to timeout "
                                 "of {} seconds".format(sleep_seconds, timeout))

                if slept_seconds >= timeout:
                    if len(result.get("hits").get("hits")) == 0:
                        raise Exception(
                            "{} ES taking too long to index document".format(
                                endpoint))
                    if endpoint == MOZART_ES_ENDPOINT:
                        if str(result["hits"]["hits"][0]["_source"]["status"]) == \
                                "job-started":
                            raise Exception(
                                "{} ES taking too long to update status of "
                                "job".format(endpoint))

                time.sleep(sleep_seconds)
                result = self.query_es(endpoint=endpoint, query=query,
                                  request_timeout=30, size=1)
                slept_seconds += sleep_seconds
                sleep_seconds *= 2
            return True
        except Exception as e:
            raise Exception(
                "ElasticSearch Operation failed due to : {}".format(str(e)))

    def get_job(self):
        """
        This function gets the staged products and context of previous PGE job
        :return: tuple(products_staged, prev_context, message)
        """
        job_id = str(self.job_result["payload_id"])
        endpoint = MOZART_ES_ENDPOINT
        return_job_id = None

        """
        Check if Jobs ES has updated job status and gets job information if completed/ deduped
        """
        try:
            if self.check_job_status():
                try:
                    response = self.query_es(endpoint=endpoint, doc_id=job_id)
                    # check if job not found
                    if len(response["hits"]["hits"]) == 0:
                        raise Exception(
                            "Couldn't find record with ID in MOZART: %s, at %s" % (
                                job_id, endpoint))
                except Exception as ex:
                    logger.error("Error querying MOZART for doc {}. {}. {}"
                                 .format(job_id, str(ex),
                                         traceback.format_exc()))
                    raise Exception(
                        "Error querying MOZART for doc {}. {}".format(job_id,
                                                                      str(ex)))
        except Exception as ex:
            logger.error("Failed to find job in MOZART. {}. {}. {}"
                         .format(job_id, str(ex), traceback.format_exc()))
            raise Exception("Failed to find job in MOZART. {}. {}. {}"
                            .format(job_id, str(ex), traceback.format_exc()))

        """
        Parse job's full information to get products staged, job context
        If job deduped then find original job's information
        """
        result = response["hits"]["hits"][0]
        products_staged = None
        prev_context = None
        message = None  # using this to store information regarding deduped jobs,
        status = str(result["_source"]["status"])

        # if job was deduped then find the original job status and what products (if any) were created
        if status == "job-deduped":
            logger.info("Job was deduped")
            # query ES for the original job's status
            orig_job_id = result["_source"]["dedup_job"]
            return_job_id = orig_job_id
            try:
                orig_job_info = self.query_es(endpoint=endpoint, doc_id=orig_job_id)
                if len(response["hits"]["hits"]) == 0:
                    raise Exception(
                        "Couldn't find record with ID: {}, at {}".format(job_id,
                                                                         endpoint))
            except Exception as ex:
                logger.error("Error querying ES for doc {}. {}. {}".format(
                    job_id, str(ex), traceback.format_exc()))
                raise Exception(
                    "Error querying ES for doc {}. {}".format(job_id, str(ex)))

            """
            check if original job failed -> this would happen when at the moment
            of deduplication, the original job was in 'running state', but soon
            afterwards failed. So, by the time the status is checked in this
            function, it may be shown as failed.
            """

            orig_job_info = orig_job_info["hits"]["hits"][0]
            orig_job_status = str(orig_job_info["_source"]["status"])
            if orig_job_status == "job-failed":
                message = "Job was deduped against a failed job with id: {}," \
                          " please retry sciflo.".format(orig_job_id)
                logger.info("Job was deduped against a job which has now failed "
                            "with id: {}, Please retry sciflo.".format(orig_job_id)
                            )
            elif orig_job_status == "job-started" or orig_job_status == \
                    "job-queued":
                logger.info("Job was deduped against a queued/started job with "
                            "id: {}. Please look at already running sciflo with "
                            "same params.".format(orig_job_id))
                message = "Job was deduped against a queued/started job with " \
                          "id: {}. Please look at already running sciflo with " \
                          "same params.".format(orig_job_id)

            elif orig_job_status == "job-completed":
                products_staged = orig_job_info["_source"]["job"]["job_info"][
                    "metrics"]["products_staged"]
                prev_context = orig_job_info["_source"]["context"]
                logger.info("Queried ES to get Job context and staged files info")
                message = "success"
        elif status == "job-completed":
            logger.info("Job completed")
            products_staged = result["_source"]["job"]["job_info"]["metrics"][
                "products_staged"]
            prev_context = result["_source"]["context"]
            logger.info("Queried ES to get Job context and staged files info")
            message = "success"
            return_job_id = job_id
        else:
            logger.info("Job was not completed. Status: {}".format(
                result["_source"]["status"]))
            message = "Job was not completed. Status: {}".format(
                result["_source"]["status"])

        return products_staged, prev_context, message, return_job_id

    def create_products_list(self, products):
        """
            This function creates a list of the product URLs and metadata required
            for the next PGE's input preprocessor.
            :param products: list of products staged after PGE run
            :return: tuple( product's id, list of products' URLs, list of products'
            metadata)
        """
        product_id = None
        products_url_list = []
        products_metadata_list = []

        for product in products:
            input_product_id = product["id"]
            # get information required for next PGE's input preprocessor
            product_id = input_product_id

            try:
                product_url, metadata = self.get_product_info(product_id=input_product_id)
                product_info = dict()
                product_info["id"] = input_product_id
                product_info["url"] = product_url
                product_info["metadata"] = metadata
                products_metadata_list.append(product_info)
                products_url_list.append(product_url)
            except Exception as ex:
                raise Exception("Failed to get product information, {}. {}".format(
                    str(ex), traceback.format_exc()))

        return product_id, products_url_list, products_metadata_list

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
                        endpoint=GRQ_ES_ENDPOINT, doc_id=product_id)
                    if len(response.get("hits").get("hits")) == 0:
                        raise Exception(
                            "ES taking too long to index product with id "
                            "%s." % product_id)
                except Exception as ex:
                    raise Exception(
                        "ElasticSearch Operation failed due to : {}".format(
                            str(ex)))
        except Exception as ex:
            raise Exception("Failed to find product in GRQ. {}. {}".format(
                str(ex), traceback.format_exc()))

        try:
            result = response.get("hits").get("hits")[0]
            product_urls = result.get("_source").get("urls")
            product_url = None
            for url in product_urls:
                if url.startswith("s3://"):
                    product_url = url
            metadata = result.get("_source").get("metadata")
        except Exception as ex:
            raise Exception("Failed to get product info. {}. {}".format(
                str(ex), traceback.format_exc()))

        return product_url, metadata

    def required_post_process_steps(self):
        """
        The mandatory post processing steps of Chimera are:
        1. check submitted job's status
        2. Get complete job run information i.e job's context, products produced, job id
            (in case job submitted by sciflo was deduped, the original job ID is tracked down)
        3.

        :return:
        """
        pseudo_context = dict()

        """
        check submitted job's status
        """
        job_status = self.check_job_status()

        """
        get the products staged, context of job and job's ID (incase job submitted by sciflo was deduped)
        """
        try:
            products, prev_context, message, job_id = self.get_job()
        except Exception as ex:
            logger.error("Couldn't get job info for {}. {}. {}".format(
                job_id, str(ex), traceback.format_exc()))
            job_status_code = -1
            logger.error("Job was not found.")
            raise RuntimeError("Couldn't get job info for {}. {}. {}".format(
                job_id, str(ex), traceback.format_exc()))

        """
        Handle job status codes for all outcomes of a deduped job
        # Case 1: if the original job is queued or has started, fail the current sciflo
                  so that the original workflow can take care of the PGE run
                  update the job status with -3
                  
        # Case 2: if the original job has completed, then get products and prev _context from original job
                  update job_status_code to 2
        
        # Case 3: if the original job was deduped (NOTE: Unlikely unrealistic case)
                   set job_status_code to -2
        """
        # case 1
        if products is None and prev_context is None:
            job_status_code = -3
            raise RuntimeError(message)
        else:
            # case 2
            if job_status == "job-completed":
                job_status_code = 2
            # case 3
            elif job_status == "job-deduped":
                job_status_code = -2

        """
        Query to get information of all products staged.
        NOTE: Sciflo gets notification of the celery task completion and moves to the post processing step when
              it gets the PGE job submission results. The completion of a celery task is different than completion of
              a HySDS job. A HySDS job includes the celery task execution, worker post processing and dataset ingestion.
        We get to the step of querying a product's information before it has been indexed into GRQ. To handle this race
        condition we have an exponential backoff logic in the query to wait for the product to appear.
        Max wait time is 2 mins.
        """
        try:
            product_id, products_url_list, products_metadata_list = \
                self.create_products_list(products=products)
        except Exception as ex:
            job_status_code = -1
            logger.error(
                "Setting Job failure status code as product was not found.")
            raise RuntimeError("Failed PGE run as products list could not be made."
                               " {}. {}".format(str(ex), traceback.format_exc()))

        """
        Now that we have all job and products information we can put the psuedo context contents together.
        """
        logger.info("Job Status Code: {}".format(job_status_code))
        product_url_key = ChimeraConstants.PRODUCT_PATHS
        metadata_key = ChimeraConstants.PRODUCTS_METADATA

        pseudo_context[product_url_key] = products_url_list
        pseudo_context[metadata_key] = products_metadata_list
        pseudo_context["job_id"] = job_id
        pseudo_context["job_context"] = prev_context

        return pseudo_context

    def process(self):
        new_context = dict()
        try:
            module = import_module(self._module_path)
            cls = getattr(module, self._class_name)
            if not issubclass(cls, PostProcessFunctions):
                raise RuntimeError("Class must be a subclass of {}: {}".format(PostProcessFunctions.__name__,
                                                                               cls.__name__))
            # run mandatory post process funtions
            new_context.update(self.required_post_process_steps())
            # run custom post processing steps and update the psuedo context content
            cls_object = cls(self._sf_context, self._pge_config, self._settings, new_context)
            new_context.update(cls_object.run(self._pge_config.get(ChimeraConstants.POSTPROCESS, list())))
            # write to output context file
            new_context_file = self.prepare_psuedo_context(new_context)
            return new_context_file
        except Exception as e:
            logger.error("Post processor failure: {}. {}".format(e, traceback.format_exc()))
            raise RuntimeError("Post processor failure: {}".format(e))
