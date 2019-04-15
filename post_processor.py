"""
The Post Processor queries:
1. Mozart for job infor
2. GRQ for product metadata
and creates a context.json (not the same as _context.json)
"""
# !/usr/bin/env python
import os
import logging
import time
import json
import sys
from hysds.celery import app
import elasticsearch
import traceback
from commons import constants, query_util

# this python code needs to query ES with job_id
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")
LOGGER.setLevel(logging.DEBUG)
stdout = logging.StreamHandler(sys.stdout)
stdout.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout.setFormatter(formatter)
LOGGER.addHandler(stdout)

BASE_PATH = os.path.dirname(__file__)
MOZART_ES_ENDPOINT = "MOZART"
GRQ_ES_ENDPOINT = "GRQ"
STATUS_INDEX = None
STATUS_TYPE = None

JOBS_ES_URL = app.conf['JOBS_ES_URL']
JOBS_ES = elasticsearch.Elasticsearch(JOBS_ES_URL)
GRQ_ES_URL = app.conf["GRQ_ES_URL"]
GRQ_ES = elasticsearch.Elasticsearch(GRQ_ES_URL)


def query_es(endpoint, doc_id=None, query=None, request_timeout=30, retried=False, size=1):
    """
    This function queries ES. Not using the query util because the ES connection is set
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
        result = es.search(index=es_index, body=query, size=size, request_timeout=request_timeout)
        # retry in case of time out
        if result.get("timed_out", True):
            LOGGER.warning("ES responded with a timed out result, retrying....: {}".format(json.dumps(result)))
            raise RuntimeWarning("ES responded with a timed out result, retrying....")
    except Exception as e:
        LOGGER.warning("Caught exception from elasticsearch retrying: {}".format(traceback.format_exc()))
        # Retry querying, this is incase ES takes too long to respond
        if not retried:
            query_es(endpoint=endpoint, doc_id=doc_id, size=size,
                     request_timeout=int(request_timeout+30), retried=True)
        else:
            raise Exception(str(e))

    return result


def wait_condition(endpoint, result):
    results_exist = len(result.get("hits").get("hits")) == 0
    if endpoint == MOZART_ES_ENDPOINT:
        return results_exist or str(result.get("hits").get("hits")[0].get("_source").get("status")) == "job-started"
    if endpoint == GRQ_ES_ENDPOINT:
        return results_exist


def wait_for_doc(endpoint, query, timeout):
    """
    This function executes the search query for specified wait time until document is found
    :param endpoint: GRQ or MOZART
    :param query: search query
    :param timeout: time to wait in seconds
    :return: True if document found else raise suitable Exception
    """
    try:
        result = query_es(endpoint=endpoint, query=query, request_timeout=30, size=1)
        slept_seconds = 0
        sleep_seconds = 2

        while wait_condition(endpoint=endpoint, result=result):
            if result.get("timed_out", True):
                slept_seconds += 30

            if slept_seconds + sleep_seconds < timeout:
                LOGGER.debug("Slept for {} seconds".format(slept_seconds))
                LOGGER.debug("Sleeping for {} seconds".format(sleep_seconds))
            else:
                sleep_seconds = timeout - slept_seconds
                LOGGER.debug("Slept for {} seconds".format(slept_seconds))
                LOGGER.debug("Sleeping for {} seconds to conform to timeout of {} seconds".format(sleep_seconds,
                                                                                                  timeout))

            if slept_seconds >= timeout:
                if len(result.get("hits").get("hits")) == 0:
                    raise Exception("{} ES taking too long to index document".format(endpoint))
                if endpoint == MOZART_ES_ENDPOINT:
                    if str(result["hits"]["hits"][0]["_source"]["status"]) == "job-started":
                        raise Exception("{} ES taking too long to update status of job".format(endpoint))

            time.sleep(sleep_seconds)
            result = query_es(endpoint=endpoint, query=query, request_timeout=30, size=1)
            slept_seconds += sleep_seconds
            sleep_seconds *= 2
        return True
    except Exception as es:
        raise Exception("ElasticSearch Operation failed due to : {}".format(es.message))


def check_job_status(doc_id):
    """
    There is a latency in the update of ES job status after
    celery signals job completion.
    To handle that case, we much poll ES (after sciflo returns status after blocking)
    until the job status is correctly reflected.
    :param doc_id: ID of the Job ES doc
    :return: True  if the ES has updated job status within 5 minutes
            otherwise raise a run time error
    """
    query = {
        "_source": [
                   "status"
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
        if wait_for_doc(endpoint=MOZART_ES_ENDPOINT, query=query, timeout=300):
            return True
    except Exception as ex:
        LOGGER.error("Error querying MOZART for job status of job {}. {}. {}"
                     .format(doc_id, ex.message, traceback.format_exc()))
        raise Exception("Error querying MOZART for job status of job {}. {}".format(doc_id, ex.message))


def get_job(job_id):
    """
    This function gets the staged products and context of previous PGE job
    :param job_id: this is the id of the job on mozart
    :return: tuple(products_staged, prev_context, message)
    the message refects the
    """
    endpoint = MOZART_ES_ENDPOINT
    return_job_id = None

    # check if Jobs ES has updated job status
    try:
        if check_job_status(job_id):
            try:
                response = query_es(endpoint, job_id)
                if len(response["hits"]["hits"]) == 0:
                    raise Exception("Couldn't find record with ID in MOZART: %s, at %s" % (job_id, endpoint))
            except Exception as ex:
                LOGGER.error("Error querying MOZART for doc {}. {}. {}"
                             .format(job_id, ex.message, traceback.format_exc()))
                raise Exception("Error querying MOZART for doc {}. {}".format(job_id, ex.message))
    except Exception as ex:
        LOGGER.error("Failed to find job in MOZART. {}. {}. {}"
                             .format(job_id, ex.message, traceback.format_exc()))
        raise Exception("Failed to find job in MOZART. {}. {}. {}"
                             .format(job_id, ex.message, traceback.format_exc()))

    result = response["hits"]["hits"][0]
    products_staged = None
    prev_context = None
    message = None  # using this to store information regarding deduped jobs, used later to as
    # error message unless it's value is "success"

    status = str(result["_source"]["status"])
    if status == "job-deduped":
        LOGGER.info("Job was deduped")
        # query ES for the original job's status
        orig_job_id = result["_source"]["dedup_job"]
        return_job_id = orig_job_id
        try:
            orig_job_info = query_es(endpoint, orig_job_id)
            if len(response["hits"]["hits"]) == 0:
                raise Exception("Couldn't find record with ID: {}, at {}".format(job_id, endpoint))
        except Exception as ex:
            LOGGER.error("Error querying ES for doc {}. {}. {}".format(job_id, ex.message, traceback.format_exc()))
            raise Exception("Error querying ES for doc {}. {}".format(job_id, ex.message))

        """check if original job failed -> this would happen when at the moment of deduplication, the original job
         was in 'running state', but soon afterwards failed. So, by the time the status is checked in this function,
         it may be shown as failed."""
        orig_job_info = orig_job_info["hits"]["hits"][0]
        orig_job_status = str(orig_job_info["_source"]["status"])
        if orig_job_status == "job-failed":
            message = "Job was deduped against a failed job with id: {}," \
                      " please retry sciflo.".format(orig_job_id)
            LOGGER.info("Job was deduped against a job which has now failed with id: {}, Please retry sciflo."
                        .format(orig_job_id))
        elif orig_job_status == "job-started" or orig_job_status == "job-queued":
            LOGGER.info("Job was deduped against a queued/started job with id: {}. "
                        "Please look at already running sciflo with same params."
                        .format(orig_job_id))
            message = "Job was deduped against a queued/started job with id: {}. " \
                      "Please look at already running sciflo with same params.".format(orig_job_id)

        elif orig_job_status == "job-completed":
            products_staged = orig_job_info["_source"]["job"]["job_info"]["metrics"]["products_staged"]
            prev_context = orig_job_info["_source"]["context"]
            LOGGER.info("Queried ES to get Job context and staged files info")
            message = "success"
    elif status == "job-completed":
        LOGGER.info("Job completed")
        products_staged = result["_source"]["job"]["job_info"]["metrics"]["products_staged"]
        prev_context = result["_source"]["context"]
        LOGGER.info("Queried ES to get Job context and staged files info")
        message = "success"
        return_job_id = job_id
    else:
        LOGGER.info("Job was not completed. Status: {}".format(result["_source"]["status"]))
        message = "Job was not completed. Status: {}".format(result["_source"]["status"])

    return products_staged, prev_context, message, return_job_id


def product_in_grq(doc_id):
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
        if wait_for_doc(endpoint=GRQ_ES_ENDPOINT, query=query, timeout=120):
            return True
    except Exception as ex:
        LOGGER.error("Error querying GRQ for product {}. {}. {}".format(doc_id, ex.message, traceback.format_exc()))
        raise Exception("Error querying GRQ for product {}. {}".format(doc_id, ex.message))


def get_product_info(product_id):
    """
    This function gets the product's URL and associated metadata from Elastic Search
    :param product_id: id of product
    :return: tuple(product_url, metadata)
    """
    response = None
    try:
        if product_in_grq(product_id):
            try:
                response = query_es(endpoint=GRQ_ES_ENDPOINT, doc_id=product_id)
                if len(response.get("hits").get("hits")) == 0:
                    raise Exception("ES taking too long to index product with id %s." % product_id)
            except Exception as ex:
                raise Exception("ElasticSearch Operation failed due to : {}".format(ex.message))
    except Exception as ex:
        raise Exception("Failed to find product in GRQ. {}. {}".format(ex.message, traceback.format_exc()))

    try:
        result = response.get("hits").get("hits")[0]
        product_urls = result.get("_source").get("urls")
        product_url = None
        for url in product_urls:
            if url.startswith("s3://"):
                product_url = url
        metadata = result.get("_source").get("metadata")
    except Exception as ex:
        raise Exception("Failed to get product info. {}. {}".format(ex.message, traceback.format_exc()))

    return product_url, metadata


def create_products_list(products, input_file_extensions):
    """
    tThis function creates a list of the product URLs and metadata required for the next PGE's input preprocessor.
    :param products: list of products staged after PGE run
    :param input_file_extensions: list of scientific product file extensions specific to PGE
    :return: tuple( product's id, list of products' URLs, list of products' metadata)
    """
    product_id = None
    products_url_list = []
    products_metadata_list = []

    for product in products:
        input_product_id = product["id"]
        product_extension = input_product_id[input_product_id.rfind('.'):]
        # check if file staged is a scientific product expected to be produced by PGE
        # doing this by matching against expected file extension
        if product_extension in input_file_extensions:
            # get information required for next PGE's input preprocessor
            product_id = input_product_id

            try:
                product_url, metadata = get_product_info(input_product_id)
                product_info = dict()
                product_info["id"] = input_product_id
                product_info["url"] = product_url
                product_info["metadata"] = metadata
                products_metadata_list.append(product_info)
                products_url_list.append(product_url)
            except Exception as ex:
                raise Exception("Failed to get product information, {}. {}".format(ex.message, traceback.format_exc()))

    return product_id, products_url_list, products_metadata_list


def create_context(sf_context, job_result, pge_type, pge_config_file, test_mode=False):
    """
    The main task of the post processor is
    to create a file [PGE_type]_context.json.
    The file's purpose is to pass metadata of
    the previous smap_sciflo process (PGE run) to
    the next one's input preprocessor.
    product produced and the job status of the PGE run.

    JOB Status Codes:
    -3 -> job deduped against a failed, queued/updated job
    -2 -> job deduped against a completed job
    -1 -> failed (handled at commoms.sciflo_util)
    0 -> never ran (default value in document)
    1 -> running (set in run_pge_docker.py)
    2 -> completed successfully
    Parameters:
    @job_result - job_id of the PGE run
    @pge_type - type of SMAP PGE run
    @pge_config_file - path of the config file of specific PGE type
    """
    # getting the job paylooad and status
    job_id = str(job_result["payload_id"])
    job_status = str(job_result["status"])
    job_status_code = None

    LOGGER.info("Recieved JOB ID: {} with status: {}".format(job_id, job_status))

    if job_status != "job-completed" and job_status != "job-deduped":
        LOGGER.info("Job with job_id: {} was not completed. Status: {}".format(job_id, job_status))
        raise ValueError("Job with job_id: {} was not completed. Status: {}".format(job_id, job_status))

    input_file_extensions = None

    config_file = open(pge_config_file, 'r')
    pge_config = json.loads(config_file.read())
    if "output_file_extension" in pge_config:
        input_file_extensions = pge_config["output_file_extension"]
    config_file.close()

    try:
        products, prev_context, message, job_id = get_job(job_id)
    except Exception as ex:
        LOGGER.error("Couldn't get job info for {}. {}. {}".format(job_id, ex.message, traceback.format_exc()))
        job_status_code = -1
        LOGGER.error("Job was not found.")
        raise RuntimeError("Couldn't get job info for {}. {}. {}".format(job_id, ex.message, traceback.format_exc()))

    # this is being done to handle all outcomes of a deduped job
    if products is None and prev_context is None:
        # if the original job is queued or has started, fail sciflo and update the job status with -3,
        # let original workflow take care of it
        job_status_code = -3
        raise RuntimeError(message)
    else:
        # if the original job has completed, then grabbing products and prev_context from original job
        if job_status == "job-completed":
            job_status_code = 2
        elif job_status == "job-deduped":
            job_status_code = -2

    try:
        product_id, products_url_list, products_metadata_list = \
            create_products_list(products=products, input_file_extensions=input_file_extensions)
    except Exception as ex:
        job_status_code = -1
        LOGGER.error("Setting Job failure status code as product was not found.")
        raise RuntimeError("Failed PGE run as products list could not be made. {}. {}".format(ex.message,
                                                                                              traceback.format_exc()))

    LOGGER.info("Job Status Code: {}".format(job_status_code))
    product_url_key = constants.PRODUCT_PATHS_KEY
    metadata_key = constants.PRODUCTS_METADATA_KEY

    job_json = dict()
    job_json[product_url_key] = products_url_list
    job_json[metadata_key] = products_metadata_list
    job_json["job_id"] = job_id
    job_json["job_context"] = prev_context

    # write out job context
    job_context = open("%s_context.json" % pge_type, "w")
    job_context.write(json.dumps(job_json))
    job_context.close()

    LOGGER.info("Newly created Job Context: \n {}".format(json.dumps(job_json)))

    return "%s_context.json" % pge_type
