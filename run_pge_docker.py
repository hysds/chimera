#!/usr/bin/env python
import json
import os
import logging
import uuid  # only need this import to simulate returned mozart job id
import commons
import copy, hashlib
from commons import constants, query_util
from datetime import datetime
from hysds_commons.job_utils import resolve_hysds_job

# Take PGE input job params json and construct the JOB submission jobs json

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")
'''
This is a sample mozart job payload
{
        "job_name": "%s-%s" % (job_type, l0b_lr_raw_id),
        "job_type": "job:%s" % job_type,
        "job_queue": job_queue,
        "container_mappings": container_mappings,
        "soft_time_limit": 86400,
        "time_limit": 86700,
        "payload": {
            # smap_sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job spec for dependencies
            "job_specification": {
              "digest": "sha256:3debc246c9d86f45a317ae6af4fa82ef9faf1206faf8201ed94db511468d214b", 
              "id": "container-aria-hysds_aria-pdl-clone:master", 
              "url": "s3://s3-us-west-2.amazonaws.com/grfn-v2-ops-code-bucket/container-aria-hysds_aria-pdl-clone:master.tar.gz", 
              "version": "master"
              "dependency_images": dependency_images,
            },
  
            # job params
            "context_blob": job_payload, # one param - one JSON blob

            # v2 cmd
            "_command": "/home/ops/verdi/ops/SPDM-with-HySDS/run_pge.sh",

            # disk usage
            "_disk_usage": disk_usage,

            # localize urls
            "localize_urls": localize_urls,
        }
    }
'''


def get_localize_urls(localize):
    """
    create the list of products to be localized within the docker for the PGE run
    :param localize: list of urls to be localized
    :return: localize list that osaka understands
    """
    localize_list = []

    for url in localize:
        element = {"url": url}
        localize_list.append(element)

    return localize_list


def get_payload_hash(run_config, job_type):
    clean_payload = copy.deepcopy(run_config)

    # delete the keys from clean_payload that change on every run even though runconfig is technically the same
    '''
    if "ProductionDateTime" in clean_payload["JobIdentification"]:
        del clean_payload["JobIdentification"]["ProductionDateTime"]

    if "ProductCounter" in clean_payload["ProductPathGroup"]:
        del clean_payload["ProductPathGroup"]["ProductCounter"]
    '''

    clean_payload["job_type"] = job_type
    return hashlib.md5(json.dumps(clean_payload, sort_keys=True,
                                  ensure_ascii=True)).hexdigest()


def get_input_file_name(input_products):
    if isinstance(input_products, list):
        input_file = [os.path.basename(path) for path in input_products if not path.endswith(".XFR")]
        files = "-".join(input_file)
    else:
        input_file = os.path.basename(input_products)
        files = input_file
    return files


def update_run_status(es_doc_id, run_status, run_key_name, data_value, data_key_name):
    """
    This function updates the half orbit status doc
    with the product id of dataset produced by PGE
    """
    new_doc = {}
    doc = {}

    if run_status is not None:
        doc[run_key_name] = run_status

    if data_value is not None:
        doc[data_key_name] = data_value

    doc[constants.LAST_MOD_TIME] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    new_doc["doc"] = doc
    new_doc["doc_as_upsert"] = True
    
    if es_doc_id is not None:
        result = query_util.update_doc(body=new_doc, index=None, doc_type=None, doc_id=es_doc_id)
        # print ("Updating: %s with %s"%(es_doc_id, json.dumps(new_doc)))
        logging.debug("Updating: {} with {}".format(es_doc_id, json.dumps(new_doc)))
        # print ("Result from ES update: %s"%result)
    else:
        logging.warning("Cannot update status for {} to {}. es_doc_id is None".format(run_key_name, run_status))
    return


def construct_params(run_config, job_type):
    """
    Construct the params for the PGE job submission
    :param run_config: run config produced by input preprocessor
    :param run_config:
    :param job_type:
    :return:
    """
    try:
        localize_urls = get_localize_urls(run_config[commons.constants.LOCALIZE_KEY])
    except Exception:
        raise ValueError("Couldn't find {} in runconfig from input preprocessor".format(commons.constants.LOCALIZE_KEY))
  
    job_params = {
        "runconfig": run_config,
        "product_tag": "sciflo_run",
        "localize_urls": localize_urls
    }

    localize_hash = get_payload_hash(run_config, job_type=job_type)
   
    return job_params, localize_hash


def mock_mozart(job_param_file):
    run_cmd = "./mozart_compose.sh {}".format(job_param_file)
    os.system(run_cmd)


def submit_to_mozart_backdoor(payload):
    # use mozart API to submit job
    # get and return the mozart job id??
    pass


def construct_job_payload(params=None, dataset_id=None, pge_config=None, job_type=None, job_queue=None,
                          payloash_hash=None):
    """
    Uses resolve hysds job to get the job json
    :param params:
    :param dataset_id:
    :param pge_config:
    :param job_type:
    :param job_queue:
    :param payloash_hash:
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
        job = resolve_hysds_job(job_type, job_queue, params=params, job_name=job_name, enable_dedup=True, tags=tags,
                                payload_hash=payloash_hash)
    except Exception as e:
        raise Exception(e)
    except:
        raise RuntimeError("Wasn't able to get Job JSON from resolve_hysds_job.")

    print(json.dumps(job, sort_keys=True, indent=4, separators=(',', ': ')))
    return job


def submit_pge_job(sf_context, runconfig, pge_config_file, sys_config_file, wuid=None, job_num=None):
    """
    'JOBS_ES_URL'
    This function returns the job payload that needs to be mapped by sciflo
    and run on a remote worker.
    :param sf_context: context of workflow job
    :param runconfig: Run config created by input preprocessor
    :param pge_config_file: PGE's config file name
    :param sys_config_file:
    :param wuid: wuid of sciflo
    :param job_num: job_num in sciflo
    :return: job payload of PGE job
    """

    pge_config = json.loads(open(pge_config_file, 'r').read())
    pge_type = pge_config["pge_name"]

    sys_config = json.loads(open(sys_config_file, 'r').read())
    job_name = sys_config["job_type"]
    job_queue = sys_config["job_queue"]

    ctx = open(sf_context, 'r')
    context = json.loads(ctx.read())

    if constants.RELEASE_VERSION in context:
        release_version = context[constants.RELEASE_VERSION]
    else:
        release_version = context['container_specification']['version']
    ctx.close()

    dataset_id = get_input_file_name(runconfig.get("InputFileGroup").get("InputFilePath"))

    job_type = job_name + ":" + release_version

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    if not isinstance(runconfig, dict):
        raise RuntimeError("The output from input preprocessor is not a dictionary")

    params, localize_hash = construct_params(runconfig, job_type=job_type)
    job_json = construct_job_payload(params, dataset_id=dataset_id, pge_config=pge_config, job_type=job_type,
                                     job_queue=job_queue, payloash_hash=localize_hash)
    job_json['payload']['_sciflo_wuid'] = wuid
    job_json['payload']['_sciflo_job_num'] = job_num

    return job_json


def run_simulation(job_payload, pge_config_file):
    """
    This  will simulate the expected result of running the PGEs as jobs on mozart
    :param job_payload:
    :param pge_config_file:
    :return:
    """
    print("Passing job_payload to mozart")  # will actually need to construct a json like in above commented section
    print("Mozart sends back the job id")
    job_id = uuid.uuid4().hex
    print(str(job_id))
    return job_id 
