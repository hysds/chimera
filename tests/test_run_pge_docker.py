import json
import os
from smap_sciflo import run_pge_docker

if __name__ == '__main__':
    """
    This is for testing of PGE Job Submission JSON
    Comment out from hysds.celery import app in query_util.py
    Comment out from hysds_commons.job_utils import resolve_hysds_job in run_pge_docker.py

    In commons/query_util.py, overwrite GRQ URL with:
    ES_URL = "http://127.0.0.1:9300"

    In run_pge_docker.construct_job_payload()
    make the following change:
    if test_mode is False:
        #job = resolve_hysds_job(job_type, job_queue, params=params, job_name= job_name, enable_dedup= True, tags=tags, payload_hash= payload_hash)
        job = {}

    Update the following sample files:
    sf_context: should be the sciflo context of an actual workflow run
    runconfig: output from the input preprocessor
    job_json: this is optional, it is used to determine if the workflow has been retried.
            This is currently only used at step PGE_L0B_Radiometer.
            It should be the _job.json of an actual workflow run

    If not testing for an L0B run please update:
    pge_config_file: path to PGE's config file

    run this script
    """

    # Testing L0B PGE job submission
    os.path.dirname(os.path.realpath(__file__))
    sf_context = os.path.dirname(os.path.realpath(__file__))+"/test-files/sf_context.json"
    runconfig = json.loads(open(os.path.dirname(os.path.realpath(__file__))+"/test-files/runconfig.json","r").read())
    pge_config_file = os.path.abspath(os.path.join(os.path.realpath(__file__),"../..","configs/examples/PGE_L0A_RADIOMETER.json"))
    sys_config_file = os.path.abspath(
        os.path.join(os.path.realpath(__file__), "../..", "configs/sys.config.json"))
    job_json = os.path.dirname(os.path.realpath(__file__))+"/test-files/job.json"

    print json.dumps(run_pge_docker.submit_pge_job(sf_context, runconfig, pge_config_file, sys_config_file, sf_job_json=job_json, wuid="1213", job_num="231232", test_mode=True))