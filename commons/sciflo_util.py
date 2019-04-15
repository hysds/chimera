#!/usr/bin/env python
import os, sys, json, re, shutil
import query_util
import constants
from datetime import datetime

WORK_RE = re.compile(r'\d{5}-.+')
ORBIT_INDEX = constants.ORBIT_INDEX
ORBIT_STATUS_INDEX = constants.ORBIT_STATUS_INDEX
ORBIT_STATUS_TYPE = constants.ORBIT_STATUS_TYPE

# sciflo PGE process names and mapping to their config files
# This is the list of PGEs that need to report status to an explict index
PGE_PROCESS_LIST = ["PGE_Name"]
CONFIG_FILES = {"PGE_Name": "/path/to/pge/config/file"
                }

def update_es(es_doc_id, prod_id, data_key_name, run_status, run_key_name, job_id_key, job_id, sciflo_run_status):
    """
    This function updates the half orbit status doc
    with the product id of dataset produced by PGE
    """
    new_doc = {}
    doc = {}
    if prod_id != None:
        doc[data_key_name] = prod_id
    if run_status != None:
        doc[run_key_name] = run_status
    if job_id != None:
        doc[job_id_key] = job_id

    doc[constants.LAST_MOD_TIME] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    doc[constants.WORKFLOW_JOB_STATUS] = sciflo_run_status
    new_doc["doc"] = doc
    new_doc["doc_as_upsert"] = True

    result = query_util.update_doc(body=new_doc, index=ORBIT_STATUS_INDEX, doc_type=ORBIT_STATUS_TYPE, doc_id=es_doc_id)
    print ("Updating: %s with %s"%(es_doc_id, json.dumps(new_doc)))
    print ("Result from ES update: %s"%result)
    return


def copy_sciflo_work(output_dir):
    """Move over smap_sciflo work dirs."""

    for root, dirs, files in os.walk(output_dir):
        for d in dirs:
            if not WORK_RE.search(d): continue
            path = os.path.join(root, d)
            if os.path.islink(path) and os.path.exists(path):
                real_path = os.path.realpath(path)
                base_name= os.path.basename(real_path)
                new_path = os.path.join(root, base_name)
                shutil.copytree(real_path, new_path)
                os.unlink(path)
                os.symlink(base_name, path)
    return


def update_half_orbit_status_doc(context_file, proc=None, job_id=None):
    ctx = open(context_file, 'r')
    context = json.loads(ctx.read())
    purpose = context["purpose"]
    if purpose == "Rest_of_NRT":
        es_doc_id = context["half_orbit_status_doc_id"]
    ctx.close()

    if purpose != "Rest_of_NRT":
        return

    if proc == None:
        # the sciflo run completed successfully
        update_es(es_doc_id=es_doc_id, prod_id=None, data_key_name=None, run_status=None, run_key_name=None, job_id_key=None, job_id=None, sciflo_run_status="C")
        return

    if proc not in PGE_PROCESS_LIST:
        # sciflo fails at IPP or PP
        update_es(es_doc_id=es_doc_id, prod_id=None, data_key_name=None, run_status=None, run_key_name=None, job_id_key=None, job_id=None, sciflo_run_status="F")
        return

    data_key_name = None
    job_status_key = None
    job_id_key = None

    pge_config_file = CONFIG_FILES[proc]
    pge_config = json.loads(open(pge_config_file, 'r').read())
    if "pge_run_status_field" in pge_config:
        job_status_key = pge_config["pge_run_status_field"]
    if "pge_job_id_field" in pge_config:
        job_id_key = pge_config["pge_job_id_field"]
    if "orbit_status_field" in pge_config:
        data_key_name = pge_config["orbit_status_field"]
    job_status = -1

    update_es(es_doc_id, prod_id="F", data_key_name = data_key_name, run_status=job_status, run_key_name= job_status_key, job_id=job_id, job_id_key=job_id_key, sciflo_run_status="F")
    return

def extract_error(sfl_json, context_file):
    """Extract SciFlo error and traceback for mozart."""

    with open(sfl_json) as f: j = json.load(f)
    exc_message = j.get('exceptionMessage', None)
    if exc_message is not None:
        try: exc_list = eval(exc_message)
        except: exc_list = []
        if len(exc_list) == 3:
            proc = exc_list[0]
            exc = exc_list[1]
            tb = exc_list[2]
            try: exc = eval(exc)
            except: pass
            if isinstance(exc, tuple) and len(exc) == 2:
                err = exc[0]
                job_json = exc[1]
                if isinstance(job_json, dict):
                    if 'job_id' in job_json:
                        err_str = 'SciFlo step %s with job_id %s (task %s) failed: %s' % \
                                  (proc, job_json['job_id'], job_json['uuid'], err)
                        if proc in PGE_PROCESS_LIST:
                            update_half_orbit_status_doc(context_file, proc, job_json['payload_id'])
                        else:
                            update_half_orbit_status_doc(context_file, proc, job_id=None)
                        with open('_alt_error.txt', 'w') as f:
                            f.write("%s\n" % err_str)
                        with open('_alt_traceback.txt', 'w') as f:
                            f.write("%s\n" % job_json['traceback'])
            else:
                err_str = 'SciFlo step %s failed: %s' % (proc, exc)
                update_half_orbit_status_doc(context_file, proc, job_id=None)
                with open('_alt_error.txt', 'w') as f:
                    f.write("%s\n" % err_str)
                with open('_alt_traceback.txt', 'w') as f:
                    f.write("%s\n" % tb)


def set_to_complete(context_file):
    update_half_orbit_status_doc(context_file, proc=None, job_id=None)
    return


def run_sciflo(sfl_file, sfl_args, output_dir):
    """Run sciflo."""

    # build paths to executables
    sflexec_path = os.path.join(os.environ['HOME'], 'verdi', 'bin', 'sflExec.py')

    # execute smap_sciflo
    cmd = [sflexec_path, "-s", "-f", "-o", output_dir, "--args", '"%s"' % ','.join(sfl_args), sfl_file]
    print("Running sflExec.py command:\n%s" % ' '.join(cmd))
    #check_call(cmd, shell)
    status = os.system(' '.join(cmd))
    sf_key, context_file = sfl_args[0].split("=")
    print("Exit status is: %d" % status)
    if status != 0:
        extract_error('%s/sciflo.json'%output_dir, context_file)
        status = 1
    else:
        #update sciflo run status to completed with "C"
        set_to_complete(context_file)

    # copy smap_sciflo work and exec dir
    try: copy_sciflo_work(output_dir)
    except: pass

    return status
