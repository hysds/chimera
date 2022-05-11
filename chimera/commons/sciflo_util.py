#!/usr/bin/env python
import os
import json
import re
import shutil

WORK_RE = re.compile(r'\d{5}-.+')

# sciflo PGE process names and mapping to their config files
# This is the list of PGEs that need to report status to an explict index


def copy_sciflo_work(output_dir):
    """Move over smap_sciflo work dirs."""

    # Instead of creating symlinks like it was initially doing, this has been updated
    # to copy the sciflo workunit directories to its human readable sciflo step.
    for root, dirs, files in os.walk(output_dir):
        for d in dirs:
            if not WORK_RE.search(d):
                continue
            path = os.path.join(root, d)
            if os.path.islink(path) and os.path.exists(path):
                real_path = os.path.realpath(path)
                os.unlink(path)
                base_name = os.path.basename(path)
                new_path = os.path.join(root, base_name)
                shutil.copytree(real_path, new_path)
    return


def extract_error(sfl_json):
    """Extract SciFlo error and traceback for mozart."""

    with open(sfl_json) as f:
        j = json.load(f)
    exc_message = j.get('exceptionMessage', None)
    if exc_message is not None:
        try:
            exc_list = eval(exc_message)
        except Exception:
            exc_list = []
        if len(exc_list) == 3:
            proc = exc_list[0]
            exc = exc_list[1]
            tb = exc_list[2]
            accountability = None
            try:
                exc = eval(exc)
            except Exception:
                pass
            if isinstance(exc, tuple) and len(exc) == 2:
                err = exc[0]
                job_json = exc[1]
                if isinstance(job_json, dict):
                    if 'job_id' in job_json:
                        err_str = 'SciFlo step %s with job_id %s (task %s) failed: %s' % \
                                  (proc, job_json['job_id'],
                                   job_json['uuid'], err)
                        with open('_alt_error.txt', 'a') as f:
                            f.write("%s\n" % err_str)
                        with open('_alt_traceback.txt', 'a') as f:
                            f.write("%s\n" % job_json['traceback'])
            else:
                err_str = 'SciFlo step %s failed: %s' % (proc, exc)
                with open('_alt_error.txt', 'a') as f:
                    f.write("%s\n" % err_str)
                with open('_alt_traceback.txt', 'a') as f:
                    f.write("%s\n" % tb)


def run_sciflo(sfl_file, sfl_args, output_dir):
    """Run sciflo."""

    # build paths to executables
    sflexec_path = os.path.join(
        os.environ['HOME'], 'verdi', 'bin', 'sflExec.py')

    # execute sciflo
    cmd = [sflexec_path, "-s", "-f", "-o", output_dir,
           "--args", '"%s"' % ','.join(sfl_args), sfl_file]
    print("Running sflExec.py command:\n%s" % ' '.join(cmd))
    status = os.system(' '.join(cmd))
    sf_key, context_file = sfl_args[0].split("=")
    print("Exit status is: %d" % status)
    if status != 0:
        extract_error('%s/sciflo.json' % output_dir)
        status = 1

    # copy smap_sciflo work and exec dir
    try:
        copy_sciflo_work(output_dir)
    except Exception:
        pass

    return status
