#!/usr/bin/env python
import os
import json
import re
import subprocess

WORK_RE = re.compile(r"\d{5}-.+")

# sciflo PGE process names and mapping to their config files
# This is the list of PGEs that need to report status to an explict index
MAX_PLACEHOLDER_FILE_SIZE = 1000
PLACEHOLDER_ERROR_FILE = "_alt_error_hold.txt"
PLACEHOLDER_TB_FILE = "_alt_traceback_hold.txt"
PLACEHOLDER_DOCKER_STATS_FILE = "_docker_stats_hold.json"

PLACEHOLDER_FILES = [
    PLACEHOLDER_ERROR_FILE,
    PLACEHOLDER_TB_FILE,
    PLACEHOLDER_DOCKER_STATS_FILE,
]


def __create_placeholder_alt_files():
    """
    Due to possible disk space issues, this function will create temporary
    files in case we need to capture the _alt_error, _alt_traceback, and _docker_stats
    files

    :param work_dir:
    :return:
    """
    with open(PLACEHOLDER_ERROR_FILE, "wb") as f:
        f.seek(MAX_PLACEHOLDER_FILE_SIZE)
        f.write(b"\0")

    with open(PLACEHOLDER_TB_FILE, "wb") as f:
        f.seek(MAX_PLACEHOLDER_FILE_SIZE)
        f.write(b"\0")

    with open(PLACEHOLDER_DOCKER_STATS_FILE, "w") as f:
        json.dump(dict(), f)


def __cleanup_placeholder_alt_files():
    for temp_file in PLACEHOLDER_FILES:
        if os.path.exists(temp_file):
            print(f"Remove existing placeholder file: {temp_file}")


def __write_error_files(error, traceback):
    alt_error_file = "_alt_error.txt"
    alt_tb_file = "_alt_traceback.txt"
    docker_stats_file = "_docker_stats.json"

    try:
        with open(alt_error_file, "w") as f:
            f.write("%s\n" % error)
        with open(alt_tb_file, "w") as f:
            f.write("%s\n" % traceback)
    except OSError as oe:
        print(
            f"OSError encountered: {str(oe)}. Will write errors to placeholder files."
        )
        print(f"Renaming {PLACEHOLDER_ERROR_FILE} to {alt_error_file}.")
        os.rename(PLACEHOLDER_ERROR_FILE, alt_error_file)
        print(f"Renaming {PLACEHOLDER_TB_FILE} to {alt_tb_file}.")
        os.rename(PLACEHOLDER_TB_FILE, alt_tb_file)

        with open(alt_error_file, "w") as f:
            f.write("%s\n" % error[:MAX_PLACEHOLDER_FILE_SIZE])

        with open(alt_tb_file, "w") as f:
            f.write("%s\n" % traceback[:MAX_PLACEHOLDER_FILE_SIZE])
        print(f"Successfully wrote the errors to {alt_error_file} and {alt_tb_file}")

        if (
            os.path.exists(docker_stats_file)
            and os.path.getsize(docker_stats_file) == 0
        ):
            print(f"Renaming {PLACEHOLDER_DOCKER_STATS_FILE} to {docker_stats_file}")
            os.rename(PLACEHOLDER_DOCKER_STATS_FILE, docker_stats_file)
            print(
                f"Successfully renamed {PLACEHOLDER_DOCKER_STATS_FILE} to {docker_stats_file}"
            )


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
                try:
                    subprocess.run(["cp", "-R", real_path, new_path], check=True, text=True, capture_output=True)
                except subprocess.CalledProcessError as e:
                    print(f"Error occurred during copy: return code = {e.returncode}\n{e.stderr}")  # captures stderr output
    return


def extract_error(sfl_json):
    """Extract SciFlo error and traceback for mozart."""

    with open(sfl_json) as f:
        j = json.load(f)
    exc_message = j.get("exceptionMessage", None)
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
                    if "job_id" in job_json:
                        err_str = (
                            "SciFlo step %s with job_id %s (task %s) failed: %s"
                            % (proc, job_json["job_id"], job_json["uuid"], err)
                        )
                        __write_error_files(err_str, job_json["traceback"])
            else:
                err_str = "SciFlo step %s failed: %s" % (proc, exc)
                __write_error_files(err_str, tb)


def run_sciflo(sfl_file, sfl_args, output_dir):
    """Run sciflo."""

    # build paths to executables
    sflexec_path = os.path.join(os.environ["HOME"], "verdi", "bin", "sflExec.py")
    __create_placeholder_alt_files()
    # execute sciflo
    cmd = [
        sflexec_path,
        "-s",
        "-f",
        "-o",
        output_dir,
        "--args",
        '"%s"' % ",".join(sfl_args),
        sfl_file,
    ]
    print("Running sflExec.py command:\n%s" % " ".join(cmd))
    status = os.system(" ".join(cmd))
    sf_key, context_file = sfl_args[0].split("=")
    print("Exit status is: %d" % status)
    if status != 0:
        extract_error("%s/sciflo.json" % output_dir)
        status = 1

    # copy smap_sciflo work and exec dir
    try:
        copy_sciflo_work(output_dir)
    except Exception:
        pass

    __cleanup_placeholder_alt_files()
    return status
