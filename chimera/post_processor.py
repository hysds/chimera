#!/usr/bin/env python
"""
Contributors:
- Namrata Malarout
- Michael Cayanan
- Sujen Shah

The Post Processor queries:
1. Mozart for job infor
2. GRQ for product metadata
and creates a context.json (not the same as _context.json)
"""

from chimera.logger import logger
from chimera.postprocess_evaluator import PostProcessor


def post_process(sf_context, job_result, chimera_config_file, pge_config_file,  settings_file, test_mode=False):
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
    logger.info("Starting post_preprocessor step.")
    post_processor = PostProcessor(sf_context, chimera_config_file, pge_config_file,  settings_file, job_result)
    output_context = post_processor.process()
    logger.info("Finished post_processor step.")
    return output_context


if __name__ == '__main__':
    pass
