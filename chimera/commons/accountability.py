#!/usr/bin/env python
import json
from chimera.commons.constants import ChimeraConstants as chimera_const

class Accountability(object):
    def __init__(self, context, work_dir):
        self.context = context
        self.job_json = None
        self.job_id = None
        self.work_dir = work_dir
        if work_dir is not None:
            with open("{}/_job.json".format(work_dir), "r") as f:
                self.job_json = json.load(f)

                self.job_id = self.job_json.get(chimera_const.JOB_INFO).get(chimera_const.JOB_PAYLOAD).get(chimera_const.PAYLOAD_TASK_ID)

    def get_entries(self):
        pass

    def create_job_entry(self):
        pass

    def set_products(self, job_results):
        pass
