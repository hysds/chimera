#!/usr/bin/env python
import json
from chimera.commons.constants import ChimeraConstants as chimera_const

class Accountability(object):
    def __init__(self, context, job_path, pge_config=None):
        self.context = context
        self.pge_config = pge_config
        
        with open("{}/_job.json".format(job_path), "r") as f:
            self.job_json = json.load(f)

            self.job_id = self.job_json.get(chimera_const.JOB_INFO).get(chimera_const.JOB_PAYLOAD).get(chimera_const.PAYLOAD_TASK_ID)

    def get_entries(self):
        pass

    def create_job_entry(self):
        pass
