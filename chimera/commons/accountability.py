#!/usr/bin/env python
import chimera
from chimera.commons.constants import ChimeraConstants as chimera_const


class Accountability(object):
    def __init__(self, context, pge_config=None, job_json=None, job_id=None):
        self.pge_config = pge_config
        self.input_dataset_type = context.get(chimera_const.INPUT_DATASET_TYPE) + "_id"
        self.input_dataset_id = context.get(chimera_const.INPUT_DATASET_ID)
        self.step = context.get(chimera_const.STEP)
        self.product_paths = context.get(chimera_const.PRODUCT_PATHS)
        if job_id is not None:
            self.job_id = job_id
        elif job_json is not None:
            self.job_id = job_json.get(chimera_const.JOB_INFO).get(chimera_const.JOB_PAYLOAD).get(chimera_const.PAYLOAD_TASK_ID)
        else:
            self.job_id = None
        
        if job_json is not None:
            self.job_path = job_json.get(chimera_const.JOB_INFO).get(chimera_const.JOB_DIR)
        elif self.pge_config is not None:
            self.job_path = "/".join(self.pge_config.get("groups").get("InputFileGroup").get("InputFilePath")[0].split("/")[0: -1])
        else:
            self.job_path = None

    def _search(self, query):
        pass

    def _update_doc(self, id, body):
        pass

    def get_entries(self):
        pass

    def set_status(self, status):
        pass
    
    def create_entry(self):
        pass
