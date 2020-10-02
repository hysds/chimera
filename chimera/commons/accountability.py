#!/usr/bin/env python
from chimera.commons.constants import ChimeraConstants as chimera_const


class Accountability(object):
    def __init__(self, context, index=None):
        self.input_dataset_type = context.get(chimera_const.INPUT_DATASET_TYPE) + "_id"
        self.input_dataset_id = context.get(chimera_const.INPUT_DATASET_ID)
        self.step = context.get(chimera_const.STEP)
        self.index = index

    def _search(self, query):
        pass

    def _update_doc(self, id, body):
        pass

    def get_entries(self):
        pass

    def set_status(self, status):
        pass