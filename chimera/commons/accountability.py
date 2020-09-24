from constants import ChimeraConstants as chimera_const


class Accountability(object):
    def __init__(self, context):
        self.input_dataset_type = context.get(chimera_const.INPUT_DATASET_TYPE)
        self.input_dataset_id = context.get(chimera_const.INPUT_DATASET_ID)
        self.step = context.get(chimera_const.STEP)

    def set_status(self, status):
        pass