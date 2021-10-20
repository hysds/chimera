"""
Add field names from PGE config files, names of functions,
match patterns or key names that can be referenced throughout code base

Note: To add new keys, please follow an alphabetical order

e.g.
LOCALIZE_KEY = "localize" # name of key found in input preprocessor output
GET_PGE_NAME = "pge_name" # name of key found in PGE config file
GET_ICE_SCLK = "getIceSclk" # name of function
"""


class ChimeraConstants(object):
    def __init__(self):
        pass

    # PGE's name
    PGE_NAME = "pge_name"

    # To identify the preconditions to check for
    PRECONDITIONS = "preconditions"

    # To identify the post processing steps to run
    POSTPROCESS = "postprocess"

    # Key identifying the payload in the _context file
    RUNCONFIG = "runconfig"

    # To Specify which group elements to localize
    LOCALIZE_GROUPS = "localize_groups"

    # To specify which filepaths to localize in the worker. Used by Mozart
    LOCALIZE = "localize"
    CONFIGURATION = "configuration"
    PRODUCTION_DATETIME = "ProductionDateTime"

    # Key in runconfig for list of inputs
    RC_INPUT = "InputFilePath"

    # To identify file type level conditions
    CONDITIONS = "conditions"

    # Keys for identifying in the post_processor produced context.json
    PRODUCTS_ID = "product_ids"

    # primary input key in PGE config
    PRIMARY_INPUT = "primary_input"

    # identifier token to specify empty runconfig values to be filled
    EMPTY_FIELD_IDENTIFIER = "empty_field_identifier"

    # field to specify optionals runconfig fields
    OPTIONAL_FIELDS = "optionalFields"

    # Key used in post processor to identify the metadata of all products generated
    # This is a list of dictionaries
    PRODUCTS_METADATA = "product_metadata"

    # Key used to identify output products from the previous PGE run
    PRODUCT_NAMES = "product_names"

    # Key used to identify the path of the products created by the previous PGE
    PRODUCT_PATHS = "product_paths"

    RELEASE_VERSION = "release_version"

    SIMULATE_OUTPUTS = "simulate_outputs"

    PGE_SIM_MODE = "PGE_SIMULATION_MODE"

    OUTPUT_TYPES = "output_types"

    LAST_MOD_TIME = "LastModifiedTime"

    JOB_INFO = "job_info"

    JOB_PAYLOAD = "job_payload"

    PAYLOAD_TASK_ID = "payload_task_id"

    JOB_ID_FIELD = "job_id"

    JOB_TYPES = "JOB_TYPES"

    JOB_QUEUES = "JOB_QUEUES"

    INPUT_DATASET_TYPE = "dataset_type"

    INPUT_DATASET_ID = "input_dataset_id"

    STEP = "purpose"
