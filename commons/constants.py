"""
Add field names from PGE config files, names of functions,
match patterns or key names that can be referenced throughout code base

Note: To add new keys, please follow an alphabetical order

e.g.
LOCALIZE_KEY = "localize" # name of key found in input preprocessor output
GET_PGE_NAME = "pge_name" # name of key found in PGE config file
GET_ICE_SCLK = "getIceSclk" # name of function
"""
# To identify the preconditions to check for
PRECONDITIONS_KEY = "preconditions"

# Key identifying the payload in the _context file
RUNCONFIG_KEY = "runconfig"

# To Specify which group elements to localize
LOCALIZE_GROUPS_KEY = "localize_groups"

# To specify which filepaths to localize in the worker. Used by Mozart
LOCALIZE_KEY = "localize"

# Function for job specs info retrieval
JOB_INFO = "get_info2"
INFO_FIELDS = "info_fields"

# To identify the preconditions to check for
PRECONDITIONS_KEY = "preconditions"

# To identify file type level conditions
CONDITIONS_KEY = "conditions"

# Keys for identifying in the post_processor produced context.json
PRODUCTS_ID_KEY = "product_ids"

# Key used in post processor to identify the metadata of all products generated
# This is a list of dictionaries
PRODUCTS_METADATA_KEY = "product_metadata"

# Key used to identify output products from the previous PGE run
PRODUCT_NAMES_KEY = "product_names"

# Key used to identify the path of the products created by the previous PGE
PRODUCT_PATHS_KEY = "product_paths"

RELEASE_VERSION = "release_version"

LAST_MOD_TIME = "LastModifiedTime"
