#!/usr/bin/env python
from collections import OrderedDict
from datetime import datetime
from datetime import timedelta
import logging, traceback
import re
import sys
# from urlparse import urlparse
import simplejson
from string import Template
from commons import constants, orbit_util, query_util, ancillary_util, product_metadata
import os
import yaml

# Set up logging
LOGGER = logging.getLogger("preconditions")
LOGGER.setLevel(logging.DEBUG)
stdout = logging.StreamHandler(sys.stdout)
stdout.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout.setFormatter(formatter)
LOGGER.addHandler(stdout)


# This is the output generated
job_params = {}
# Used to identify fields to be filled within the runconfig context.json of PGE
EMPTY_FIELD_IDENTIFIER = None


def set_product_time():
    # Set ProductionDateTime as PGE binary needs that to create/name the log file
    job_params.update({"ProductionDateTime": "{}Z".format(datetime.utcnow().isoformat()[:-3])})


def localize_paths(pge_config, output_context):
    """
    To set file to localize in the docker
    :param pge_config:
    """
    LOGGER.debug("Preparing to localize file paths")

    # Deprecated function since not all values in localize_groups are on s3 for example SPS config files
    # def is_url(val):
    #     parse_result = urlparse(val)
    #     schemes = ["s3", "s3s", "http", "https", "ftp", "sftp", "azure", "azures", "rsync"]
    #     return parse_result.scheme in schemes
    #
    # localize_paths_list = []
    # for group in pge_config.get(constants.LOCALIZE_GROUPS_KEY, []):
    #     for elem in output_context.get(group, []):
    #         value = output_context.get(group).get(elem)
    #
    #         # If the value is a list, example some InputFilGroups could be scalars or vectors
    #         if isinstance(value, list):
    #             for v in value:
    #                 if is_url(v):
    #                     localize_paths_list.append(v)
    #
    #         elif isinstance(value, str):
    #             if is_url(value):
    #                 localize_paths_list.append(value)
    #
    #         else:
    #             continue
    #
    # return localize_paths_list


def get_product_counter(pge_config, context, sys_config, job_params):
    """
    To get the product counter
    :return:
    """
    LOGGER.info("Evaluating precondition {}".format(constants.GET_PRODUCT_COUNTER_KEY))
    # TODO Write this function out
    product_counter = "001"  # Default value
    input_path_pges = ["L0B_Radiometer", "TSURF_PP",
                       "SNOW_PP", "PRECIP_PP"]  # PGEs for with the input count is determined by number of inputs
    input_count = 0
    conditions = None
    product_counter_config = pge_config.get(constants.GET_PRODUCT_COUNTER_KEY)
    product_type = pge_config.get(constants.GET_PGE_NAME)
    input_paths = context.get(constants.PRODUCT_PATHS_KEY)
    if product_counter_config is not None:
        geometry = product_counter_config.get("geometry", "half_orbit")
        try:
            # Set input counter
            if input_paths:
                if product_type in input_path_pges:
                    input_count = len(input_paths)
                else:
                    # Get the product counter of the input product
                    if isinstance(input_paths, list):
                        match = re.match(r".*_(\d{3})\..*", input_paths[0])
                    else:
                        match = re.match(r".*_(\d{3})\..*", input_paths)
                    input_count = int(match.group(1))
            if product_counter_config.get("condition") is not None:
                conditions = dict()
                for condition_key in product_counter_config.get("condition"):
                    conditions[condition_key] = job_params.get(condition_key)
            product_counter = query_util.get_product_counter(product_type=product_type,
                                                             crid=sys_config.get(constants.COMPOSITE_RELEASE_ID),
                                                             geometry=geometry,
                                                             orbit_number=job_params.get(constants.ORBIT_NUMBER),
                                                             orbit_direction=job_params.get(constants.ORBIT_DIRECTION),
                                                             rangeBeginningDateTime=job_params.get(
                                                                 constants.RANGEBEGINDATETIME),
                                                             rangeEndingDateTime=job_params.get(
                                                                 constants.RANGEENDDATETIME),
                                                             input_count=input_count, other_conditions=conditions)
        except Exception as e:
            LOGGER.warn("Could not get max product counter: {}".format(traceback.format_exc()))
            raise RuntimeError("Could not get product counter: {}".format(e))

    return {"ProductCounter": str(product_counter).rjust(3, "0")}


def getAntPosition(input_context):
    '''
    Tries to find the previous AntPosition file (within 45 minutes in the past). If one is found
    '''
    LOGGER.info("Evaluating precondition {}".format(constants.GET_ANTAZ_PRODUCTS_KEY))
    product_paths = []
    if input_context.get(constants.PRODUCT_PATHS_KEY) and input_context.get(constants.PRODUCTS_METADATA_KEY):
        # Get the input AntPosition file and associated metadata
        product = input_context.get(constants.PRODUCT_PATHS_KEY)
        product_metadata = input_context.get(constants.PRODUCTS_METADATA_KEY)
        if isinstance(product_metadata, list):
            for met in product_metadata:
                metadata = met.get("metadata")
        else:
            metadata = product_metadata.get("metadata")
        if isinstance(product, list):
            product_paths.extend(product)
        else:
            product_paths.append(product)
        # Find the previous AntPosition file
        try:
            previous = ancillary_util.get_previous_antposition_file(metadata.get(constants.DVTSCLKCOARSE))
            product_paths.insert(0, previous)
        except Exception as e:
            message = "Previous AntPosition file could not be found."
            LOGGER.info("{}: {}".format(message, e))
    else:
        raise RuntimeError("Could not find input product and/or metadata")

    return product_paths


def getIceSclk(input_context, pge_config):
    '''
    Gets the matching ICE_SCLK file that is associated with the given AntPositions
    '''
    ANTPOSITION_MATCH_PATTERN = "/(?P<id>0251_(?P<DvtSclkCoarse>\\d{10})-(?P<DvtSclkFine>\\d{7})-(?P<VersionID>\\d{1,2})\\.dat)$"
    pattern = re.compile(ANTPOSITION_MATCH_PATTERN)
    LOGGER.info("Evaluating precondition {}".format(constants.GET_ICE_SCLK))
    antpositions = input_context.get("InputFilePath")
    '''
    Check if a list. If it is not, then create a list
    '''
    if not isinstance(antpositions, list):
        antpositions_list = []
        antpositions_list.append(antpositions)
        antpositions = antpositions_list
    matching_ice_sclks = []
    for antposition in antpositions:
        match = pattern.search(antposition)
        if match:
            key = 'DvtSclkCoarse'
            try:
                sclk_value = match.groupdict()[key]
                matching_ice_sclks.append(ancillary_util.get_ice_sclk(sclk_value))
            except KeyError:
                raise KeyError(('Could not find the {} field in the file {}. Match pattern is {}'.format(key,
                                                                                                         antposition,
                                                                                                         ANTPOSITION_MATCH_PATTERN)))

    if len(matching_ice_sclks) == 1:
        return {constants.ICE_SCLK_ALIAS.upper(): matching_ice_sclks[0]}
    else:
        return {constants.ICE_SCLK_ALIAS.upper(): matching_ice_sclks}


def getL2Products(input_context, pge_config):
    """
    The inputs to the L2_SM_P_SPS are an L1C_TB and an L2_SM_A product of the same OrbitNumber, OrbitDirection, and
    CompositeReleaseID. The trigger file is L1C_TB. To find the associated L2_SM_A product, query for the following:

    Find the latest L2_SM_A file with the same OrbitNumber, OrbitDirection, and CompositeReleaseID as the input
    L1C_TB file.
    If no results could not be found, find the latest L2_SM_A_DEFAULT file.
    This should cover both the forward and reprocessing use cases.

    When setting, these inputs in the Run Config, the L2_SM_A file must be listed first before the L1C_TB input product.
    :param input_context:
    :param pge_config:
    :return:
    """
    LOGGER.info("Evaluating precondition {}".format(constants.GET_L2_PRODUCTS_KEY))
    input_file_path_key = "InputFilePath"
    product_paths = []
    if input_context.get(constants.PRODUCT_PATHS_KEY) and input_context.get(constants.PRODUCTS_METADATA_KEY):
        # Yields a list of input products and list of associated metadata, there should be only one L1C
        product = input_context.get(constants.PRODUCT_PATHS_KEY)[0]
        metadata = input_context.get(constants.PRODUCTS_METADATA_KEY)[0].get("metadata")
        conditions = list()
        conditions.append((constants.ORBIT_NUMBER, metadata.get(constants.ORBIT_NUMBER)))
        conditions.append((constants.ORBIT_DIRECTION, metadata.get(constants.ORBIT_DIRECTION)))
        conditions.append((constants.COMPOSITE_RELEASE_ID, metadata.get(constants.COMPOSITE_RELEASE_ID)))
        query = query_util.construct_bool_query(conditions)
        try:
            result = query_util.get_latest_pge_product_by_counter(query=query, product_type="L2_SM_A")
            if result.get("hits").get("total") == 0:
                raise RuntimeWarning("Could not find an L2_SM_A product")
            product_paths.append(query_util.get_datastore_refs(result))
        except:
            LOGGER.warn("Could not find an L2_SM_A product, looking for default")

            result = query_util.get_latest_product_by_version(product_type="L2_SM_A_DEFAULT",
                                                              index=constants.L2_SM_A_DEFAULT_ALIAS)
            product_paths.append(result)
        product_paths.append(product)
        return product_paths

    else:
        raise RuntimeError("Could not find input product and/or metadata")


def get_previous_L3_FT_P(product_type, granule_date, input_context, crid):
    """
    Intended to be used to get the previous day L3_FT_P(_E) file selection rule
    Step 1: Find the latest with same CRID used to find the input L1C_TB(_E)
    Step 2: If nothing was found, find the latest (highest CRID, highest
    ProductCounter)
    Step 3: If nothing, find the L3_FT_P(_E)_DEFAULT file.

    :param product_type: product type (L3_FT_P or L3_FT_P_E)
    :param granule_date: granule date
    :param input_context: input context
    :param crid: CompositeReleaseID

    :return:
    """
    LOGGER.info("Evaluating precondition {}".format(constants.GET_PREVIOUS_L3_FT_P_KEY))
    previous_date = datetime.strptime(granule_date, "%Y-%m-%d").date() \
                    - timedelta(days=1)
    crid_key = "metadata.{}.raw".format(
        product_metadata.COMPOSITE_RELEASE_ID_METADATA)
    conditions = {}
    conditions[crid_key] = crid
    conditions[product_metadata.RANGE_BEGIN_DATE_METADATA] = previous_date.strftime(
        "%Y-%m-%d")
    LOGGER.info("Finding latest (highest ProductCounter) {} with conditions {}"
                .format(product_type, conditions))
    query = query_util.construct_bool_query(conditions)
    try:
        result = query_util.get_latest_record_by_version(
            product_type, es_query=query, index=product_type.lower(),
            version_metadata=product_metadata.PRODUCT_COUNTER_METADATA)
        datastore_ref = query_util.get_datastore_ref_from_es_record(
            result.get("hits").get("hits")[0])
        if datastore_ref is not None:
            return datastore_ref[0]

    except Exception as e:
        LOGGER.error("{}".format(e))
        pass
    LOGGER.info("Could not find {} with conditions {}".format(product_type,
                                                              conditions))
    ''' Find highest CRID '''
    conditions.pop(crid_key, None)
    LOGGER.info("Finding latest (highest CRID, highest ProductCounter) {} with "
                "conditions {}".format(product_type, conditions))
    query = query_util.construct_bool_query(conditions)
    sort_clauses = list()
    sort_clauses.extend(ancillary_util.construct_sort_clause(
        "{}:{}".format(constants.COMPOSITE_RELEASE_ID, "desc")))
    sort_clauses.extend(ancillary_util.construct_sort_clause(
        "{}:{}".format(product_metadata.PRODUCT_COUNTER_METADATA, "desc")))
    query.update({"sort": sort_clauses})
    try:
        result = query_util.run_query(body=query, doc_type=product_type, size=1,
                                      index=product_type.lower())
        datastore_ref = query_util.get_datastore_ref_from_es_record(
            result.get("hits").get("hits")[0])
        if datastore_ref is not None:
            return datastore_ref[0]

    except Exception as e:
        pass

    LOGGER.info("Could not find latest {} with conditions {}".format(
        product_type, conditions))
    ''' Find Default '''
    default_type = "{}_DEFAULT".format(product_type)
    LOGGER.info("Finding {} ".format(default_type))
    ref = query_util.get_latest_product_by_version(product_type=default_type,
                                                   index=default_type.lower())
    if ref is not None:
        if isinstance(ref, list):
            return ref[0]
        else:
            return ref
    else:
        LOGGER.error("Could not find latest {}".format(default_type))
        raise RuntimeError("Could not find the an {} product for day {}"
                           .format(product_type, previous_date))


def get_products(input_context, pge_config, sys_config):
    """
    Returns the names of the products generated by the previous step and its metadata as a dict
    :param input_context: input context as a python dict that triggered this stage
    :return: dict containing the product s3 paths
    """
    LOGGER.info("Evaluating precondition {}".format(constants.GET_PRODUCTS_KEY))
    input_file_path_key = "InputFilePath"
    product_paths = []
    sps_name = _get_sps_name(pge_config)
    if sps_name == constants.L2_SM_P_SPS or sps_name == constants.L2_SM_P_E_SPS:
        product_paths = getL2Products(input_context, pge_config)

    elif sps_name.startswith("L3"):
        granule_date = input_context.get(constants.GRANULE_DATE)
        if sps_name == constants.L3_FT_P_SPS:
            product_paths.extend(ancillary_util.get_latest_L3_inputs(
                granule_date, constants.L1C_TB, sys_config.get(constants.COMPOSITE_RELEASE_ID)))
            ''' get previous day L3_FT_P '''
            product_paths.insert(0, get_previous_L3_FT_P(constants.L3_FT_P,
                                                         granule_date,
                                                         input_context,
                                                         sys_config.get(
                                                             constants.COMPOSITE_RELEASE_ID)))

    elif sps_name == constants.ANTAZ_PP:
        product_paths = getAntPosition(input_context)

    elif input_context.get(constants.PRODUCT_PATHS_KEY):
        ppaths = input_context.get(constants.PRODUCT_PATHS_KEY, [])
        if isinstance(ppaths, list):
            for ppath in ppaths:
                LOGGER.debug("{}: Adding product {}".format(constants.GET_PRODUCTS_KEY, ppath))
                product_paths.append(ppath)
        else:
            LOGGER.debug("{}: Adding product {}".format(constants.GET_PRODUCTS_KEY, ppaths))
            product_paths.append(ppaths)

    else:
        raise RuntimeError("Key {} not found in provided context file".format(constants.PRODUCT_PATHS_KEY))

    LOGGER.info("Setting {} input products: {}".format(len(product_paths), ', '.join(product_paths)))
    # L0A is triggered through the directory crawler, so we dont have a dataset yet unlike others
    if pge_config.get(constants.GET_PGE_NAME) != "L0A_Radiometer":
        product_paths = ancillary_util.convert_dataset_to_object(product_paths)

    if len(product_paths) == 0:
        raise RuntimeError("No products found to set as input in the context")

    return {input_file_path_key: product_paths}


def get_product_metadata(input_context, pge_config):
    """
    To get the metadata that was extracted from the products generated by the previous PGE run
    :param input_context:
    :param keys: Metadata keys to extract
    :return:
    """
    LOGGER.info("Evaluating precondition {}".format(constants.GET_PRODUCT_METADATA_KEY))
    try:
        if input_context.get(constants.PRODUCTS_METADATA_KEY) is None:
            raise ValueError("No product metadata key found in the input context")
        keys = pge_config.get(constants.GET_PRODUCT_METADATA_KEY).get("keys", [])
        metadata = dict()
        metadata_obj = input_context.get(constants.PRODUCTS_METADATA_KEY)
        if isinstance(metadata_obj, list):
            for product in metadata_obj:
                metadata.update(_get_keys_from_dict(product.get("metadata"), keys))
        else:
            metadata.update(_get_keys_from_dict(metadata_obj.get("metadata"), keys))
        return metadata

    except Exception as e:
        LOGGER.error("Could not extract product metadata: {}".format(traceback))
        raise RuntimeError("Could not extract product metadata: {}".format(e))


def get_metadata(input_context, keys):
    """
    Returns a dict with only the key: value pair for keys in 'keys' from the input_context
    :param input_context: input context as a python dict that triggered this stage
    :param keys: Keys to copy
    :return: dict or raises error if not found
    """
    LOGGER.info("Evaluating precondition {}".format(constants.GET_METADATA_KEY))
    try:
        metadata = _get_keys_from_dict(input_context, keys)
        return metadata
    except Exception as e:
        LOGGER.error("Could not extract metadata from input context: {}".format(traceback.format_exc()))
        raise RuntimeError("Could not extract metadata from input context: {}".format(e))


def getEclipseTimes(orbit_number, orbit_direction):
    """
    Get the BeginEclipseDateTime and EndEclipseDateTime values associated with the half-orbit that is being processed,
    if they exist in the Orbits table. If they do not exist, set their attribute values to NULL
    in the Run Configuration file.
    :param :
    :return:
    """
    record = record = orbit_util.get_eclipse_times(orbit_number=orbit_number, orbit_direction=orbit_direction)
    result = dict()
    result[product_metadata.BEGIN_ECLIPSE_DATETIME_METADATA] = \
        record.get(product_metadata.BEGIN_ECLIPSE_DATETIME_METADATA, "NULL")
    result[product_metadata.END_ECLIPSE_DATETIME_METADATA] = \
        record.get(product_metadata.END_ECLIPSE_DATETIME_METADATA, "NULL")
    return result


def get_L3_temporal_and_geometry_metadata(job_params, pge_config, context):
    """
    Returns the Temporal and Geometry metadata for L3 PGEs.
    """
    metadata = {}
    if _get_sps_name(pge_config) == constants.L3_FT_P_SPS:
        earliest_product = os.path.basename(job_params["InputFilePath"][1])
        latest_product = os.path.basename(job_params["InputFilePath"][-1])
        pattern = re.compile(constants.L1C_TB_MATCH_PATTERN)
        match = pattern.match(earliest_product)
        if match:
            metadata['StartOrbitNumber'] = match.groupdict()['OrbitNumber']
        else:
            raise RuntimeError("Could not get StartOrbitNumber metadata")

        match = pattern.match(latest_product)
        if match:
            metadata['StopOrbitNumber'] = match.groupdict()['OrbitNumber']
        else:
            raise RuntimeError("Could not get StopOrbitNumber metadata")

    metadata['RangeBeginningDate'] = context.get(constants.GRANULE_DATE)
    metadata['RangeEndingDate'] = context.get(constants.GRANULE_DATE)
    metadata['RangeBeginningDateTime'] = context.get(constants.GRANULE_DATE) \
                                         + "T00:00:00.000Z"
    metadata['RangeEndingDateTime'] = context.get(constants.GRANULE_DATE) \
                                      + "T23:59:59.999Z"
    return metadata


def get_orbit_info(half_orbit_id):
    """
    Returns the orbit record with the specified id from the orbits index in ES
    :param half_orbit_id: ID of the record to retrieve, if null the method will return the latest half_orbit info
    :return: orbit record
    """
    LOGGER.info("Evaluating precondition {}".format(constants.GET_ORBIT_INFO_KEY))

    def convert_datetime(datetime):
        result = str(datetime).split("T")
        return result[0], result[1]

    if half_orbit_id is None:
        record = orbit_util.get_latest_orbit_record()
    else:
        record = orbit_util.get_record_by_id(half_orbit_id)

    if record is None:
        # If no orbit record found, return record with StartOrbitNumber=0 and OrbitDirection=NULL
        record = {'StartOrbitNumber': 0, 'OrbitDirection': 'NULL'}
    else:
        record[product_metadata.START_ORBIT_NUMBER_METADATA] = record.get('OrbitNumber', 0)
        record[product_metadata.STOP_ORBIT_NUMBER_METADATA] = record.get('OrbitNumber', 0)

        radiometerBeginDateTime = record.get('RadiometerBeginningDateTime')
        radiometerEndingDateTime = record.get('RadiometerEndingDateTime')

        record['RangeBeginningDate'], record['RangeBeginningTime'] = convert_datetime(radiometerBeginDateTime)
        record['RangeEndingDate'], record['RangeEndingTime'] = convert_datetime(radiometerEndingDateTime)

        record['HalfOrbitStartDate'], record['HalfOrbitStartTime'] = convert_datetime(
            record.get('HalfOrbitStartDateTime'))
        record['HalfOrbitStopDate'], record['HalfOrbitStopTime'] = convert_datetime(record.get('HalfOrbitStopDateTime'))

    return record


def get_spice_files(pge_config):
    """
    To verify the existence of and get the latest version of the SPICE files from the catalog
    :return: dict of filetype: filepath
    """
    static_ancillary_config = pge_config.get(constants.GET_SPICE_FILES_KEY)
    product_types = static_ancillary_config.get(constants.GET_SPICE_PRODUCT_TYPES_KEY, [])
    _filter = static_ancillary_config.get(constants.FILTER_KEY, None)
    LOGGER.info("Evaluating precondition {}".format(constants.GET_SPICE_FILES_KEY))
    LOGGER.debug("Finding latest versions of following SPICE product types {}".format(', '.join(product_types)))
    products = {}
    for _type in product_types:
        # Search catalog for LOM location
        LOGGER.debug("Querying for type {} with filters ".format(_type))
        conditions = None
        if _filter and _type in _filter:
            conditions = _filter.get(_type, {}).get(constants.CONDITIONS_KEY)
        attribute_name = _type
        _attribute_names = static_ancillary_config.get(
            constants.ATTRIBUTE_NAMES_KEY)
        # ASSUMPTION: The filepath returned is an s3 url containing the file name
        if isinstance(conditions, list):
            for condition in conditions:
                filepath = ancillary_util.get_latest_static_by_version(
                    _type, condition)
                if filepath is not None:
                    attribute_name = _get_attribute_name(_type,
                                                         _attribute_names,
                                                         condition)
                    products.update({attribute_name: filepath})
                    LOGGER.debug(
                        "Found {} for {} with condition {}".format(
                            products.get(attribute_name), _type,
                            condition))
                else:
                    raise RuntimeError("No files found for {} with "
                                       "condition {}".format(_type,
                                                             condition))
        else:
            filepath = ancillary_util.get_latest_static_by_version(_type, conditions)
            if filepath is not None:
                attribute_name = _get_attribute_name(_type,
                                                     _attribute_names,
                                                     conditions)
                products.update({attribute_name: filepath})
                LOGGER.debug("Found {} for {} with condition {}".format(
                    products.get(attribute_name), _type, conditions))
            else:
                raise RuntimeError("No files found for {} with "
                                   "condition {}".format(_type,
                                                         conditions))

    return products


def get_dynamic_ancillary_files(job_params, pge_config):
    """
    To get the dynamic ancillary files for the given types

    :param pge_config:
    :param job_params:
    :return:
    """
    LOGGER.info("Evaluating precondition {}".format(constants.GET_DYNAMIC_ANCILLARY_FILES))
    LOGGER.debug("Getting dynamic spice files for {}"
                 .format(pge_config.get(constants.GET_DYNAMIC_ANCILLARY_FILES).get("spiceProductTypes")))
    file_refs = dict()
    dataBeginningDateTime = job_params.get("RangeBeginningDateTime")
    dataEndingDateTime = job_params.get("RangeEndingDateTime")
    dynamic_ancillary_config = pge_config.get(constants.GET_DYNAMIC_ANCILLARY_FILES)
    _filter = dynamic_ancillary_config.get(constants.FILTER_KEY)

    try:
        for key in dynamic_ancillary_config.get("spiceProductTypes"):
            conditions = None
            if _filter:
                conditions = _filter.get(key, {}).get(constants.CONDITIONS_KEY)
            results = None
            attribute_name = key
            _attribute_names = dynamic_ancillary_config.get(constants.ATTRIBUTE_NAMES_KEY)
            if isinstance(conditions, list):
                for condition in conditions:
                    results = ancillary_util.get_dynamic_spice_file(
                        ancillary_type=key,
                        dataBeginningDateTime=dataBeginningDateTime,
                        dataEndingDateTime=dataEndingDateTime,
                        pge_name=pge_config.get(constants.GET_PGE_NAME),
                        conditions=condition)
                    if results is not None:
                        attribute_name = _get_attribute_name(key,
                                                             _attribute_names,
                                                             condition)
                        file_refs[attribute_name] = results
                        LOGGER.debug(
                            "Found {} for {} with condition {}".format(
                                file_refs.get(attribute_name), key, condition))
                    else:
                        raise RuntimeError("No files found for {} with "
                                           "condition {}".format(key, condition))
            else:
                results = ancillary_util.get_dynamic_spice_file(
                    ancillary_type=key,
                    dataBeginningDateTime=dataBeginningDateTime,
                    dataEndingDateTime=dataEndingDateTime,
                    pge_name=pge_config.get(constants.GET_PGE_NAME),
                    conditions=conditions)
                if results is not None:
                    attribute_name = _get_attribute_name(key, _attribute_names,
                                                         conditions)
                    file_refs[attribute_name] = results
                    LOGGER.debug("Found {} for {} with conditions {}".format(
                        file_refs.get(attribute_name), key, conditions))
                else:
                    error = "No files found for {}".format(key)
                    if conditions:
                        error = error + " with condition {}".format(conditions)
                    raise RuntimeError(error)
    except Exception as e:
        error = "Dynamic ancillary not found for data datetime range {} to {}".format(
            dataBeginningDateTime, dataEndingDateTime)
        LOGGER.error("{}: {}".format(error, traceback.format_exc()))
        raise RuntimeError("{}: {}".format(error, e))

    return file_refs


def _get_attribute_name(key, attribute_names, substitution_map):
    attribute_name = key
    if attribute_names:
        if key in attribute_names:
            template = Template(attribute_names[key])
            attribute_name = template.substitute(substitution_map)
    return attribute_name


def set_geometry(input_context):
    metadata = dict()
    metadata_obj = input_context.get(constants.PRODUCTS_METADATA_KEY)
    if isinstance(metadata_obj, list):
        metadata = metadata_obj[0].get("metadata")
    else:
        metadata = metadata_obj.get("metadata")
    # metadata = input_context.get(constants.PRODUCTS_METADATA_KEY)[0].get("metadata")
    orbit_direction = metadata.get(product_metadata.ORBIT_DIRECTION_METADATA)
    northernMostCrossingPoint = metadata.get(product_metadata.HALFORBIT_START_DATETIME_METADATA)
    if orbit_direction == "Ascending":
        northernMostCrossingPoint = metadata.get(product_metadata.HALFORBIT_STOP_DATETIME_METADATA)

    return {product_metadata.NORTHERN_MOST_CROSSING_POINT_METADATA: northernMostCrossingPoint}


# Departure from python function naming convention to make the method name same as the key in json
# TODO Update all other method names to camelCase
def getEaseGrid(pge_config, context):
    """
    Get EASE GRID files
    :param pge_config:
    :param context:
    :return:
    """
    try:
        pattern = "((?P<ShortName>EZ2(?P<Coordinate>Lat|Lon))_(?P<Region>[MNS])(?P<GridSpacing>\d{2})_(?P<VersionID>\d{3})\.float32)$"
        eas_grid_conditions = pge_config.get(constants.GET_EASEGRID)
        refs = list()
        for condition in eas_grid_conditions:
            gridspacing = condition.get(product_metadata.GRIDSPACING_METADATA)
            region = condition.get(product_metadata.REGION_METADATA)
            for easegrid in ancillary_util.get_latest_easegrid(gridspacing, region):
                refs.append(easegrid)
        result = dict()
        for r in refs:
            match = re.match(r".*EZ2(?P<Coordinate>Lat|Lon)_(?P<Region>[MNS])(?P<GridSpacing>\d{2})_(\d{3})\.float32$",
                             r)
            if match is None:
                raise Exception("{} does not match pattern {} for EASEGRID ".format(r, pattern))

            pge_name = pge_config.get(constants.GET_PGE_NAME)

            if pge_name == "L1C_TB_SPS" or pge_name == "L1C_TB_E_SPS":
                filename = "EASEGRID_{}_{}".format(match.groupdict().get("Coordinate"),
                                                   match.groupdict().get("Region")).upper()
            else:
                filename = "EASEGRID_{}_{}{}".format(match.groupdict().get("Coordinate"),
                                                     match.groupdict().get("Region"),
                                                     match.groupdict().get("GridSpacing")).upper()
            result.update({filename: r})

        return result
    except Exception as e:
        LOGGER.error("Could not get EASEGRID file : {}".format(traceback.format_exc()))
        raise RuntimeError("Could not get EASEGRID file : {}".format(e))


def getIMSLat(pge_config, context):
    """
    Get IMS LAT files
    :param pge_config:
    :param context:
    :return:
    """
    ims_lat = ancillary_util.get_latest_ims_lat()
    result = dict()
    result.update({'IMS_LAT': ims_lat})
    return result


def getIMSLon(pge_config, context):
    """
    Get IMS Lon files
    :param pge_config:
    :param context:
    :return:
    """
    ims_lon = ancillary_util.get_latest_ims_lon()
    result = dict()
    result.update({'IMS_LON': ims_lon})
    return result


def getPrimaryExecutableMetadata(pge_config, sys_config):
    """
    To fill in the metadata values for CompositeReleaseID, SWVersionID, AlgorithmVersionID, ParameterVersionID
    and SeriesID
    :param pge_config:
    :param sys_config:
    :return: dict
    """
    try:
        # Get the CRID from the sysconfig
        CRID = sys_config.get(constants.COMPOSITE_RELEASE_ID, None)
        sps_name = pge_config.get(constants.RUNCONFIG_KEY).get("SPSNameGroup", {}).get("SPSName")

        # Get the rest by querying ES
        index = constants.VERSIONS_ALIAS
        query = {"query": {"term": {"_id": CRID}}}
        result = query_util.run_query(index=index, body=query)
        LOGGER.debug("Result from querying versions: {}".format(simplejson.dumps(result)))
        if len(result["hits"]["hits"]) != 1:
            msg = "Found {} documents for {} in index {}, expecting exactly 1".format(len(result["hits"]["hits"]),
                                                                                      CRID, index)
            LOGGER.error(msg)
            raise RuntimeError(msg)
        doc = result["hits"]["hits"][0]["_source"]
        if doc.get(sps_name) is None:
            msg = "{} not found in versions doc for CRID {}".format(sps_name, CRID)
            LOGGER.error(msg)
            raise RuntimeError(msg)
        sps_info = doc.get(sps_name)
        sps_info.update({constants.COMPOSITE_RELEASE_ID: CRID})
        LOGGER.debug("Updating {} INFO with folowing : {}".format(sps_name, simplejson.dumps(sps_info)))
        return sps_info

    except Exception as e:
        raise Exception(e)


def duplicateValues(job_params, pge_config):
    """
    Duplicates values of key provided in PGE config into to list of keys
    "from_key" : ["to_keys", "..", ...]
    :param job_params:
    :param pge_config:
    :return: dict
    """
    result = dict()
    config = pge_config.get(constants.DUPLICATE_VALUES_KEY, {})
    for from_key, to_keys in config.iteritems():
        if from_key in job_params:
            for key in to_keys:
                result.update({key: job_params.get(from_key)})
    return result


def check_missing(input_dict):
    for key in input_dict:
        if isinstance(input_dict.get(key), dict):
            pass


def prepare_runconfig(pge_config):
    """
    To prepare the final completed runconfig context.json which will be fed in to the pge
    :param pge_config:
    :return: dict
    """
    LOGGER.debug("Preparing runconfig for {}".format(pge_config.get('pge_name')))
    output_context = OrderedDict()

    if pge_config.get(constants.RUNCONFIG_KEY):
        for group in pge_config.get(constants.RUNCONFIG_KEY):
            if isinstance(pge_config.get(constants.RUNCONFIG_KEY).get(group), str):
                output_context[group] = pge_config.get(constants.RUNCONFIG_KEY).get(group)
                continue
            elif pge_config.get(constants.RUNCONFIG_KEY).get(group) == EMPTY_FIELD_IDENTIFIER:
                output_context[group] = job_params.get(group)
                continue
            output_context[group] = OrderedDict()  # Create group

            for item in pge_config.get(constants.RUNCONFIG_KEY).get(group):  # Populate group
                item_value = pge_config.get(constants.RUNCONFIG_KEY).get(group).get(item)

                if item_value == EMPTY_FIELD_IDENTIFIER:
                    # Search job params for the item
                    if job_params.get(item) is not None:
                        item_value = job_params.get(item)
                    else:
                        raise ValueError("{} value has not been evaluated by the preprocessor".format(item))

                output_context[group][item] = item_value
    else:
        raise KeyError("Key runconfig not found in PGE config file")

    # Add localized urls
    output_context[constants.LOCALIZE_KEY] = localize_paths(pge_config, output_context)
    return output_context


def _get_keys_from_dict(input_dict, keys=list()):
    """
    Returns a dict with the requested keys from the input dict
    :param input_dict:
    :param keys:
    :return:
    """
    new_dict = dict()
    for key in keys:
        if key in input_dict:
            new_dict.update({key: input_dict.get(key)})
    return new_dict


def _get_sps_name(pge_config):
    return pge_config.get(constants.RUNCONFIG_KEY).get("SPSNameGroup").get("SPSName")


def get_info2(context, pge_config):
    """

    :param context:
    :param job_params:
    :return:
    """
    job_inf_dict = dict()
    job_fields = pge_config.get(constants.JOB_INFO).get(constants.INFO_FIELDS)
    for field in job_fields:
        print(field)
        job_inf_dict[field] = context.get(field)
    return job_inf_dict
