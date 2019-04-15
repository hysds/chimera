from commons import query_util, product_metadata, constants
import datetime
import os
import logging


"""
Contains all the utility functions required to generate
the spice files
"""

# Set up logging
LOGGER = logging.getLogger("inputpp")


def construct_sort_clause(clause):
    """
    Converts a string or list of strings of type "key:sort_order" to an ES sort statements
    Eg. version:desc ==> {"version": {"order": "desc"}}
    :param clause: str
    :return: list[dict]
    """
    result = list()
    if isinstance(clause, str):
        clause = [clause]
    for c in clause:
        c = str(c).split(":")
        result.append({c[0].strip(): {"order": c[1].strip()}})
    return result


def find_latest_version_for_day(product_type, dataBeginningDateTime=None, cal_string=None, index=query_util.GRQ_INDEX):
    """
    Select the latest versions of the given products with the same date as the input data:
    :param product_type:
    :param dataBeginningDateTime:
    :return:
    """
    if cal_string is None:
        input_date = convert_datetime(dataBeginningDateTime)
        day = input_date.replace(hour=0,  minute=0, second=0, microsecond=0)
        cal_string = "{}-{}".format(str(day.month).rjust(2, "0"), str(day.day).rjust(2, "0"))
    result = query_util.run_query(q="{}:\"{}\"".format(product_metadata.CALENDAR_DAY_STRING_METADATA, cal_string),
                                  doc_type=product_type,
                                  sort="version:desc",
                                  size=1, index=index)
    if len(result["hits"]["hits"]) > 0:
        return result["hits"]["hits"][0]

    return None


def find_closest(product_type, dataDateTime, datetime_key, comparator, term=None, value=None,
                 order="desc", custom_sort=None, index=None, conditions=None):
    """
    Finds closest record in the past or future based on comparator and get the latest version of that record
    :param product_type:
    :param dataDateTime:
    :param datetime_key:
    :param comparator:
    :param term:
    :param value:
    :param order:
    :param custom_sort: Eg.  clause should be of the form "version:desc"
    :param index:
    :return:
    """



    query = {"filter": {"range": {datetime_key: {comparator: dataDateTime}}}}
    if term:
        query.update({"query": {"match": {term: value}}})
    elif conditions:
        query.update(query_util.construct_bool_query(conditions))
    sort = list()
    sort.extend(construct_sort_clause("{}:{}".format(datetime_key, order)))
    if custom_sort:
        sort.extend(construct_sort_clause(custom_sort))
    sort.extend(construct_sort_clause("version:desc"))
    query["sort"] = sort
    result = query_util.run_query(body=query, doc_type=product_type, index=index, size=1)
    if len(result["hits"]["hits"]) > 0:
        return result["hits"]["hits"][0]

    return None


def find_within_range(product_type, dataBeginningDateTime, dataEndingDateTime,
                      datetime_start_key, datetime_end_key,
                      term=None, value=None, index=None, custom_sort=None):
    """
    Finds the file within the specified data beginning and ending time using the appropriate
    datetime keys (as used in the ES record)
    :param product_type:
    :param dataBeginningDateTime:
    :param dataEndingDateTime:
    :param datetime_start_key:
    :param datetime_end_key:
    :param term:
    :param value:
    :param custom_sort:
    :return:
    """
    query = {"filter": {"and": [
        {"range": {
            datetime_end_key: {"gt": dataBeginningDateTime}
        }},
        {"range": {
            datetime_end_key: {"lt": dataEndingDateTime}
        }}
    ]}}
    if custom_sort is None:
        custom_sort = "{}:desc".format(datetime_start_key)
    if term:
        query.update({"query": {"match": {term: value}}})
    result = query_util.run_query(index=index, body=query, doc_type=product_type,
                                  sort="{},version:desc".format(custom_sort),
                                  size=1)
    if len(result["hits"]["hits"]) > 0:
        return result["hits"]["hits"][0]

    return None


def find_earliest_in_future(product_type, dataEndingDateTime, datetime_key, term=None, value=None, custom_sort=None,
                            index=None, conditions=None):
    """

    :param product_type:
    :param dataEndingDateTime:
    :param datetime_key:
    :return:
    """
    return find_closest(product_type=product_type, dataDateTime=dataEndingDateTime, datetime_key=datetime_key,
                        comparator="gte", term=term, value=value, order="asc", custom_sort=custom_sort, index=index,
                        conditions=conditions)


def find_closest_to_past(product_type, dataBeginningDateTime, datetime_key, term=None, value=None,
                         index=None, custom_sort=None, conditions=None):
    """

    :param dataBeginningDateTime:
    :param datetime_key:
    :return:
    """
    return find_closest(product_type=product_type, dataDateTime=dataBeginningDateTime, datetime_key=datetime_key,
                        comparator="lte", term=term, value=value, index=index, custom_sort=custom_sort,
                        conditions=conditions)


def find_latest_record(records):
    """
    Finds the latest file by its RECONSTRUCT_END_DATE_TIME_METADATA, if this field is not
    present then it uses the RangeEndingTime
    :param records:
    :return:
    """
    latest = datetime.datetime.min
    bestRecord = None
    for rec in records:
        metadata = rec.get("_source").get("metadata")
        reconstructed_datetime = metadata.get(product_metadata.RECONSTRUCT_END_DATE_TIME_METADATA)
        if reconstructed_datetime is None:
            reconstructed_datetime = metadata.get(product_metadata.RANGE_END_DATETIME)

        reconstructed_datetime = convert_datetime(reconstructed_datetime)
        if latest < reconstructed_datetime:
            latest = reconstructed_datetime
            bestRecord = rec

    return bestRecord


def convert_datetime(datetime_obj, strformat="%Y-%m-%dT%H:%M:%S.%fZ"):
    if isinstance(datetime_obj, datetime.datetime):
        return datetime_obj.strftime(strformat)
    return datetime.datetime.strptime(str(datetime_obj), strformat)


def is_time_covered(dataBeginDateTime, dataEndDateTime, spice_record, gap_threshold=0):
    LOGGER.info("Determining if Spice record is a partial fit for data range {} to {}".format(
        dataBeginDateTime, dataEndDateTime
    ))
    dataBeginDateTime = convert_datetime(dataBeginDateTime)
    dataEndDateTime = convert_datetime(dataEndDateTime)

    spice_metadata = spice_record.get("metadata")
    if spice_metadata.get(product_metadata.RECONSTRUCT_END_DATE_TIME_METADATA) is not None or \
                    spice_metadata.get(product_metadata.RANGE_END_DATETIME) is not None:
        spice_begin_datetime = convert_datetime(spice_metadata.get(product_metadata.RANGE_BEGIN_DATETIME_METADATA))
        spice_end_datetime = convert_datetime(spice_metadata.get(product_metadata.RANGE_END_DATETIME))

        if spice_begin_datetime <= dataBeginDateTime and spice_end_datetime >= dataEndDateTime:
            LOGGER.info("Spice file with start={} and end={} completely covers the data time range"
                        .format(spice_begin_datetime, spice_end_datetime))
            return True
        # If it begins `gap_threshold` second(s) after the data begin time
        if spice_begin_datetime <= dataBeginDateTime + datetime.timedelta(gap_threshold) and \
                        spice_end_datetime >= dataEndDateTime:
            LOGGER.info(
                "Spice file with start={} and end={} completely covers the data time range with gap {} at the begning"
                    .format(spice_begin_datetime, spice_end_datetime, gap_threshold))
            return True

        # If it end `gap_threshold` second(s) before the data end time
        if spice_begin_datetime <= dataBeginDateTime and \
                        spice_end_datetime >= dataEndDateTime - datetime.timedelta(gap_threshold):
            LOGGER.info(
                "Spice file with start={} and end={} completely covers the data time range with gap {} at the end"
                    .format(spice_begin_datetime, spice_end_datetime, gap_threshold))
            return True

    return False


def is_partial(dataBeginDateTime, dataEndDateTime, spice_record):
    """
    Checks to see if the file partially covers the data date time ranges
    :param dataBeginDateTime:
    :param dataEndDateTime:
    :param spice_record:
    :return:
    """
    LOGGER.info("Determining if Spice record is a partial fit for data range {} to {}".format(
        dataBeginDateTime, dataEndDateTime
    ))
    dataBeginDateTime = convert_datetime(dataBeginDateTime)
    dataEndDateTime = convert_datetime(dataEndDateTime)
    beginsWithin = False
    endsWithin = False
    spice_metadata = spice_record.get("metadata")
    if spice_metadata.get(product_metadata.RECONSTRUCT_END_DATE_TIME_METADATA) is not None or \
                    spice_metadata.get(product_metadata.RANGE_END_DATETIME) is not None:
        spice_begin_datetime = convert_datetime(spice_metadata.get(product_metadata.RANGE_BEGIN_DATETIME_METADATA))
        spice_end_datetime = convert_datetime(spice_metadata.get(product_metadata.RANGE_END_DATETIME))

        if spice_begin_datetime >= dataBeginDateTime < dataEndDateTime:
            LOGGER.info("Spice record ")
            beginsWithin = True
        if spice_end_datetime > dataBeginDateTime <= dataEndDateTime:
            endsWithin = True

    return beginsWithin or endsWithin


def do_partials_provide_coverage(dataBeginDateTime, dataEndDateTime, partials, gap_threshold=0):
    """

    :param dataBeginDateTime:
    :param dataEndDateTime:
    :param partials:
    :param gap_threshold:
    :return:
    """
    LOGGER.info("Checking to see if partials provide coverage with gap={}".format(gap_threshold))
    dataBeginDateTime = convert_datetime(dataBeginDateTime)
    dataEndDateTime = convert_datetime(dataEndDateTime)

    # Sort partials based on start time
    partials.sort(key=lambda x: convert_datetime(x.get("_source").get("starttime")))
    currentStart = convert_datetime(partials[0].get("_source").get("starttime"))
    # If the first starts after the data begin time + gap threshold
    if currentStart > dataBeginDateTime + datetime.timedelta(seconds=gap_threshold):
        return False
    isCovered = False
    currentEnd = convert_datetime(partials[0].get("_source").get("endtime"))
    # Iterate to check if they cover data time range
    for rec in partials[1:]:
        if currentEnd >= dataEndDateTime:
            return True
        rec_begin_time = convert_datetime(rec.get("_source").get("starttime"))
        rec_end_time = convert_datetime(rec.get("_source").get("endtime"))

        # Check if next partial begins before the previous ends + gap threshold
        if currentEnd >= rec_begin_time - datetime.timedelta(seconds=gap_threshold):
            currentEnd = rec_end_time
        else:
            break

    return currentEnd >= dataEndDateTime


def select_best_fit(dataBeginDateTime, dataEndDateTime, spice_records, gap_threshold=0):
    """
    The logic to determine the best file to select is the following:

    If reconstructed data exists and the file completely covers the data datetime range, use that over predicted data.
    If multiple reconstructed data were found that completely cover the data datetime range, select the one with the
    latest reconstructed end datetime.
    If there are no files that completely cover the data datetime range, then check to see if the set of partials
    will complete the coverage.

    :param dataBeginDateTime:
    :param dataEndDateTime:
    :param spice_records:
    :param gap_threshold:
    :return: List of best fit ES records
    """
    LOGGER.info("Selecting best fit between {} to {} with gap threshold={} and {} spice files"
                .format(dataBeginDateTime, dataEndDateTime, gap_threshold, len(spice_records)))

    # Find the best fit file
    if len(spice_records) == 1:
        if is_time_covered(dataBeginDateTime, dataEndDateTime, spice_records[0].get("_source")):
            return spice_records

    # If multiple reconstructed data were found that completely cover the data datetime range,
    # select the one with the latest reconstructed end datetime.
    elif len(spice_records) > 1:
        complete_coverage_records = list()
        partials = list()
        for rec in spice_records:
            if is_time_covered(dataBeginDateTime, dataEndDateTime, rec.get("_source")):
                complete_coverage_records.append(rec)
            elif is_partial(dataBeginDateTime, dataEndDateTime, rec.get("_source")):
                partials.append(rec)
        if len(complete_coverage_records) > 0:
            # multiple reconstructed data were found that completely cover the data datetime range,
            # select the one with the latest reconstructed end datetime
            best_record = find_latest_record(complete_coverage_records)
            return [best_record]

        # If no complete were found, check if partials cover the data datetime range
        elif len(partials) > 0:
            if do_partials_provide_coverage(dataBeginDateTime=dataBeginDateTime, dataEndDateTime=dataEndDateTime,
                                            partials=partials, gap_threshold=gap_threshold):
                return partials
    return []


def get_latest_granules_by_version(dataBeginDateTime, dataEndDateTime, product_type, padding=0, index=None):
    """
    Gets the latest records covering fully or partially the given time range by distinct granules
    :param dataBeginDateTime:
    :param dataEndDateTime:
    :param product_type:
    :param padding:
    :return:
    """
    LOGGER.debug("Getting latest granules by version for {}".format(product_type))
    result = query_util.perform_es_range_intersection_query(
        product_type=product_type,
        beginningDateTime=dataBeginDateTime,
        endingDateTime=dataEndDateTime,
        padding=padding,
        size=1000,
        index=index)

    granules = set()
    for record in result.get("hits", {}).get("hits", []):
        granules.add(record.get("_source").get("metadata").get(product_metadata.GRANULE_NAME_METADATA))
    LOGGER.debug("Found {} granules {}".format(len(granules), ', '.join(granules)))

    records = list()
    # Get latest file for each granule found
    for granule in granules:
        record = query_util.get_latest_record_by_version(product_type,
                                                         lucene_query="{}:\"{}\"".format(product_metadata.GRANULE_NAME_METADATA,
                                                                                         granule), index=product_type.lower())
        records.extend(record.get("hits").get("hits"))
    return records


def get_data_volume_estimate(rangeBeginningDateTime):
    """

    :param rangeBeginningDateTime:
    :return:
    """
    LOGGER.info("Querying for {} from time {}".format(product_metadata.PRODUCT_TYPE_DATA_VOLUME_ESTIMATE,
                                                      rangeBeginningDateTime))
    index = constants.DATAVOLUME_ESTIMATE_ALIAS
    query = {"filter": {"range": {
        product_metadata.PRODUCTION_DATETIME_METADATA: {"lte": rangeBeginningDateTime}
    }}}
    result = query_util.run_query(body=query, sort="metadata.{}".format(product_metadata.PRODUCTION_DATETIME_METADATA),
                                  size=1, index=index)
    datastore_ref = query_util.get_datastore_refs(result)
    return convert_dataset_to_object(datastore_ref)


def find_closest_in_list(center_value, values, is_sorted=False):
    """
    Finds the value in the list of values closest to the center and returns the index of the closest element
    :param center_value:
    :param values:
    :return:
    """
    if not is_sorted:
        values = values.sort()
    min_difference, index = abs(center_value - values[0]), 0
    for i in range(1, len(values)):
        diff = abs(center_value - values[i])
        if diff < min_difference:
            index = i
    return index


def get_range_center_datetime(rangeBeginningDateTime, rangeEndingDateTime):
    """
    Computes the center date time (RangeBeginningDateTime + RangeEndingDateTime)/2
    :param rangeBeginningDateTime:
    :param rangeEndingDateTime:
    :return:
    """
    rangeBeginningDateTime = convert_datetime(rangeBeginningDateTime)
    rangeEndingDateTime = convert_datetime(rangeEndingDateTime)

    # To compute average of two date times we calculate the interval
    # Halve the interval and add it to the earlier time
    interval = rangeEndingDateTime - rangeBeginningDateTime
    interval /= 2
    return rangeBeginningDateTime + interval


def convert_dataset_to_object(dataset_path):
    """
    To convert a dataset s3 url to an object s3 url.
    The url stored in ES points to the dataset directory and not the actual object,
    hence this functions provides the conversion
    :param dataset_path:
    :return:
    """
    if isinstance(dataset_path, list):
        object_paths = list()
        for dataset in dataset_path:
            object_paths.append(convert_dataset_to_object(dataset))
        if len(object_paths) == 1:
            return object_paths[0]
        return object_paths
    dataset_name = os.path.basename(str(dataset_path))
    return os.path.join(str(dataset_path), dataset_name)
