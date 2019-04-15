import elasticsearch
import json
import traceback
import datetime
import logging
from commons import product_metadata
# from hysds.celery import app


# ES_URL = app.conf.get("GRQ_ES_URL", "http://localhost:9200")
ES_URL = "http://localhost:9200"
ES = elasticsearch.Elasticsearch(ES_URL)
GRQ_INDEX = "grq"
PRODUCT_COUNTER_INDEX = "product_counter"
ORBIT_INDEX = ""
ORBIT_STATUS_INDEX = ""
ORBIT_DOC_TYPE = ""

LOGGER = logging.getLogger("inputpp")


def get_datastore_ref_from_es_record(es_record):
    """
    Returns a list of S3 URLs in the "url" field of the ES record
    :param es_record:
    :return: []
    """
    refs = list()
    if es_record is None:
        raise RuntimeError("No ES record found")
    doc = es_record.get("_source", {})
    for url in doc.get("urls", []):
        if str(url).startswith("s3://"):
            refs.append(url)
    return refs


def get_datastore_refs_from_es_records(records):
    refs = []
    for record in records:
        refs.extend(get_datastore_ref_from_es_record(record))
    return refs


def get_datastore_refs(result):
    """
    Returns a list of S3 url of product from the ES result hits
    :param result:
    :return: list
    """
    refs = []
    hits = result.get("hits", {}).get("hits", [])
    for hit in hits:
        refs.extend(get_datastore_ref_from_es_record(hit))
    return refs


def get_latest_record_by_version(product_type, es_query=None, lucene_query=None, index=GRQ_INDEX, version_metadata='version', **kwargs):
    LOGGER.debug("Getting latest version for {} with query {} (ES) {} (lucene)".format(
        product_type, es_query, lucene_query))
    return run_query(index=index, body=es_query, q=lucene_query, doc_type=product_type, sort="{}:desc".format(version_metadata),
                     size=1, **kwargs)


def get_latest_pge_product_by_counter(query, product_type):
    """
    To get the latest product of a PGE by a specified query and product type
    :param query:
    :param product_type:
    :return: Returns the S3 url of the result
    """
    LOGGER.info("Getting latest PGE product for {}".format(product_type))
    source_includes = ["urls"]
    sort = {"sort": {product_metadata.PRODUCT_COUNTER_METADATA: {"order": "desc"}}}
    query.update(sort)
    index = "products"
    result = run_query(body=query, doc_type=product_type, index=index, size=1)
    if result.get("hits").get("total") == 1:
        doc = result.get("hits").get("hits")[0]
        datastore_ref = get_datastore_ref_from_es_record(doc)
        return str(datastore_ref[0])
    else:
        LOGGER.warn("Did not find exactly one result for {}".format(product_type))
        return None


def get_latest_product_by_version(product_type, index, es_query=None, version_metadata='version'):
    """
    Queries the GRQ index for the latest version of a product type.
    Assuming the product metadata has a field called "version"
    :param product_type:
    :param index:
    :param es_query:
    :param version_metadata:
    :return: S3 url
    """
    source_includes = ["urls"]
    result = get_latest_record_by_version(product_type, index=index, es_query=es_query, version_metadata=version_metadata, _source_include=source_includes)
    datastore_refs = get_datastore_refs(result)
    if len(datastore_refs) == 0:
        raise ValueError("Could not find any datastore refs for {}".format(product_type))
    return str(datastore_refs[0])


def construct_range_entirely_within_bounds_query(beginningDateTime, endingDateTime,
                                                 startDateTimeKey="metadata.RangeBeginningDateTime",
                                                 endDateTimeKey="metadata.RangeEndingDateTime"):
    """
    Constructs an ES filter query for data entirely within the given bounds
    :param beginningDateTime:
    :param endingDateTime:
    :param startDateTimeKey:
    :param endDateTimeKey:
    :return:
    """
    _filter = range_entirely_within_bounds(beginningDateTime, endingDateTime, startDateTimeKey, endDateTimeKey)
    query = {"filter": _filter}
    return query


def range_entirely_within_bounds(beginningDateTime, endingDateTime, startDateTimeKey="metadata.RangeBeginningDateTime",
                                 endDateTimeKey="metadata.RangeEndingDateTime"):
    """
    Returns ES range filter query for data entirely within the specified bound.
    RangeBeginningDateTime >= {beginningDateTime} AND RangeEndingDateTime <= {endingDateTime}
    :param beginningDateTime:
    :param endingDateTime:
    :return:
    """
    _filter = {
        "and": [
            {
                "range": {
                    startDateTimeKey: {
                        "gte": beginningDateTime
                    }
                }
            },
            {
                "range": {
                    endDateTimeKey: {
                        "lte": endingDateTime
                    }
                }
            }
        ]
    }
    return _filter


def range_starts_within_bounds(beginningDateTime, endingDateTime):
    """
    Returns ES range filter query for data starting within the specified bound.
    RangeBeginningDateTime >= {beginningDateTime} AND RangeBeginningDateTime <= {endingDateTime}
    :param beginningDateTime:
    :param endingDateTime:
    :return:
    """
    _filter = {
        "and": [
            {
                "range": {
                    "metadata.RangeBeginningDateTime": {
                        "gte": beginningDateTime
                    }
                }
            },
            {
                "range": {
                    "metadata.RangeBeginningDateTime": {
                        "lte": endingDateTime
                    }
                }
            }
        ]
    }

    return _filter


def range_ends_within_bounds(beginningDateTime, endingDateTime):
    """
    Returns ES range filter query for data ending within the specified bound.
    RangeEndingDateTime <= {endingDateTime} AND RangeEndingDateTime >= {beginningDateTime}
    :param beginningDateTime:
    :param endingDateTime:
    :return:
    """
    _filter = {
        "and": [
            {
                "range": {
                    "metadata.RangeEndingDateTime": {
                        "gte": beginningDateTime
                    }
                }
            },
            {
                "range": {
                    "metadata.RangeEndingDateTime": {
                        "lte": endingDateTime
                    }
                }
            }
        ]
    }

    return _filter


def range_covers_entire_bounds(beginningDateTime, endingDateTime):
    """
    Returns ES range filter query for data entirely covering the specified bound.
    RangeBeginningDateTime <= {beginningDateTime} AND RangeEndingDateTime >= {endingDateTime}
    :param beginningDateTime:
    :param endingDateTime:
    :return:
    """
    _filter = {
        "and": [
            {
                "range": {
                    "metadata.RangeBeginningDateTime": {
                        "lte": beginningDateTime
                    }
                }
            },
            {
                "range": {
                    "metadata.RangeEndingDateTime": {
                        "gte": endingDateTime
                    }
                }
            }
        ]
    }
    return _filter


def get_datetime_coverage_filter(beginningDateTime, endingDateTime, padding=0):
    """
    Generates a ES filter query covering the bounds for the specified begin and end date time
    :param beginningDateTime:
    :param endingDateTime:
    :param padding: seconds to pad the beginningDateTime by. Used for L1A_Radiometer and L1A_Radar
    :return:
    """
    filter = {"or":{}}


def perform_es_range_intersection_query(product_type, beginningDateTime, endingDateTime, padding=0, sort=None,
                                        size=None, index=GRQ_INDEX):
    if padding > 0:
        beginningDateTime = pad_datetime(beginningDateTime, padding)
    query = {}
    _filters = list()
    _filters.append(range_covers_entire_bounds(beginningDateTime, endingDateTime))
    _filters.append(range_ends_within_bounds(beginningDateTime, endingDateTime))
    _filters.append(range_entirely_within_bounds(beginningDateTime, endingDateTime))
    _filters.append(range_starts_within_bounds(beginningDateTime, endingDateTime))
    query["filter"] = {"or": _filters}

    return run_query(index=index, doc_type=product_type, body=query, sort=sort, size=size)


def get_fully_or_partially_covered_products(product_type, beginningDateTime, endingDateTime, padding=0,
                                            sort=None, size=None, index=GRQ_INDEX):
    """

    :param beginningDateTime:
    :param endingDateTime:
    :param padding:
    :return:
    """

    result = perform_es_range_intersection_query(product_type, beginningDateTime, endingDateTime, padding, sort, size,
                                                 index=index)

    datastore_refs = get_datastore_refs(result)
    if len(datastore_refs) == 0:
        raise ValueError("Could not find any datastore refs for {} within date range {} to {}"
                         .format(product_type, beginningDateTime, endingDateTime))
    return datastore_refs

def match_clause(key, value):
        return {"match": {key: value}}


def construct_bool_query(conditions, boolean_type="must", query_type="match"):
    """
    Constructs an ES bool "must"(deafult) query containing a list of clauses for query type match(deafult) for the
    conditions specified
    :param conditions: A list of key-value tuples or a dictionary
    :return: ES query dictionary object
    """
    if conditions is None or len(conditions) == 0:
        return {}

    clauses = []
    query = None

    # Proceed only if conditions is a list or dictionary
    if isinstance(conditions, list) and len(conditions) > 0:
        if isinstance(conditions[0], tuple):
            for cond in conditions:
                if query_type == "match":
                    clauses.append(match_clause(cond[0], cond[1]))
    elif isinstance(conditions, dict):
        for key, value in conditions.iteritems():
            if query_type == "match":
                clauses.append(match_clause(key, value))
    if boolean_type == "must":
        query = {"query": {"bool": {"must": clauses}}}

    if query is not None:
        return query
    else:
        raise RuntimeError("Unable to construct query with given conditions")


def delete_doc(id, index=GRQ_INDEX, doc_type=None):
    try:
        result = ES.delete(id=id, index=index, doc_type=doc_type)
    except:
        return False

    return result["found"]


def run_query(body=None, doc_type=None, q=None, sort=None, size=10, index=GRQ_INDEX, request_timeout=30, retried=False, **kwargs):
    try:
        LOGGER.debug("Query ES with params: {}".format(json.dumps(locals())))
        if sort:
            sort = sort.replace(" ", "")
            if q and body is None:
                result = ES.search(index=index, doc_type=doc_type, sort=sort, size=size, q=q, request_timeout=request_timeout, **kwargs)
            else:
                result = ES.search(index=index, body=body, doc_type=doc_type, sort=sort, size=size, request_timeout=request_timeout, **kwargs)
        else:
            if q and body is None:
                result = ES.search(index=index, doc_type=doc_type, size=size, q=q, request_timeout=request_timeout, **kwargs)
            else:
                result = ES.search(index=index, body=body, doc_type=doc_type, size=size, request_timeout=request_timeout, **kwargs)
        if result.get("timed_out", True):
            LOGGER.warn("ES responded with a timed out result, retrying....: {}".format(json.dumps(result)))
            raise RuntimeWarning("ES responded with a timed out result, retrying....")
        return result
    except Exception as e:
        LOGGER.warn("Caught exception from elasticsearch retrying: {}".format(traceback.format_exc()))
        # Retry querying, this is incase ES takes too long to respond
        if not retried:
            run_query(body=body, doc_type=doc_type, q=q, sort=sort, size=size, index=index,
                      request_timeout=int(request_timeout+30), retried=True, **kwargs)
        else:
            raise Exception(str(e))


def update_doc(body=None, index=None, doc_type=None, doc_id=None):
    ES.update(index= index, doc_type= doc_type, id=doc_id,
              body=body)


def pad_datetime(datetime_string, padding):
    beginningDateTime = datetime.datetime.strptime(datetime_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    beginningDateTime += datetime.timedelta(seconds=padding)
    return beginningDateTime.strftime("%Y-%m-%dT%H:%M:%SZ")

