from commons import query_util

def get_latest_orbit_record():
    """
    Get the latest half_orbit record, if no record found return doc with
    StartOrbitNumber=0 and OrbitDirection=NULL
    :return: ES doc
    """
    return


def get_record_by_id(doc_id):
    """
    Get the ES record from the "orbits" index for the provided doc_id
    :param doc_id:
    :return: ES record in as a python dict, if any or else None
    """
    return


def get_eclipse_times(orbit_number, orbit_direction):
    """

    :param orbit_number:
    :param orbit_direction:
    :return:
    """
    return
