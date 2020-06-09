import simplejson
import sys

from smap_sciflo import input_preprocessor as ipp


if __name__ == '__main__':

    context_file = "test-files/sf_context.json"
    context = simplejson.load(open(context_file, 'r'))
    pge_config = simplejson.load(open("../configs/PGE_TSURF.json", "r"))
    # context = process_for_l0b_radiometer(context, simplejson.load(open(pge_config, 'r')))
    job_params = ipp.get_product_counter(pge_config, context)
    # test output of get_product_metadata)_
    print(job_params)
