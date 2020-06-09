import simplejson
from nisar_chimera import input_preprocessor as ipp

if __name__ == '__main__':
    sys_config = "../nisar_chimera/configs/sys.config.json"
    test_configs = list()

    # For testing without sfl_exec L0A
    context = simplejson.load(
        open("test-files/L0A_sfcontext.json", 'r'))
    ipp_def_filepath = "../nisar_chimera/configs/precondition_definition.yaml"
    pge_config = "../nisar_chimera/configs/pge_configs/PGE_L0A.yaml"
    settings_file = '../../nisar-pcm/conf/settings.yaml'
    test_configs.append((context, pge_config))

    # context = process_for_l0b_radiometer(context, simplejson.load(open(pge_config, 'r')))
    # Loop through all test configs
    for context, pge_config in test_configs:
        payload = ipp.process(sf_context=context, ipp_def_filepath=ipp_def_filepath, pge_config_filepath=pge_config,
                              sys_config_file=sys_config, settings_file=settings_file, testmode=True)

    print(simplejson.dumps(payload, indent=2))
