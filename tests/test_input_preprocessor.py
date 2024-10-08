import simplejson
from chimera import input_preprocessor as ipp

if __name__ == '__main__':
    sys_config = "../nisar_chimera/configs/sys.config.json"
    test_configs = list()

    # For testing without sfl_exec L0A
    context = simplejson.load(
        open("test-files/L0B_datatake.json", 'r'))
    #ipp_def_filepath = "../nisar_chimera/configs/precondition_definition.yaml"
    chimera_config = "/Users/mcayanan/git/nisar-pcm/nisar_chimera/configs/chimera_config.yaml"
    pge_config = "/Users/mcayanan/git/nisar-pcm/nisar_chimera/configs/pge_configs/PGE_L0B.yaml"
    settings_file = '/Users/mcayanan/git/nisar-pcm/conf/settings.yaml'
    test_configs.append((context, pge_config))

    # context = process_for_l0b_radiometer(context, simplejson.load(open(pge_config, 'r')))
    # Loop through all test configs
    for context, pge_config in test_configs:
        payload = ipp.process(sf_context=context, chimera_config_file=chimera_config, pge_config_filepath=pge_config,
                              settings_file=settings_file)

        print(simplejson.dumps(payload, indent=2))
