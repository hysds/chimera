class PostProcessFunctions(object):
    def __init__(self, context, pge_config, settings, psuedo_context):
        self._context = context
        self._pge_config = pge_config
        self._settings = settings
        self._psuedo_context = psuedo_context

    def run(self, function_list):
        """
        Runs the set of post processing functions passed into the given list.

        :param function_list: A list of post process methods that will be defined in the subclasses.

        :return: a dictionary containing information about the results of the post PGE processes.
        """
        output_context = dict()
        for func in function_list:
            self._psuedo_context.update(getattr(self, func)())

        return self._psuedo_context
