from chimera.commons.accountability import Accountability

class PreConditionFunctions(object):
    def __init__(self, context, pge_config, settings, job_params):
        self._context = context
        self._pge_config = pge_config
        self._settings = settings
        self._job_params = job_params
        self.accountability = Accountability(self._context)

    def run(self, function_list):
        """
        Runs the set of preconditions passed into the given list.

        :param function_list: A list of precondition methods that will be defined in the subclasses.

        :return: a dictionary containing information about the results of the precondition evaluations.
        """
        self.accountability.set_status("job-pp-started")
        for func in function_list:
            self._job_params.update(getattr(self, func)())

        return self._job_params
