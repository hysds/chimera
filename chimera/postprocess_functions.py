from chimera.commons.accountability import Accountability

class PostProcessFunctions(object):
    def __init__(self, context, pge_config, settings, psuedo_context):
        self._context = context
        self._pge_config = pge_config
        self._settings = settings
        self._psuedo_context = psuedo_context
        self.accountability = Accountability(self._context)

    def run(self, function_list):
        """
        Runs the set of post processing functions passed into the given list.

        :param function_list: A list of post process methods that will be defined in the subclasses.

        :return: a dictionary containing information about the results of the post PGE processes.
        """
        output_context = dict()
        logger.info(
            "function_list: {}".format(function_list)
        )
        for func in function_list:
            self._psuedo_context.update(getattr(self, func)())
        
        self.accountability.set_status("job-completed")
        return self._psuedo_context

    def _check_job_status(self):
        """
        Check if job is completed or deduped. If any other status then raise an error.
        :return:
        """
        # getting the job paylooad and status
        job_id = str(self._job_result["payload_id"])
        job_status = str(self._job_result["status"])

        logger.info("Recieved JOB ID: {} with status: {}".format(job_id, job_status))

        if job_status != "job-completed" and job_status != "job-deduped":
            logger.info(
                "Job with job_id: {} was not completed. Status: {}".format(
                    job_id, job_status
                )
            )
            raise ValueError(
                "Job with job_id: {} was not completed. Status: {}".format(
                    job_id, job_status
                )
            )
        return job_status

    def _get_job(self):
        """
        This function gets the staged products and context of previous PGE job
        :return: tuple(products_staged, prev_context, message)
        """
        job_id = str(self._job_result["payload_id"])
        endpoint = MOZART_ES_ENDPOINT
        return_job_id = None

        """
        Check if Jobs ES has updated job status and gets job information if completed/ deduped
        """
        try:
            if self._check_job_status():
                try:
                    response = self.query_es(endpoint=endpoint, doc_id=job_id)
                    # check if job not found
                    if len(response["hits"]["hits"]) == 0:
                        raise Exception(
                            "Couldn't find record with ID in MOZART: %s, at %s"
                            % (job_id, endpoint)
                        )
                except Exception as ex:
                    logger.error(
                        "Error querying MOZART for doc {}. {}. {}".format(
                            job_id, str(ex), traceback.format_exc()
                        )
                    )
                    raise Exception(
                        "Error querying MOZART for doc {}. {}".format(job_id, str(ex))
                    )
        except Exception as ex:
            logger.error(
                "Failed to find job in MOZART. {}. {}. {}".format(
                    job_id, str(ex), traceback.format_exc()
                )
            )
            raise Exception(
                "Failed to find job in MOZART. {}. {}. {}".format(
                    job_id, str(ex), traceback.format_exc()
                )
            )

        """
        Parse job's full information to get products staged, job context
        If job deduped then find original job's information
        """
        result = response["hits"]["hits"][0]
        products_staged = None
        prev_context = None
        message = None  # using this to store information regarding deduped jobs,
        status = str(result["_source"]["status"])

        # if job was deduped then find the original job status and what products (if any) were created
        if status == "job-deduped":
            logger.info("Job was deduped")
            # query ES for the original job's status
            orig_job_id = result["_source"]["dedup_job"]
            return_job_id = orig_job_id
            try:
                orig_job_info = self.query_es(endpoint=endpoint, doc_id=orig_job_id)
                if len(response["hits"]["hits"]) == 0:
                    raise Exception(
                        "Couldn't find record with ID: {}, at {}".format(
                            job_id, endpoint
                        )
                    )
            except Exception as ex:
                logger.error(
                    "Error querying ES for doc {}. {}. {}".format(
                        job_id, str(ex), traceback.format_exc()
                    )
                )
                raise Exception(
                    "Error querying ES for doc {}. {}".format(job_id, str(ex))
                )

            """
            check if original job failed -> this would happen when at the moment
            of deduplication, the original job was in 'running state', but soon
            afterwards failed. So, by the time the status is checked in this
            function, it may be shown as failed.
            """

            orig_job_info = orig_job_info["hits"]["hits"][0]
            orig_job_status = str(orig_job_info["_source"]["status"])
            if orig_job_status == "job-failed":
                message = (
                    "Job was deduped against a failed job with id: {},"
                    " please retry sciflo.".format(orig_job_id)
                )
                logger.info(
                    "Job was deduped against a job which has now failed "
                    "with id: {}, Please retry sciflo.".format(orig_job_id)
                )
            elif orig_job_status == "job-started" or orig_job_status == "job-queued":
                logger.info(
                    "Job was deduped against a queued/started job with "
                    "id: {}. Please look at already running sciflo with "
                    "same params.".format(orig_job_id)
                )
                message = (
                    "Job was deduped against a queued/started job with "
                    "id: {}. Please look at already running sciflo with "
                    "same params.".format(orig_job_id)
                )

            elif orig_job_status == "job-completed":
                products_staged = orig_job_info["_source"]["job"]["job_info"][
                    "metrics"
                ]["products_staged"]
                prev_context = orig_job_info["_source"]["context"]
                logger.info("Queried ES to get Job context and staged files info")
                message = "success"
        elif status == "job-completed":
            logger.info("Job completed")
            products_staged = result["_source"]["job"]["job_info"]["metrics"][
                "products_staged"
            ]
            prev_context = result["_source"]["context"]
            logger.info("Queried ES to get Job context and staged files info")
            message = "success"
            return_job_id = job_id
        else:
            logger.info(
                "Job was not completed. Status: {}".format(result["_source"]["status"])
            )
            message = "Job was not completed. Status: {}".format(
                result["_source"]["status"]
            )

        return products_staged, prev_context, message, return_job_id

    def _create_products_list(self, products):
        """
            This function creates a list of the product URLs and metadata required
            for the next PGE's input preprocessor.
            :param products: list of products staged after PGE run
            :return: tuple( product's id, list of products' URLs, list of products'
            metadata)
        """
        product_id = None
        products_url_list = []
        products_metadata_list = []

        for product in products:
            input_product_id = product["id"]
            # get information required for next PGE's input preprocessor
            product_id = input_product_id

            try:
                product_url, metadata = self.get_product_info(
                    product_id=input_product_id
                )
                product_info = dict()
                product_info["id"] = input_product_id
                product_info["url"] = product_url
                product_info["metadata"] = metadata
                products_metadata_list.append(product_info)
                products_url_list.append(product_url)
            except Exception as ex:
                raise Exception(
                    "Failed to get product information, {}. {}".format(
                        str(ex), traceback.format_exc()
                    )
                )

        return product_id, products_url_list, products_metadata_list

    def core_post_process_steps(self):
        """
        The mandatory post processing steps of Chimera are:
        1. check submitted job's status
        2. Get complete job run information i.e job's context, products produced, job id
            (in case job submitted by sciflo was deduped, the original job ID is tracked down)
        3.

        :return:
        """
        pseudo_context = dict()

        """
        check submitted job's status
        """
        job_status = self._check_job_status()

        """
        get the products staged, context of job and job's ID (incase job submitted by sciflo was deduped)
        """
        try:
            products, prev_context, message, job_id = self._get_job()
        except Exception as ex:
            logger.error(
                "Couldn't get job info for {}. {}. {}".format(
                    job_id, str(ex), traceback.format_exc()
                )
            )
            job_status_code = -1
            logger.error("Job was not found.")
            raise RuntimeError(
                "Couldn't get job info for {}. {}. {}".format(
                    job_id, str(ex), traceback.format_exc()
                )
            )

        """
        Handle job status codes for all outcomes of a deduped job
        # Case 1: if the original job is queued or has started, fail the current sciflo
                  so that the original workflow can take care of the PGE run
                  update the job status with -3
                  
        # Case 2: if the original job has completed, then get products and prev _context from original job
                  update job_status_code to 2
        
        # Case 3: if the original job was deduped (NOTE: Unlikely unrealistic case)
                   set job_status_code to -2
        """
        # case 1
        if products is None and prev_context is None:
            job_status_code = -3
            raise RuntimeError(message)
        else:
            # case 2
            if job_status == "job-completed":
                job_status_code = 2
            # case 3
            elif job_status == "job-deduped":
                job_status_code = -2

        """
        Query to get information of all products staged.
        NOTE: Sciflo gets notification of the celery task completion and moves to the post processing step when
              it gets the PGE job submission results. The completion of a celery task is different than completion of
              a HySDS job. A HySDS job includes the celery task execution, worker post processing and dataset ingestion.
        We get to the step of querying a product's information before it has been indexed into GRQ. To handle this race
        condition we have an exponential backoff logic in the query to wait for the product to appear.
        Max wait time is 2 mins.
        """
        try:
            (
                product_id,
                products_url_list,
                products_metadata_list,
            ) = self._create_products_list(products=products)
        except Exception as ex:
            job_status_code = -1
            logger.error("Setting Job failure status code as product was not found.")
            raise RuntimeError(
                "Failed PGE run as products list could not be made."
                " {}. {}".format(str(ex), traceback.format_exc())
            )

        """
        Now that we have all job and products information we can put the psuedo context contents together.
        """
        logger.info("Job Status Code: {}".format(job_status_code))
        product_url_key = ChimeraConstants.PRODUCT_PATHS
        metadata_key = ChimeraConstants.PRODUCTS_METADATA

        pseudo_context[product_url_key] = products_url_list
        pseudo_context[metadata_key] = products_metadata_list
        pseudo_context["job_id"] = job_id
        pseudo_context["job_context"] = prev_context

        return pseudo_context
