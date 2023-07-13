import json
import os
import logging
import logging_utils
from dbclient import *
import wmconstants

class JobsClient(ClustersClient):

    def get_jobs_default_cluster_conf(self):
        if self.is_aws() and not self.hipaa:
            cluster_json_file = 'data/default_jobs_cluster_aws.json'
        elif self.is_aws() and self.hipaa:
            cluster_json_file = 'data/default_jobs_cluster_aws_hipaa.json'
        elif self.is_azure():
            cluster_json_file = 'data/default_jobs_cluster_azure.json'
        elif self.is_gcp():
            cluster_json_file = 'data/default_jobs_cluster_gcp.json'
        with open(cluster_json_file, 'r', encoding="utf-8") as fp:
            cluster_json = json.loads(fp.read())
            return cluster_json

    def get_jobs_list(self, print_json=False):
        """ 
        Returns an array of json objects for jobs. It might contain jobs in SINGLE_TASK and 
        MULTI_TASK format. 
        """
        jobsById = {}
        # fetch all jobs using API 2.0. The 'format' field of each job can either be SINGLE_TASK 
        # or MULTI_TASK. MULTI_TASK jobs, however, are returned without task definitions (the 
        # 'tasks' field) on API 2.0.
        res = self.get("/jobs/list", print_json, version='2.0')
        for job in res.get('jobs', []):
            jobsById[job.get('job_id')] = job

        limit = 25 # max limit supported by the API
        offset = 0
        has_more = True
        # fetch all jobs again, this time using API 2.1, in order to get MULTI_TASK jobs with 
        # task definitions. Note that the 'format' field will be set as MULTI_TASK for all jobs 
        # and the 'tasks' field will be present for all jobs as well
        while has_more:
            res = self.get(f'/jobs/list?expand_tasks=true&offset={offset}&limit={limit}', print_json, version='2.1')
            offset += limit
            has_more = res.get('has_more')
            for job in res.get('jobs', []):
                jobId = job.get('job_id')
                # only replaces "real" MULTI_TASK jobs, as they contain the task definitions.
                if jobsById[jobId]['settings'].get('format') == 'MULTI_TASK':
                    jobsById[jobId] = job
        return jobsById.values()

    def get_job_id_by_name(self):
        """
        get a dict mapping of job name to job id for the new job ids
        :return:
        """
        jobs = self.get_jobs_list()
        job_ids = {}
        for job in jobs:
            job_ids[job['settings']['name']] = job['job_id']
        return job_ids

    def update_imported_job_names(self, error_logger, checkpoint_job_configs_set):
        # loop through and update the job names to remove the custom delimiter + job_id suffix
        current_jobs_list = self.get_jobs_list()
        for job in current_jobs_list:
            job_id = job['job_id']
            job_name = job['settings']['name']
            # job name was set to `old_job_name:::{job_id}` to support duplicate job names
            # we need to parse the old job name and update the current jobs
            if checkpoint_job_configs_set.contains(job_name):
                continue
            old_job_name = job_name.split(':::')[0]
            new_settings = {'name': old_job_name}
            update_args = {'job_id': job_id, 'new_settings': new_settings}
            logging.info(f'Updating job name: {update_args}')
            resp = self.post('/jobs/update', update_args)
            if not logging_utils.log_response_error(error_logger, resp):
                checkpoint_job_configs_set.write(job_name)
            else:
                raise RuntimeError("Import job has failed. Refer to the previous log messages to investigate.")

    def log_job_configs(self, users_list=None, groups_list = None, log_file='jobs.log', acl_file='acl_jobs.log'):
        """
        log all job configs and the ACLs for each job
        :param users_list: a list of users / emails to filter the results upon (optional for group exports)
        :param groups_list: a list of groups to filter the results upon (resolves to users)
        :param log_file: log file to store job configs as json entries per line
        :param acl_file: log file to store job ACLs
        :return:
        """
        if users_list is None:
            users_list = []

        # if groups_to_keep is provided, get users_list based on groups_list
        if groups_list is not None:
            all_users = self.get('/preview/scim/v2/Users').get('Resources', None)
            users_list = list(set([user.get("emails")[0].get("value") for user in all_users
                                   for group in user.get("groups") if group.get("display") in groups_list]))

        jobs_log = self.get_export_dir() + log_file
        acl_jobs_log = self.get_export_dir() + acl_file
        error_logger = logging_utils.get_error_logger(wmconstants.WM_EXPORT, wmconstants.JOB_OBJECT, self.get_export_dir())
        acl_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.JOB_ACL_OBJECT, self.get_export_dir()
        )
        # pinned by cluster_user is a flag per cluster
        jl_full = self.get_jobs_list(False)
        if users_list:
            # filter the jobs list to only contain users that exist within this list
            jl = list(filter(lambda x: x.get('creator_user_name', '') in users_list, jl_full))
        else:
            jl = jl_full
        with open(jobs_log, "w", encoding="utf-8") as log_fp, open(acl_jobs_log, 'w', encoding="utf-8") as acl_fp:
            for x in jl:
                job_id = x['job_id']
                new_job_name = x['settings']['name'] + ':::' + str(job_id)
                # grab the settings obj
                job_settings = x['settings']
                # update the job name
                job_settings['name'] = new_job_name
                # reset the original struct with the new settings
                x['settings'] = job_settings

                # get ACLs and check that the job has one owner before writing
                job_perms = self.get(f'/preview/permissions/jobs/{job_id}')
                if not logging_utils.log_response_error(error_logger, job_perms):
                    valid_acl = False
                    acls = job_perms.get("access_control_list")
                    if acls:
                        for acl in acls:
                            for permission in acl.get("all_permissions"):
                                if permission.get("permission_level") == "IS_OWNER":
                                    valid_acl = True
                    if valid_acl:
                        # job and job_acl are fine, writing both to the output files
                        log_fp.write(json.dumps(x) + '\n')

                        job_perms['job_name'] = new_job_name
                        acl_fp.write(json.dumps(job_perms) + '\n')
                    else:
                        # job_acl is malformed, the job is written to error output file
                        message = f"The following job id {job_id} has malformed permissions: {json.dumps(job_perms)}"
                        logging.error(message)
                        logging_utils.log_response_error(acl_error_logger, {
                            'error': message
                        })
                        logging_utils.log_response_error(error_logger, {
                            'error': message, 'json': json.dumps(x)
                        })

    def import_job_configs(self, log_file='jobs.log', acl_file='acl_jobs.log', job_map_file='job_id_map.log'):
        jobs_log = self.get_export_dir() + log_file
        acl_jobs_log = self.get_export_dir() + acl_file
        job_map_log = self.get_export_dir() + job_map_file
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.JOB_OBJECT, self.get_export_dir())
        job_acl_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.JOB_ACL_OBJECT, self.get_export_dir())
        if not os.path.exists(jobs_log):
            logging.info("No job configurations to import.")
            return
        # get an old cluster id to new cluster id mapping object
        cluster_mapping = self.get_cluster_id_mapping()
        old_2_new_policy_ids = self.get_new_policy_id_dict()  # dict { old_policy_id : new_policy_id }
        checkpoint_job_configs_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_IMPORT, wmconstants.JOB_OBJECT)

        def adjust_ids_for_cluster(settings): #job_settings or task_settings
            """
            The task setting may have existing_cluster_id/new_cluster/job_cluster_key for cluster settings.
            The job level setting may have existing_cluster_id/new_cluster for cluster settings.
            Adjust cluster settings for existing_cluster_id and new_cluster scenario.
            """
            if 'existing_cluster_id' in settings:
                old_cid = settings['existing_cluster_id']
                # set new cluster id for existing cluster attribute
                new_cid = cluster_mapping.get(old_cid, None)
                if not new_cid:
                    logging.info("Existing cluster has been removed. Resetting job to use new cluster.")
                    settings.pop('existing_cluster_id')
                    settings['new_cluster'] = self.get_jobs_default_cluster_conf()
                else:
                    settings['existing_cluster_id'] = new_cid
            elif 'new_cluster' in settings:  # new cluster config
                cluster_conf = settings['new_cluster']
                if 'policy_id' in cluster_conf:
                    old_policy_id = cluster_conf['policy_id']
                    cluster_conf['policy_id'] = old_2_new_policy_ids[old_policy_id]
                # check for instance pools and modify cluster attributes
                if 'instance_pool_id' in cluster_conf:
                    new_cluster_conf = self.cleanup_cluster_pool_configs(cluster_conf, job_creator, True)
                else:
                    new_cluster_conf = cluster_conf
                settings['new_cluster'] = new_cluster_conf
            return settings

        with open(jobs_log, 'r', encoding="utf-8") as fp, open(job_map_log, 'w', encoding="utf-8") as jm_fp:
            for line in fp:
                job_conf = json.loads(line)
                # need to do str(...), otherwise the job_id is recognized as integer which becomes
                # str vs int which never matches.
                # (in which case, the checkpoint never recognizes that the job_id is already checkpointed)
                if 'job_id' in job_conf and checkpoint_job_configs_set.contains(str(job_conf['job_id'])):
                    continue
                job_creator = job_conf.get('creator_user_name', '')
                job_settings = job_conf['settings']
                job_schedule = job_settings.get('schedule', None)
                if job_schedule:
                    # set all imported jobs as paused
                    job_schedule['pause_status'] = 'PAUSED'
                    job_settings['schedule'] = job_schedule
                job_schedule_continuous = job_settings.get("continuous", None)
                if job_schedule_continuous: 
                    # set all import jobs as paused
                    job_schedule_continuous['pause_status'] = "PAUSED" 
                    job_settings['continuous'] = job_schedule_continuous
                
                    
                if 'format' not in job_settings or job_settings.get('format') == 'SINGLE_TASK':
                    adjust_ids_for_cluster(job_settings)
                else:
                    mod_task_settings = []
                    for task_settings in job_settings.get('job_clusters', []):
                        mod_task_settings.append(adjust_ids_for_cluster(task_settings))
                    if len(mod_task_settings) > 0:
                        job_settings['job_clusters'] = mod_task_settings
                        mod_task_settings = []

                    # multi-task jobs may have existing_cluster_id per task
                    for task_settings in job_settings.get('tasks', []):
                        mod_task_settings.append(adjust_ids_for_cluster(task_settings))
                    if len(mod_task_settings) > 0:
                        job_settings['tasks'] = mod_task_settings

                logging.info("Current Job Name: {0}".format(job_conf['settings']['name']))
                # creator can be none if the user is no longer in the org. see our docs page
                create_resp = self.post('/jobs/create', job_settings)
                if logging_utils.check_error(create_resp):
                    logging.info("Resetting job to use default cluster configs due to expired configurations.")
                    if job_settings.get("format", "") == "MULTI_TASK":

                        # if an MTJ has a cluster that no longer exists, use the default configuration for all tasks
                        updated_tasks = []
                        for task in job_settings.get("tasks"):
                            if task.get("existing_cluster_id", None):
                                task.pop("existing_cluster_id")
                            task["new_cluster"] = self.get_jobs_default_cluster_conf()
                            updated_tasks.append(task)
                        job_settings["tasks"] = updated_tasks
                    else:
                        job_settings['new_cluster'] = self.get_jobs_default_cluster_conf()

                    create_resp_retry = self.post('/jobs/create', job_settings)
                    if not logging_utils.log_response_error(error_logger, create_resp_retry):
                        if 'job_id' in job_conf:
                            checkpoint_job_configs_set.write(job_conf["job_id"])
                    else:
                        raise RuntimeError("Import job has failed. Refer to the previous log messages to investigate.")

                else:
                    if 'job_id' in job_conf:
                        checkpoint_job_configs_set.write(job_conf["job_id"])
                    _job_map = {"old_id": job_conf["job_id"], "new_id": str(create_resp["job_id"])}
                    jm_fp.write(json.dumps(_job_map) + '\n')


        # update the jobs with their ACLs
        with open(acl_jobs_log, 'r', encoding="utf-8") as acl_fp:
            job_id_by_name = self.get_job_id_by_name()
            for line in acl_fp:
                acl_conf = json.loads(line)
                if 'object_id' in acl_conf and checkpoint_job_configs_set.contains(acl_conf['object_id']):
                    continue
                current_job_id = job_id_by_name[acl_conf['job_name']]
                job_path = f'jobs/{current_job_id}'  # contains `/jobs/{job_id}` path
                api = f'/preview/permissions/{job_path}'
                # get acl permissions for jobs
                acl_perms = self.build_acl_args(acl_conf['access_control_list'], True)
                acl_create_args = {'access_control_list': acl_perms}
                acl_resp = self.patch(api, acl_create_args)
                if not logging_utils.log_response_error(job_acl_error_logger, acl_resp) and 'object_id' in acl_conf:
                    checkpoint_job_configs_set.write(acl_conf['object_id'])
                else:
                    if self.is_skip_failed():
                        logging.error(f"Skipped {acl_conf}")
                    else:
                        raise RuntimeError("Import job has failed. Refer to the previous log messages to investigate.")
        # update the imported job names
        self.update_imported_job_names(error_logger, checkpoint_job_configs_set)

    def import_pause_status(self, log_file='jobs.log', job_map_file='job_id_map.log'):
        log_file = self.get_export_dir() + log_file
        job_map_file = self.get_export_dir() + job_map_file


        if not os.path.exists(log_file) or not os.path.exists(job_map_file):
            raise ValueError('Jobs log and jobs id map must exist to map jobs to previous existing jobs ids')

        job_map_log = self._load_job_id_map(job_map_file)
        
        with open(log_file, 'r', encoding="utf-8") as fp:
            for line in fp:
                job_conf = json.loads(line)
                new_job_id = job_map_log[job_conf['job_id']]
                job_settings = job_conf['settings']
                update_job_conf = {'job_id': new_job_id,
                                   'new_settings': job_settings}
                update_job_resp = self.post('/jobs/reset', update_job_conf)

    def _load_job_id_map(self, job_id_map_log):
        id_map = {}
        with open(job_id_map_log, 'r', encoding="utf-8") as fp:
            for single_id_map_str in fp:
                single_id_map = json.loads(single_id_map_str)
                id_map[single_id_map["old_id"]] = single_id_map["new_id"]
        return id_map

    def pause_all_jobs(self, pause=True):
        job_list = self.get_jobs_list()
        for job_conf in job_list:
            job_settings = job_conf['settings']
            job_schedule = job_settings.get('schedule', None)
            if job_schedule:
                # set all imported jobs as paused or un-paused
                if pause:
                    job_schedule['pause_status'] = 'PAUSED'
                else:
                    job_schedule['pause_status'] = 'UNPAUSED'
                job_settings['schedule'] = job_schedule
                update_job_conf = {'job_id': job_conf['job_id'],
                                   'new_settings': job_settings}
                update_job_resp = self.post('/jobs/reset', update_job_conf)

    def delete_all_jobs(self):
        job_list = self.get('/jobs/list').get('jobs', [])
        for job in job_list:
            self.post('/jobs/delete', {'job_id': job['job_id']})
