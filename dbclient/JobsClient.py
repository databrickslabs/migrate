import os

from dbclient import *


class JobsClient(ClustersClient):

    def get_jobs_default_cluster_conf(self):
        if self.is_aws():
            cluster_json_file = 'data/default_jobs_cluster_aws.json'
        else:
            cluster_json_file = 'data/default_jobs_cluster_azure.json'
        with open(cluster_json_file, 'r') as fp:
            cluster_json = json.loads(fp.read())
            return cluster_json

    def get_jobs_list(self, print_json=False):
        """ Returns an array of json objects for jobs """
        jobs = self.get("/jobs/list", print_json)
        return jobs.get('jobs', [])

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

    def log_job_configs(self, log_file='jobs.log', acl_file='acl_jobs.log'):
        """
        log all job configs and the ACLs for each job
        :param log_file:
        :param acl_file:
        :return:
        """
        jobs_log = self.get_export_dir() + log_file
        acl_jobs_log = self.get_export_dir() + acl_file
        # pinned by cluster_user is a flag per cluster
        jl = self.get_jobs_list(False)
        with open(jobs_log, "w") as log_fp, open(acl_jobs_log, 'w') as acl_fp:
            for x in jl:
                job_id = x['job_id']
                log_fp.write(json.dumps(x) + '\n')
                job_perms = self.get(f'/preview/permissions/jobs/{job_id}')
                job_perms['job_name'] = x['settings']['name']
                acl_fp.write(json.dumps(job_perms) + '\n')

    def import_job_configs(self, log_file='jobs.log', acl_file='acl_jobs.log'):
        jobs_log = self.get_export_dir() + log_file
        acl_jobs_log = self.get_export_dir() + acl_file
        if not os.path.exists(jobs_log):
            print("No job configurations to import.")
            return
        # get an old cluster id to new cluster id mapping object
        cluster_mapping = self.get_cluster_id_mapping()
        old_2_new_policy_ids = self.get_new_policy_id_dict()  # dict { old_policy_id : new_policy_id }
        with open(jobs_log, 'r') as fp:
            for line in fp:
                job_conf = json.loads(line)
                job_creator = job_conf.get('creator_user_name', '')
                job_settings = job_conf['settings']
                job_schedule = job_settings.get('schedule', None)
                if job_schedule:
                    # set all imported jobs as paused
                    job_schedule['pause_status'] = 'PAUSED'
                    job_settings['schedule'] = job_schedule
                if 'existing_cluster_id' in job_settings:
                    old_cid = job_settings['existing_cluster_id']
                    # set new cluster id for existing cluster attribute
                    new_cid = cluster_mapping.get(old_cid, None)
                    if not new_cid:
                        print("Existing cluster has been removed. Resetting job to use new cluster.")
                        job_settings.pop('existing_cluster_id')
                        job_settings['new_cluster'] = self.get_jobs_default_cluster_conf()
                    else:
                        job_settings['existing_cluster_id'] = new_cid
                else:  # new cluster config
                    cluster_conf = job_settings['new_cluster']
                    if 'policy_id' in cluster_conf:
                        old_policy_id = cluster_conf['policy_id']
                        cluster_conf['policy_id'] = old_2_new_policy_ids[old_policy_id]
                    # check for instance pools and modify cluster attributes
                    if 'instance_pool_id' in cluster_conf:
                        new_cluster_conf = self.cleanup_cluster_pool_configs(cluster_conf, job_creator, True)
                    else:
                        new_cluster_conf = cluster_conf
                    job_settings['new_cluster'] = new_cluster_conf
                print("Current JID: {0}".format(job_conf['job_id']))
                # creator can be none if the user is no longer in the org. see our docs page
                creator_user_name = job_conf.get('creator_user_name', None)
                create_resp = self.post('/jobs/create', job_settings)
                if 'error_code' in create_resp:
                    print("Resetting job to use default cluster configs due to expired configurations.")
                    job_settings['new_cluster'] = self.get_jobs_default_cluster_conf()
                    create_resp_retry = self.post('/jobs/create', job_settings)
        # update the jobs with their ACLs
        with open(acl_jobs_log, 'r') as acl_fp:
            job_id_by_name = self.get_job_id_by_name()
            for line in acl_fp:
                acl_conf = json.loads(line)
                job_id = job_id_by_name[acl_conf['job_name']]
                job_path = f'jobs/{job_id}'  # contains `/jobs/{job_id}` path
                api = f'/preview/permissions/{job_path}'
                # get acl permissions for jobs
                acl_perms = self.build_acl_args(acl_conf['access_control_list'], True)
                acl_create_args = {'access_control_list': acl_perms}
                acl_resp = self.patch(api, acl_create_args)
                print(acl_resp)

    def pause_all_jobs(self, pause=True):
        job_list = self.get('/jobs/list').get('jobs', None)
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

    def get_cluster_id_mapping(self, log_file='clusters.log'):
        """
        Get a dict mapping of old cluster ids to new cluster ids for jobs connecting to existing clusters
        :param log_file:
        :return:
        """
        cluster_logfile = self.get_export_dir() + log_file
        current_cl = self.get('/clusters/list').get('clusters', [])
        old_clusters = {}
        # build dict with old cluster name to cluster id mapping
        with open(cluster_logfile, 'r') as fp:
            for line in fp:
                conf = json.loads(line)
                old_clusters[conf['cluster_name']] = conf['cluster_id']
        new_to_old_mapping = {}
        for new_cluster in current_cl:
            old_cluster_id = old_clusters.get(new_cluster['cluster_name'], None)
            if old_cluster_id:
                new_to_old_mapping[old_cluster_id] = new_cluster['cluster_id']
        return new_to_old_mapping
