import json
import os

from databricks_migrate import log
from databricks_migrate.dbclient import DBClient


class JobsClient(DBClient):
    __new_aws_cluster_conf = {
        "num_workers": 8,
        "spark_version": "6.1.x-scala2.11",
        "node_type_id": "i3.xlarge",
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": False,
    }

    __new_azure_cluster_conf = {
        "num_workers": 8,
        "spark_version": "6.2.x-scala2.11",
        "node_type_id": "Standard_DS3_v2",
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
    }

    def get_jobs_list(self, print_json=False):
        """ Returns an array of json objects for jobs """
        jobs = self.get("/jobs/list", print_json)
        return jobs['jobs']

    def get_job_id(self, name):
        jobs = self.get_jobs_list()
        for i in jobs:
            if i['settings']['name'] == name:
                return i['job_id']
        return None

    def log_job_configs(self, log_file='jobs.log'):
        jobs_log = self._export_dir + log_file
        # pinned by cluster_user is a flag per cluster
        jl = self.get_jobs_list(False)
        with open(jobs_log, "w") as log_fp:
            for x in jl:
                log_fp.write(json.dumps(x) + '\n')

    def import_job_configs(self, log_file='jobs.log'):
        jobs_log = self._export_dir + log_file
        if not os.path.exists(jobs_log):
            log.info("No job configurations to import.")
            return
        # get an old cluster id to new cluster id mapping object
        cluster_mapping = self.get_cluster_id_mapping()
        with open(jobs_log, 'r') as fp:
            for line in fp:
                job_conf = json.loads(line)
                job_settings = job_conf['settings']
                if 'existing_cluster_id' in job_settings:
                    old_cid = job_settings['existing_cluster_id']
                    # set new cluster id for existing cluster attribute
                    new_cid = cluster_mapping.get(old_cid, None)
                    if not new_cid:
                        log.info("Existing cluster has been removed. Resetting job to use new cluster.")
                        job_settings.pop('existing_cluster_id')
                        if self.is_aws():
                            job_settings['new_cluster'] = self.__new_aws_cluster_conf
                        else:
                            job_settings['new_cluster'] = self.__new_azure_cluster_conf
                    else:
                        job_settings['existing_cluster_id'] = new_cid
                log.info("Current JID: {0}".format(job_conf['job_id']))
                # creator can be none if the user is no longer in the org. see our docs page
                creator_user_name = job_conf.get('creator_user_name', None)
                create_resp = self.post('/jobs/create', job_settings)
                if 'error_code' in create_resp:
                    log.info("Resetting job to use default cluster configs due to expired configurations.")
                    if self.is_aws():
                        job_settings['new_cluster'] = self.__new_aws_cluster_conf
                    else:
                        job_settings['new_cluster'] = self.__new_azure_cluster_conf
                    create_resp_retry = self.post('/jobs/create', job_settings)

    def delete_all_jobs(self):
        job_list = self.get('/jobs/list').get('jobs', None)
        for job in job_list:
            self.post('/jobs/delete', {'job_id': job['job_id']})

    def get_cluster_id_mapping(self, log_file='clusters.log'):
        """
        Get a dict mapping of old cluster ids to new cluster ids for jobs connecting to existing clusters
        :param log_file:
        :return:
        """
        cluster_logfile = self._export_dir + log_file
        current_cl = self.get('/clusters/list').get('clusters', None)
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
                new_to_old_mapping[new_cluster['cluster_id']] = old_cluster_id
        return new_to_old_mapping
