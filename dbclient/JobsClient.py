from dbclient import *
from cron_descriptor import get_description
import json, datetime


class JobsClient(dbclient):

    def get_jobs_list(self, printJson=False):
        """ Returns an array of json objects for jobs """
        jobs = self.get("/jobs/list", printJson)
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
