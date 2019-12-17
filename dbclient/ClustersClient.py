import re
from dbclient import *


class ClustersClient(dbclient):
    create_configs = {'num_workers',
                      'autoscale',
                      'cluster_name',
                      'spark_version',
                      'spark_conf',
                      'aws_attributes',
                      'node_type_id',
                      'driver_node_type_id',
                      'ssh_public_keys',
                      'custom_tags',
                      'cluster_log_conf',
                      'init_scripts',
                      'spark_env_vars',
                      'autotermination_minutes',
                      'enable_elastic_disk',
                      'instance_pool_id',
                      'pinned_by_user_name',
                      'creator_user_name',
                      'cluster_id'}

    def get_spark_versions(self):
        return self.get("/clusters/spark-versions", printJson=True)

    def get_cluster_list(self, alive=True):
        """
        Returns an array of json objects for the running clusters.
        Grab the cluster_name or cluster_id
        """
        cl = self.get("/clusters/list", printJson=False)
        if alive:
            running = filter(lambda x: x['state'] == "RUNNING", cl['clusters'])
            return list(running)
        else:
            return cl['clusters']

    def remove_automated_clusters(self, cluster_list, log_file='skipped_clusters.log'):
        """
        Automated clusters like job clusters or model endpoints should be excluded
        :param cluster_list: list of cluster configurations
        :return: cleaned list with automated clusters removed
        """
        # model endpoint clusters start with the following
        ml_model_pattern = "mlflow-model-"
        # job clusters have specific format, job-JOBID-run-RUNID
        re_expr = re.compile("job-\d+-run-\d+$")
        clean_cluster_list = []
        with open(self._export_dir + log_file, 'w') as log_fp:
            for cluster in cluster_list:
                cluster_name = cluster['cluster_name']
                if re_expr.match(cluster_name) or cluster_name.startswith(ml_model_pattern):
                    log_fp.write(json.dumps(cluster) + '\n')
                else:
                    clean_cluster_list.append(cluster)
        return clean_cluster_list

    def log_cluster_configs(self, log_file='clusters.log'):
        """
        Log the current cluster configs in json file
        :param log_file:
        :return:
        """
        cluster_log = self._export_dir + log_file
        # pinned by cluster_user is a flag per cluster
        cl_raw = self.get_cluster_list(False)
        cl = self.remove_automated_clusters(cl_raw)
        # filter on these items as MVP of the cluster configs
        # https://docs.databricks.com/api/latest/clusters.html#request-structure
        with open(cluster_log, "w") as log_fp:
            for x in cl:
                run_properties = set(list(x.keys())) - self.create_configs
                for p in run_properties:
                    del x[p]
                log_fp.write(json.dumps(x) + '\n')

    def cleanup_cluster_pool_configs(self, cluster_json, cluster_creator):
        """
        Pass in cluster json and cluster_creator to update fields that are not needed for clusters submitted to pools
        :param cluster_json:
        :param cluster_creator:
        :return:
        """
        pool_id_dict = self.get_instance_pool_id_mapping()
        # if pool id exists, remove instance types
        cluster_json.pop('node_type_id')
        cluster_json.pop('driver_node_type_id')
        cluster_json.pop('enable_elastic_disk')
        # add custom tag for original cluster creator for cost tracking
        if 'custom_tags' in cluster_json:
            tags = cluster_json['custom_tags']
            tags['OriginalCreator'] = cluster_creator
            cluster_json['custom_tags'] = tags
        else:
            cluster_json['custom_tags'] = {'OriginalCreator' : cluster_creator}
        # remove all aws_attr except for IAM role if it exists
        if 'aws_attributes' in cluster_json:
            aws_conf = cluster_json.pop('aws_attributes')
            iam_role = aws_conf.get('instance_profile_arn', None)
            if not iam_role:
                cluster_json['aws_attributes'] = {'instance_profile_arn': iam_role}
        # map old pool ids to new pool ids
        old_pool_id = cluster_json['instance_pool_id']
        cluster_json['instance_pool_id'] = pool_id_dict[old_pool_id]
        return cluster_json

    def import_cluster_configs(self, log_file='clusters.log'):
        """
        Import cluster configs and update appropriate properties / tags in the new env
        :param log_file:
        :return:
        """
        cluster_log = self._export_dir + log_file
        # get instance pool id mappings
        with open(cluster_log, 'r') as fp:
            for line in fp:
                cluster_conf = json.loads(line)
                cluster_creator = cluster_conf.pop('creator_user_name')
                # check for instance pools and modify cluster attributes
                if 'instance_pool_id' in cluster_conf:
                    new_cluster_conf = self.cleanup_cluster_pool_configs(cluster_conf, cluster_creator)
                else:
                    # update cluster configs for non-pool clusters
                    # add original creator tag to help with DBU tracking
                    if 'custom_tags' in cluster_conf:
                        tags = cluster_conf['custom_tags']
                        tags['OriginalCreator'] = cluster_creator
                        cluster_conf['custom_tags'] = tags
                    else:
                        cluster_conf['custom_tags'] = {'OriginalCreator' : cluster_creator}
                    new_cluster_conf = cluster_conf
                print("Creating cluster: {0}".format(new_cluster_conf['cluster_name']))
                cluster_resp = self.post('/clusters/create', new_cluster_conf)
                stop_resp = self.post('/clusters/delete', {'cluster_id': cluster_resp['cluster_id']})
                if 'pinned_by_user_name' in cluster_conf:
                    pin_resp = self.post('/clusters/pin', {'cluster_id': cluster_resp['cluster_id']})

    def delete_all_clusters(self):
        cl = self.get_cluster_list(False)
        for x in cl:
            self.post('/clusters/unpin', {'cluster_id': x['cluster_id']})
            self.post('/clusters/permanent-delete', {'cluster_id': x['cluster_id']})

    def log_instance_profiles(self, log_file='instance_profiles.log'):
        ip_log = self._export_dir + log_file
        ips = self.get('/instance-profiles/list')['instance_profiles']
        with open(ip_log, "w") as fp:
            for x in ips:
                fp.write(json.dumps(x) + '\n')

    def import_instance_profiles(self, log_file='instance_profiles.log'):
        ip_log = self._export_dir + log_file
        # check current profiles and skip if the profile already exists
        ip_list = self.get('/instance-profiles/list')['instance_profiles']
        list_of_profiles = [x['instance_profile_arn'] for x in ip_list]
        with open(ip_log, "r") as fp:
            for line in fp:
                ip_arn = json.loads(line).get('instance_profile_arn', None)
                if ip_arn not in list_of_profiles:
                    print("Importing arn: {0}".format(ip_arn))
                    resp = self.post('/instance-profiles/add', {'instance_profile_arn': ip_arn})
                else:
                    print("Skipping since profile exists: {0}".format(ip_arn))

    def log_instance_pools(self, log_file='instance_pools.log'):
        pool_log = self._export_dir + log_file
        pools = self.get('/instance-pools/list')['instance_pools']
        with open(pool_log, "w") as fp:
            for x in pools:
                fp.write(json.dumps(x) + '\n')

    def import_instance_pools(self, log_file='instance_pools.log'):
        pool_log = self._export_dir + log_file
        with open(pool_log, 'r') as fp:
            for line in fp:
                pool_conf = json.loads(line)
                pool_resp = self.post('/instance-pools/create', pool_conf)

    def get_instance_pool_id_mapping(self, log_file='instance_pools.log'):
        pool_log = self._export_dir + log_file
        current_pools = self.get('/instance-pools/list').get('instance_pools', None)
        if not current_pools:
            return None
        new_pools = {}
        # build dict of pool name and id mapping
        for p in current_pools:
            new_pools[p['instance_pool_name']] = p['instance_pool_id']
        # mapping id from old_pool_id to new_pool_id
        pool_mapping_dict = {}
        with open(pool_log, 'r') as fp:
            for line in fp:
                pool_conf = json.loads(line)
                old_pool_id = pool_conf['instance_pool_id']
                pool_name = pool_conf['instance_pool_name']
                new_pool_id = new_pools[pool_name]
                pool_mapping_dict[old_pool_id] = new_pool_id
        return pool_mapping_dict

    def get_global_init_scripts(self):
        """ return a list of global init scripts. Currently not logged """
        ls = self.get('/dbfs/list', {'path': '/databricks/init/'}).get('files', None)
        if ls is None:
            return []
        else:
            global_scripts = [{'path': x['path']} for x in ls if x['is_dir'] == False]
            return global_scripts
