import os, re, time

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
                      'docker_image',
                      'spark_env_vars',
                      'autotermination_minutes',
                      'enable_elastic_disk',
                      'instance_pool_id',
                      'policy_id',
                      'pinned_by_user_name',
                      'creator_user_name',
                      'cluster_id'}

    def cleanup_cluster_pool_configs(self, cluster_json, cluster_creator, is_job_cluster=False):
        """
        Pass in cluster json and cluster_creator to update fields that are not needed for clusters submitted to pools
        :param cluster_json:
        :param cluster_creator:
        :param is_job_cluster: flag to not add tags for job clusters since those clusters don't have this behavior
                as interactive clusters
        :return:
        """
        pool_id_dict = self.get_instance_pool_id_mapping()
        # if pool id exists, remove instance types
        cluster_json.pop('node_type_id', None)
        cluster_json.pop('driver_node_type_id', None)
        cluster_json.pop('enable_elastic_disk', None)
        if not is_job_cluster:
            # add custom tag for original cluster creator for cost tracking
            if 'custom_tags' in cluster_json:
                tags = cluster_json['custom_tags']
                tags['OriginalCreator'] = cluster_creator
                cluster_json['custom_tags'] = tags
            else:
                cluster_json['custom_tags'] = {'OriginalCreator': cluster_creator}
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

    def delete_all_clusters(self):
        cl = self.get_cluster_list(False)
        for x in cl:
            self.post('/clusters/unpin', {'cluster_id': x['cluster_id']})
            self.post('/clusters/permanent-delete', {'cluster_id': x['cluster_id']})

    def edit_cluster(self, cid, iam_role):
        """Edits the existing metastore cluster
        Returns cluster_id"""
        version = self.get_latest_spark_version()
        import os
        real_path = os.path.dirname(os.path.realpath(__file__))
        if self.is_aws():
            print("Updating cluster with: " + iam_role)
            current_cluster_json = self.get(f'/clusters/get?cluster_id={cid}')
            run_properties = set(list(current_cluster_json.keys())) - self.create_configs
            for p in run_properties:
                del current_cluster_json[p]
            if 'aws_attributes' in current_cluster_json:
                aws_conf = current_cluster_json.pop('aws_attributes')
                aws_conf['instance_profile_arn'] = iam_role
            else:
                aws_conf = {'instance_profile_arn': iam_role}
            current_cluster_json['aws_attributes'] = aws_conf
            resp = self.post('/clusters/edit', current_cluster_json)
            print(resp)
            new_cid = self.wait_for_cluster(cid)
            return new_cid
        else:
            return False

    def get_cluster_acls(self, cluster_id, cluster_name):
        """
        Export all cluster permissions for a specific cluster id
        :return:
        """
        perms = self.get(f'/preview/permissions/clusters/{cluster_id}/')
        perms['cluster_name'] = cluster_name
        return perms

    def get_cluster_id_by_name(self, cname, running_only=False):
        cluster_list = self.get('/clusters/list').get('clusters', [])
        if running_only:
            running = list(filter(lambda x: x['state'] == "RUNNING", cluster_list))
            for x in running:
                if cname == x['cluster_name']:
                    return x['cluster_id']
        else:
            for x in cluster_list:
                if cname == x['cluster_name']:
                    return x['cluster_id']
        return None

    def get_cluster_list(self, alive=True):
        """
        Returns an array of json objects for the running clusters.
        Grab the cluster_name or cluster_id
        """
        clusters_list = self.get("/clusters/list", print_json=False).get('clusters', [])
        if alive and clusters_list:
            running = filter(lambda x: x['state'] == "RUNNING", clusters_list)
            return list(running)
        else:
            return clusters_list

    def get_execution_context(self, cid):
        print("Creating remote Spark Session")
        time.sleep(5)
        ec_payload = {"language": "python",
                      "clusterId": cid}
        ec = self.post('/contexts/create', json_params=ec_payload, version="1.2")
        # Grab the execution context ID
        ec_id = ec.get('id', None)
        if not ec_id:
            print('Unable to establish remote session')
            print(ec)
            raise Exception("Remote session error")
        return ec_id

    def get_global_init_scripts(self):
        """ return a list of global init scripts. Currently not logged """
        ls = self.get('/dbfs/list', {'path': '/databricks/init/'}).get('files', None)
        if ls is None:
            return []
        else:
            global_scripts = [{'path': x['path']} for x in ls if x['is_dir'] == False]
            return global_scripts

    def get_instance_pool_id_mapping(self, log_file='instance_pools.log'):
        pool_log = self.get_export_dir() + log_file
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

    def get_policy_id_by_name_dict(self):
        name_id_dict = {}
        resp = self.get('/policies/clusters/list').get('policies', [])
        for policy in resp:
            name_id_dict[policy['name']] = policy['policy_id']
        return name_id_dict

    def get_spark_versions(self):
        return self.get("/clusters/spark-versions", print_json=True)

    def get_instance_profiles_list(self):
        if self.is_aws():
            ip_json_list = self.get('/instance-profiles/list').get('instance_profiles', [])
            iam_roles_list = list(map(lambda x: x.get('instance_profile_arn'), ip_json_list))
            return iam_roles_list
        return []

    def get_iam_role_by_cid(self, cid):
        if self.is_aws():
            cluster_resp = self.get(f'/clusters/get?cluster_id={cid}')
            return cluster_resp.get('aws_attributes').get('instance_profile_arn', None)
        return None

    def get_new_policy_id_dict(self, policy_file='cluster_policies.log'):
        """
        mapping function to get the new policy ids. ids change when migrating to a new workspace
        read the log file and map the old id to the new id
        :param old_policy_id: str of the old id
        :return: str of new policy id
        """
        policy_log = self.get_export_dir() + policy_file
        current_policies = self.get('/policies/clusters/list').get('policies', [])
        current_policies_dict = {}  # name : current policy id
        for policy in current_policies:
            current_name = policy['name']
            current_id = policy['policy_id']
            current_policies_dict[current_name] = current_id
        policy_id_dict = {}
        with open(policy_log, 'r') as fp:
            for line in fp:
                policy_conf = json.loads(line)
                policy_name = policy_conf['name']
                old_policy_id = policy_conf['policy_id']
                policy_id_dict[old_policy_id] = current_policies_dict[policy_name] # old_id : new_id
        return policy_id_dict

    def import_cluster_configs(self, log_file='clusters.log', acl_log_file='acl_clusters.log', filter_user=None):
        """
        Import cluster configs and update appropriate properties / tags in the new env
        :param log_file:
        :return:
        """
        cluster_log = self.get_export_dir() + log_file
        acl_cluster_log = self.get_export_dir() + acl_log_file
        if not os.path.exists(cluster_log):
            print("No clusters to import.")
            return
        current_cluster_names = set([x.get('cluster_name', None) for x in self.get_cluster_list(False)])
        old_2_new_policy_ids = self.get_new_policy_id_dict()  # dict of {old_id : new_id}
        # get instance pool id mappings
        with open(cluster_log, 'r') as fp:
            for line in fp:
                cluster_conf = json.loads(line)
                cluster_name = cluster_conf['cluster_name']
                if cluster_name in current_cluster_names:
                    print("Cluster already exists, skipping: {0}".format(cluster_name))
                    continue
                cluster_creator = cluster_conf.pop('creator_user_name')
                if 'policy_id' in cluster_conf:
                    old_policy_id = cluster_conf['policy_id']
                    cluster_conf['policy_id'] = old_2_new_policy_ids[old_policy_id]
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
                        cluster_conf['custom_tags'] = {'OriginalCreator': cluster_creator}
                    new_cluster_conf = cluster_conf
                print("Creating cluster: {0}".format(new_cluster_conf['cluster_name']))
                cluster_resp = self.post('/clusters/create', new_cluster_conf)
                if cluster_resp['http_status_code'] == 200:
                    stop_resp = self.post('/clusters/delete', {'cluster_id': cluster_resp['cluster_id']})
                    if 'pinned_by_user_name' in cluster_conf:
                        pin_resp = self.post('/clusters/pin', {'cluster_id': cluster_resp['cluster_id']})
                else:
                    print(cluster_resp)
        # add cluster ACLs
        # loop through and reapply cluster ACLs
        with open(acl_cluster_log, 'r') as acl_fp:
            for x in acl_fp:
                data = json.loads(x)
                cluster_name = data['cluster_name']
                acl_args = {'access_control_list' : self.build_acl_args(data['access_control_list'])}
                cid = self.get_cluster_id_by_name(cluster_name)
                if cid is None:
                    raise ValueError('Cluster id must exist in new env. Re-import cluster configs.')
                api = f'/preview/permissions/clusters/{cid}'
                resp = self.put(api, acl_args)
                print(resp)

    def import_cluster_policies(self, log_file='cluster_policies.log', acl_log_file='acl_cluster_policies.log'):
        policies_log = self.get_export_dir() + log_file
        acl_policies_log = self.get_export_dir() + acl_log_file
        # create the policies
        with open(policies_log, 'r') as policy_fp:
            for p in policy_fp:
                policy_conf = json.loads(p)
                # when creating the policy, we only need `name` and `definition` fields
                create_args = {'name': policy_conf['name'],
                               'definition': policy_conf['definition']}
                resp = self.post('/policies/clusters/create', create_args)
        # ACLs are created by using the `access_control_list` key
        with open(acl_policies_log, 'r') as acl_fp:
            id_map = self.get_policy_id_by_name_dict()
            for x in acl_fp:
                p_acl = json.loads(x)
                acl_create_args = {'access_control_list': self.build_acl_args(p_acl['access_control_list'])}
                policy_id = id_map[p_acl['name']]
                api = f'/permissions/cluster-policies/{policy_id}'
                resp = self.put(api, acl_create_args)
                print(resp)

    def import_instance_pools(self, log_file='instance_pools.log'):
        pool_log = self.get_export_dir() + log_file
        if not os.path.exists(pool_log):
            print("No instance pools to import.")
            return
        with open(pool_log, 'r') as fp:
            for line in fp:
                pool_conf = json.loads(line)
                pool_resp = self.post('/instance-pools/create', pool_conf)

    def import_instance_profiles(self, log_file='instance_profiles.log'):
        # currently an AWS only operation
        ip_log = self.get_export_dir() + log_file
        if not os.path.exists(ip_log):
            print("No instance profiles to import.")
            return
        # check current profiles and skip if the profile already exists
        ip_list = self.get('/instance-profiles/list').get('instance_profiles', None)
        if ip_list:
            list_of_profiles = [x['instance_profile_arn'] for x in ip_list]
        else:
            list_of_profiles = []
        with open(ip_log, "r") as fp:
            for line in fp:
                ip_arn = json.loads(line).get('instance_profile_arn', None)
                if ip_arn not in list_of_profiles:
                    print("Importing arn: {0}".format(ip_arn))
                    resp = self.post('/instance-profiles/add', {'instance_profile_arn': ip_arn})
                    print(resp)
                else:
                    print("Skipping since profile exists: {0}".format(ip_arn))

    def is_spark_3(self, cid):
        spark_version = self.get(f'/clusters/get?cluster_id={cid}').get('spark_version', "")
        if spark_version[0] >= '7':
            return True
        else:
            return False

    def launch_cluster(self, iam_role=None):
        """ Launches a cluster to get DDL statements.
        Returns a cluster_id """
        # removed for now as Spark 3.0 will have backwards incompatible changes
        # version = self.get_latest_spark_version()
        import os
        real_path = os.path.dirname(os.path.realpath(__file__))
        if self.is_aws():
            with open(real_path + '/../data/aws_cluster.json', 'r') as fp:
                cluster_json = json.loads(fp.read())
            if iam_role:
                aws_attr = cluster_json['aws_attributes']
                print("Creating cluster with: " + iam_role)
                aws_attr['instance_profile_arn'] = iam_role
                cluster_json['aws_attributes'] = aws_attr
        else:
            with open(real_path + '/../data/azure_cluster.json', 'r') as fp:
                cluster_json = json.loads(fp.read())
        # set the latest spark release regardless of defined cluster json
        # cluster_json['spark_version'] = version['key']
        cluster_name = cluster_json['cluster_name']
        existing_cid = self.get_cluster_id_by_name(cluster_name)
        if existing_cid:
            # if the cluster id exists, then a cluster exists in a terminated state. let's start it
            cid = self.start_cluster_by_name(cluster_name)
            return cid
        else:
            print("Starting cluster with name: {0} ".format(cluster_name))
            c_info = self.post('/clusters/create', cluster_json)
            if c_info['http_status_code'] != 200:
                raise Exception("Could not launch cluster. Verify that the --azure flag or cluster config is correct.")
            self.wait_for_cluster(c_info['cluster_id'])
            return c_info['cluster_id']

    def log_cluster_configs(self, log_file='clusters.log', acl_log_file='acl_clusters.log', filter_user=None):
        """
        Log the current cluster configs in json file
        :param log_file: log the cluster configs
        :param acl_log_file: log the ACL definitions
        :param filter_user: user name to filter and log the cluster config
        :return:
        """
        cluster_log = self.get_export_dir() + log_file
        acl_cluster_log = self.get_export_dir() + acl_log_file
        # pinned by cluster_user is a flag per cluster
        cl_raw = self.get_cluster_list(False)
        cluster_list = self.remove_automated_clusters(cl_raw)
        ip_list = self.get('/instance-profiles/list').get('instance_profiles', [])
        nonempty_ip_list = []
        if ip_list:
            # filter none if we hit a profile w/ a none object
            # generate list of registered instance profiles to check cluster configs against
            nonempty_ip_list = list(filter(None, [x.get('instance_profile_arn', None) for x in ip_list]))

        # filter on these items as MVP of the cluster configs
        # https://docs.databricks.com/api/latest/clusters.html#request-structure
        with open(cluster_log, 'w') as log_fp, open(acl_cluster_log, 'w') as acl_log_fp:
            for cluster_json in cluster_list:
                run_properties = set(list(cluster_json.keys())) - self.create_configs
                for p in run_properties:
                    del cluster_json[p]
                if 'aws_attributes' in cluster_json:
                    aws_conf = cluster_json.pop('aws_attributes')
                    iam_role = aws_conf.get('instance_profile_arn', None)
                    if iam_role and ip_list:
                        if iam_role not in nonempty_ip_list:
                            print("Skipping log of default IAM role: " + iam_role)
                            del aws_conf['instance_profile_arn']
                            cluster_json['aws_attributes'] = aws_conf
                    cluster_json['aws_attributes'] = aws_conf
                cluster_perms = self.get_cluster_acls(cluster_json['cluster_id'], cluster_json['cluster_name'])
                acl_log_fp.write(json.dumps(cluster_perms) + '\n')
                if filter_user:
                    if cluster_json['creator_user_name'] == filter_user:
                        log_fp.write(json.dumps(cluster_json) + '\n')
                else:
                    log_fp.write(json.dumps(cluster_json) + '\n')

    def log_cluster_policies(self, log_file='cluster_policies.log', acl_log_file='acl_cluster_policies.log'):
        policies_log = self.get_export_dir() + log_file
        acl_policies_log = self.get_export_dir() + acl_log_file
        # log all cluster policy definitions
        policy_ids = {}
        policies_list = self.get('/policies/clusters/list').get('policies', [])
        with open(policies_log, 'w') as fp:
            for x in policies_list:
                policy_ids[x.get('policy_id')] = x.get('name')
                fp.write(json.dumps(x) + '\n')
        # log cluster policy ACLs, which takes a policy id as arguments
        with open(acl_policies_log, 'w') as acl_fp:
            for pid in policy_ids:
                api = f'/preview/permissions/cluster-policies/{pid}'
                perms = self.get(api)
                perms['name'] = policy_ids[pid]
                acl_fp.write(json.dumps(perms) + '\n')

    def log_instance_pools(self, log_file='instance_pools.log'):
        pool_log = self.get_export_dir() + log_file
        pools = self.get('/instance-pools/list').get('instance_pools', None)
        if pools:
            with open(pool_log, "w") as fp:
                for x in pools:
                    fp.write(json.dumps(x) + '\n')

    def log_instance_profiles(self, log_file='instance_profiles.log'):
        ip_log = self.get_export_dir() + log_file
        ips = self.get('/instance-profiles/list').get('instance_profiles', None)
        if ips:
            with open(ip_log, "w") as fp:
                for x in ips:
                    fp.write(json.dumps(x) + '\n')

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
        with open(self.get_export_dir() + log_file, 'w') as log_fp:
            for cluster in cluster_list:
                cluster_name = cluster['cluster_name']
                if re_expr.match(cluster_name) or cluster_name.startswith(ml_model_pattern):
                    log_fp.write(json.dumps(cluster) + '\n')
                else:
                    clean_cluster_list.append(cluster)
        return clean_cluster_list

    def start_cluster_by_name(self, cluster_name):
        cid = self.get_cluster_id_by_name(cluster_name)
        if cid is None:
            raise Exception('Error: Cluster name does not exist')
        print("Starting {0} with id {1}".format(cluster_name, cid))
        resp = self.post('/clusters/start', {'cluster_id': cid})
        if 'error_code' in resp:
            if resp.get('error_code', None) == 'INVALID_STATE':
                print('Error: {0}'.format(resp.get('message', None)))
            else:
                raise Exception('Error: cluster does not exist, or is in a state that is unexpected. '
                                'Cluster should either be terminated state, or already running.')
        self.wait_for_cluster(cid)
        return cid

    def submit_command(self, cid, ec_id, cmd):
        # This launches spark commands and print the results. We can pull out the text results from the API
        command_payload = {'language': 'python',
                           'contextId': ec_id,
                           'clusterId': cid,
                           'command': cmd}
        command = self.post('/commands/execute',
                            json_params=command_payload,
                            version="1.2")

        com_id = command.get('id', None)
        if not com_id:
            print("ERROR: ")
            print(command)
        # print('command_id : ' + com_id)
        result_payload = {'clusterId': cid, 'contextId': ec_id, 'commandId': com_id}

        resp = self.get('/commands/status', json_params=result_payload, version="1.2")
        is_running = self.get_key(resp, 'status')

        # loop through the status api to check for the 'running' state call and sleep 1 second
        while (is_running == "Running") or (is_running == 'Queued'):
            resp = self.get('/commands/status', json_params=result_payload, version="1.2")
            is_running = self.get_key(resp, 'status')
            time.sleep(1)
        end_result_status = self.get_key(resp, 'status')
        end_results = self.get_key(resp, 'results')
        if end_results.get('resultType', None) == 'error':
            print("ERROR: ")
            print(end_results.get('summary', None))
        return end_results

    def wait_for_cluster(self, cid):
        c_state = self.get('/clusters/get', {'cluster_id': cid})
        while c_state['state'] != 'RUNNING' and c_state['state'] != 'TERMINATED':
            c_state = self.get('/clusters/get', {'cluster_id': cid})
            print('Cluster state: {0}'.format(c_state['state']))
            time.sleep(2)
        if c_state['state'] == 'TERMINATED':
            raise RuntimeError("Cluster is terminated. Please check EVENT history for details")
        return cid

