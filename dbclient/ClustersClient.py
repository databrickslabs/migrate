import logging
import os
import re
import time
import logging_utils
import wmconstants
from dbclient import *


class ClustersClient(dbclient):
    def __init__(self, configs, checkpoint_service):
        super().__init__(configs)
        self._checkpoint_service = checkpoint_service
        self.groups_to_keep = configs.get("groups_to_keep", False)
        self.skip_missing_users = configs['skip_missing_users']
        self.hipaa = configs.get('hipaa', False)
        self.bypass_secret_acl = configs.get('bypass_secret_acl', False)

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
                      'driver_instance_pool_id',
                      'policy_id',
                      'pinned_by_user_name',
                      'creator_user_name',
                      'cluster_id',
                      'data_security_mode'}

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

        if not pool_id_dict:
            logging.info("WARNING: instance pool is outdated. Pools may have been deleted; cluster will use defaults.")
            cluster_json.pop("instance_pool_id")
        else:
            # if pool id exists, remove instance types
            cluster_json.pop('node_type_id', None)
            cluster_json.pop('driver_node_type_id', None)
            cluster_json.pop('enable_elastic_disk', None)
            # map old pool ids to new pool ids
            old_pool_id = cluster_json['instance_pool_id']
            new_pool_id = pool_id_dict.get(old_pool_id)

            if old_pool_id and new_pool_id:
                cluster_json['instance_pool_id'] = new_pool_id
            else:
                logging.warning(
                    f"Instance pool mapped to src/dest :{old_pool_id}/{new_pool_id} is not available." +
                    "It may have been deleted; cluster will use defaults.")
                cluster_json.pop("instance_pool_id")

            old_driver_pool_id = cluster_json.get('driver_instance_pool_id')
            # driver_instance_pool_id is optional. if present, try to map new id.
            if old_driver_pool_id:
                new_driver_pool_id = pool_id_dict.get(old_driver_pool_id)
                if new_driver_pool_id:
                    cluster_json['driver_instance_pool_id'] = new_driver_pool_id
                else:
                    # if new driver pool for respective source driver pool id is not available,
                    # reset to default configs.
                    logging.warning(
                        f"Driver Instance pool mapped to src/dest :{old_driver_pool_id}/{new_driver_pool_id}" +
                        "is not available.It may have been deleted; cluster will use defaults.")
                    cluster_json.pop("instance_pool_id")
                    cluster_json.pop("driver_instance_pool_id")

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
            logging.info("Updating cluster with: " + iam_role)
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
            logging.info(resp)
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
        logging.info("Creating remote Spark Session")
        time.sleep(5)
        ec_payload = {"language": "python",
                      "clusterId": cid}
        ec = self.post('/contexts/create', json_params=ec_payload, version="1.2")
        # Grab the execution context ID
        ec_id = ec.get('id', None)
        if not ec_id:
            logging.info('Unable to establish remote session')
            logging.info(ec)
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
        with open(pool_log, 'r', encoding="utf-8") as fp:
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
        with open(policy_log, 'r', encoding="utf-8") as fp:
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
            logging.info("No clusters to import.")
            return
        current_cluster_names = set([x.get('cluster_name', None) for x in self.get_cluster_list(False)])
        old_2_new_policy_ids = self.get_new_policy_id_dict()  # dict of {old_id : new_id}
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.CLUSTER_OBJECT, self.get_export_dir())
        checkpoint_cluster_configs_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_IMPORT, wmconstants.CLUSTER_OBJECT)
        # get instance pool id mappings
        with open(cluster_log, 'r', encoding="utf-8") as fp:
            for line in fp:
                cluster_conf = json.loads(line)
                if 'cluster_id' in cluster_conf and checkpoint_cluster_configs_set.contains(cluster_conf['cluster_id']):
                    continue
                cluster_name = cluster_conf['cluster_name']
                if cluster_name in current_cluster_names:
                    logging.info("Cluster already exists, skipping: {0}".format(cluster_name))
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
                    if 'cluster_id' in cluster_conf:
                        checkpoint_cluster_configs_set.write(cluster_conf['cluster_id'])
                else:
                    logging_utils.log_response_error(error_logger, cluster_resp)
                    print(cluster_resp)

        # TODO: May be put it into a separate step to make it more rerunnable.
        self._log_cluster_ids_and_original_creators(log_file)

        # add cluster ACLs
        # loop through and reapply cluster ACLs
        with open(acl_cluster_log, 'r', encoding="utf-8") as acl_fp:
            for x in acl_fp:
                data = json.loads(x)
                if 'object_id' in data and checkpoint_cluster_configs_set.contains(data['object_id']):
                    continue
                cluster_name = data['cluster_name']
                print(f'Applying acl for {cluster_name}')
                acl_args = {'access_control_list' : self.build_acl_args(data['access_control_list'])}
                cid = self.get_cluster_id_by_name(cluster_name)
                if cid is None:
                    error_message = f'Cluster id must exist in new env for cluster_name: {cluster_name}. ' \
                                    f'Re-import cluster configs.'
                    raise ValueError(error_message)
                api = f'/preview/permissions/clusters/{cid}'
                resp = self.put(api, acl_args)

                if self.skip_missing_users:
                    ignore_error_list = ["RESOURCE_DOES_NOT_EXIST", "RESOURCE_ALREADY_EXISTS"]
                else:
                    ignore_error_list = ["RESOURCE_ALREADY_EXISTS"]

                if logging_utils.check_error(resp, ignore_error_list):
                    logging_utils.log_response_error(error_logger, resp)
                elif 'object_id' in data:
                    checkpoint_cluster_configs_set.write(data['object_id'])

                print(resp)

    def _log_cluster_ids_and_original_creators(
            self,
            cluster_log_file,
            user_name_to_user_id_log_file='user_name_to_user_id.log',
            creators_file='original_creator_user_ids.log',
            cluster_ids_file='cluster_ids_to_change_creator.log'):
        """
        Log the cluster_ids_to_change_creator and original_creator_user_ids
        These can be used to edit the clusters to be owned by the correct/original creator instead of the
        PAT token owner. Fails if user_name_to_user_id.log does not exist.

        :param user_name_to_user_id_log_file: file that contains the userName to userId mapping of the DST workspace.
                                              This file is exported as part of the users-import step.
        :param cluster_log_file: file that contains the exported cluster objects.
        :param creators_file: output file written with the list of original creators of clusters.
                              The list should be in the same order as the cluster_ids_file.
        :param cluster_ids_file: output file written with the list of cluster ids that need the creator tag to change.
        """
        cluster_ids_to_change_creator = []
        original_creator_user_ids = []
        with open(self.get_export_dir() + user_name_to_user_id_log_file, 'r', encoding="utf-8") as fp:
            user_name_to_user_id = json.loads(fp.read())

        old_to_new_cluster_mapping = self.get_cluster_id_mapping(cluster_log_file)

        with open(self.get_export_dir() + cluster_log_file, 'r', encoding="utf-8") as fp:
            for line in fp:
                cluster_conf = json.loads(line)
                if 'creator_user_name' in cluster_conf and 'cluster_id' in cluster_conf:
                    original_cluster_creator = cluster_conf['creator_user_name']
                    original_cluster_id = cluster_conf['cluster_id']
                    if original_cluster_id in old_to_new_cluster_mapping and original_cluster_creator in user_name_to_user_id:
                        current_cluster_id = old_to_new_cluster_mapping[original_cluster_id]
                        cluster_ids_to_change_creator.append(current_cluster_id)
                        original_creator_user_ids.append(user_name_to_user_id[original_cluster_creator])
                    else:
                        print("The old cluster id " + original_cluster_id + " with the original_creator of " +
                              original_cluster_creator +
                              " does not get logged for EditClusterOwner due to some problems.")

        with open(self.get_export_dir() + cluster_ids_file, 'w', encoding="utf-8") as fp:
            dumped_cluster_ids = json.dumps(cluster_ids_to_change_creator, separators=(',', ':'))
            # Remove the initial '[' and ']' for easier copy-paste.
            fp.write(dumped_cluster_ids.lstrip('[').rstrip(']'))

        with open(self.get_export_dir() + creators_file, 'w', encoding="utf-8") as fp:
            dumped_user_ids = json.dumps(original_creator_user_ids, separators=(',', ':'))
            # Remove the initial '[' and ']' for easier copy-paste.
            fp.write(dumped_user_ids.lstrip('[').rstrip(']'))

    def get_cluster_id_mapping(self, log_file='clusters.log'):
        """
        Get a dict mapping of old cluster ids to new cluster ids.
        :param log_file: file that contains clusters info from the source workspace.
        :return: old_cluster_id -> new_cluster_id dictionary.
        """
        cluster_logfile = self.get_export_dir() + log_file
        current_cl = self.get_cluster_list(False)
        old_clusters = {}
        # build dict with old cluster name to cluster id mapping
        if not os.path.exists(cluster_logfile):
            raise ValueError('Clusters log must exist to map clusters to previous existing cluster ids')
        with open(cluster_logfile, 'r', encoding="utf-8") as fp:
            for line in fp:
                conf = json.loads(line)
                old_clusters[conf['cluster_name']] = conf['cluster_id']
        old_to_new_mapping = {}
        for new_cluster in current_cl:
            old_cluster_id = old_clusters.get(new_cluster['cluster_name'], None)
            if old_cluster_id:
                old_to_new_mapping[old_cluster_id] = new_cluster['cluster_id']
        return old_to_new_mapping

    def import_cluster_policies(self, log_file='cluster_policies.log', acl_log_file='acl_cluster_policies.log'):
        policies_log = self.get_export_dir() + log_file
        acl_policies_log = self.get_export_dir() + acl_log_file
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.CLUSTER_OBJECT, self.get_export_dir())
        checkpoint_cluster_policies_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_IMPORT, wmconstants.CLUSTER_OBJECT
        )
        # create the policies
        if os.path.exists(policies_log):
            with open(policies_log, 'r', encoding="utf-8") as policy_fp:
                for p in policy_fp:
                    policy_conf = json.loads(p)
                    if 'policy_id' in policy_conf and checkpoint_cluster_policies_set.contains(policy_conf['policy_id']):
                        continue
                    # when creating the policy, we only need `name` and `definition` fields
                    create_args = {'name': policy_conf['name'],
                                   'definition': policy_conf['definition']}
                    resp = self.post('/policies/clusters/create', create_args)
                    ignore_error_list = ['INVALID_PARAMETER_VALUE']
                    if not logging_utils.log_response_error(error_logger, resp, ignore_error_list=ignore_error_list):
                        if 'policy_id' in policy_conf:
                            checkpoint_cluster_policies_set.write(policy_conf['policy_id'])

            # ACLs are created by using the `access_control_list` key
            with open(acl_policies_log, 'r', encoding="utf-8") as acl_fp:
                id_map = self.get_policy_id_by_name_dict()
                for x in acl_fp:
                    p_acl = json.loads(x)
                    if 'object_id' in p_acl and checkpoint_cluster_policies_set.contains(p_acl['object_id']):
                        continue
                    acl_create_args = {'access_control_list': self.build_acl_args(p_acl['access_control_list'])}
                    policy_id = id_map[p_acl['name']]
                    api = f'/permissions/cluster-policies/{policy_id}'
                    resp = self.put(api, acl_create_args)
                    if not logging_utils.log_response_error(error_logger, resp):
                        if 'object_id' in p_acl:
                            checkpoint_cluster_policies_set.write(p_acl['object_id'])
        else:
            logging.info('Skipping cluster policies as no log file exists')

    def import_instance_pools(self, log_file='instance_pools.log'):
        pool_log = self.get_export_dir() + log_file
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.INSTANCE_POOL_OBJECT, self.get_export_dir())
        if not os.path.exists(pool_log):
            logging.info("No instance pools to import.")
            return
        with open(pool_log, 'r', encoding="utf-8") as fp:
            for line in fp:
                pool_conf = json.loads(line)
                pool_resp = self.post('/instance-pools/create', pool_conf)
                ignore_error_list = ['INVALID_PARAMETER_VALUE']
                logging_utils.log_response_error(error_logger, pool_resp, ignore_error_list=ignore_error_list)

    def import_instance_profiles(self, log_file='instance_profiles.log'):
        # currently an AWS only operation
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.INSTANCE_PROFILE_OBJECT, self.get_export_dir())
        ip_log = self.get_export_dir() + log_file
        if not os.path.exists(ip_log):
            logging.info("No instance profiles to import.")
            return
        # check current profiles and skip if the profile already exists
        ip_list = self.get('/instance-profiles/list').get('instance_profiles', None)
        if ip_list:
            list_of_profiles = [x['instance_profile_arn'] for x in ip_list]
        else:
            list_of_profiles = []
        import_profiles_count = 0
        with open(ip_log, "r", encoding="utf-8") as fp:
            for line in fp:
                ip_arn = json.loads(line).get('instance_profile_arn', None)
                if ip_arn not in list_of_profiles:
                    print("Importing arn: {0}".format(ip_arn))
                    resp = self.post('/instance-profiles/add', {'instance_profile_arn': ip_arn})
                    if not logging_utils.check_error(resp):
                        import_profiles_count += 1
                    else:
                        logging.info(f"Failed instance profile import for {ip_arn}")
                        logging_utils.log_response_error(error_logger, resp)
                else:
                    logging.info("Skipping since profile already exists: {0}".format(ip_arn))
        return import_profiles_count

    def is_spark_3(self, cid):
        spark_version = self.get(f'/clusters/get?cluster_id={cid}').get('spark_version', "")
        if spark_version[0] >= '7':
            return True
        else:
            return False

    def launch_cluster(self, iam_role=None, enable_table_acls=False):
        """ Launches a cluster to get DDL statements.
        Returns a cluster_id """
        # removed for now as Spark 3.0 will have backwards incompatible changes
        # version = self.get_latest_spark_version()
        import os
        real_path = os.path.dirname(os.path.realpath(__file__))

        # add _table_acls suffix to cluster config path if enable_table_acls is set, add _hipaa if hipaa mode enabled
        cluster_json_postfix = ''
        if enable_table_acls and self.hipaa:
            cluster_json_postfix = '_table_acls_hipaa'
        elif enable_table_acls:
            cluster_json_postfix = '_table_acls'
        elif self.hipaa:
            cluster_json_postfix = '_hipaa'

        cluster_json_postfix = '_table_acls' if enable_table_acls else ''
        if self.is_aws():
            with open(f'{real_path}/../data/aws_cluster{cluster_json_postfix}.json', 'r', encoding="utf-8") as fp:
                cluster_json = json.loads(fp.read())
            if iam_role:
                aws_attr = cluster_json['aws_attributes']
                print("Creating cluster with: " + iam_role)
                aws_attr['instance_profile_arn'] = iam_role
                cluster_json['aws_attributes'] = aws_attr
        elif self.is_azure():
            with open(f'{real_path}/../data/azure_cluster{cluster_json_postfix}.json', 'r', encoding="utf-8") as fp:
                cluster_json = json.loads(fp.read())
        elif self.is_gcp():
            with open(f'{real_path}/../data/gcp_cluster{cluster_json_postfix}.json', 'r', encoding="utf-8") as fp:
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
            logging.info("Starting cluster with name: {0} ".format(cluster_name))
            c_info = self.post('/clusters/create', cluster_json)
            if c_info['http_status_code'] != 200:
                raise Exception("Could not launch cluster. Verify that the --azure or --gcp flag or cluster config is correct.")
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

        # get users list based on groups_to_keep
        users_list = []
        if self.groups_to_keep is not None:
            all_users = self.get('/preview/scim/v2/Users').get('Resources', None)
            users_list = list(set([user.get("emails")[0].get("value") for user in all_users
                                   for group in user.get("groups") if group.get("display") in self.groups_to_keep]))

        cluster_log = self.get_export_dir() + log_file
        acl_cluster_log = self.get_export_dir() + acl_log_file
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.CLUSTER_OBJECT, self.get_export_dir())
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
        with open(cluster_log, 'w', encoding="utf-8") as log_fp, open(acl_cluster_log, 'w', encoding="utf-8") as acl_log_fp:
            for cluster_json in cluster_list:
                run_properties = set(list(cluster_json.keys())) - self.create_configs
                for p in run_properties:
                    del cluster_json[p]
                if 'aws_attributes' in cluster_json:
                    aws_conf = cluster_json.pop('aws_attributes')
                    iam_role = aws_conf.get('instance_profile_arn', None)
                    if iam_role and ip_list:
                        if iam_role not in nonempty_ip_list:
                            logging.info("Skipping log of default IAM role: " + iam_role)
                            del aws_conf['instance_profile_arn']
                            cluster_json['aws_attributes'] = aws_conf
                    cluster_json['aws_attributes'] = aws_conf
                cluster_perms = self.get_cluster_acls(cluster_json['cluster_id'], cluster_json['cluster_name'])

                if users_list:
                    acls = [acl for acl in cluster_perms.get("access_control_list") if
                            (acl.get("group_name", "") in self.groups_to_keep) or
                            (acl.get("user_name", "") in users_list) or
                            (acl.get("group_name", "") == "users")]
                    cluster_perms["access_control_list"] = acls

                    if cluster_perms['http_status_code'] == 200 and acls:
                        acl_log_fp.write(json.dumps(cluster_perms) + '\n')
                    else:
                        error_logger.error(f'Failed to get cluster ACL: {cluster_perms}')

                elif cluster_perms['http_status_code'] == 200:
                    acl_log_fp.write(json.dumps(cluster_perms) + '\n')
                else:
                    error_logger.error(f'Failed to get cluster ACL: {cluster_perms}')

                if filter_user:
                    if cluster_json['creator_user_name'] == filter_user:
                        log_fp.write(json.dumps(cluster_json) + '\n')
                elif users_list:
                    if cluster_json.get('creator_user_name') in users_list:
                        log_fp.write(json.dumps(cluster_json) + '\n')
                else:
                    log_fp.write(json.dumps(cluster_json) + '\n')

    def log_cluster_policies(self, log_file='cluster_policies.log', acl_log_file='acl_cluster_policies.log'):
        policies_log = self.get_export_dir() + log_file
        acl_policies_log = self.get_export_dir() + acl_log_file
        # log all cluster policy definitions
        policy_ids = {}
        policies_list = self.get('/policies/clusters/list').get('policies', [])
        with open(policies_log, 'w', encoding="utf-8") as fp:
            for x in policies_list:
                policy_ids[x.get('policy_id')] = x.get('name')
                fp.write(json.dumps(x) + '\n')

        # get users list based on groups_to_keep
        users_list = []
        if self.groups_to_keep is not None:
            all_users = self.get('/preview/scim/v2/Users').get('Resources', None)
            users_list = list(set([user.get("emails")[0].get("value") for user in all_users
                                   for group in user.get("groups") if
                                   group.get("display") in self.groups_to_keep]))

        # log cluster policy ACLs, which takes a policy id as arguments
        with open(acl_policies_log, 'w', encoding="utf-8") as acl_fp:
            for pid in policy_ids:
                api = f'/preview/permissions/cluster-policies/{pid}'
                perms = self.get(api)
                perms['name'] = policy_ids[pid]

                # remove any ACLs that involve users/groups that have been filtered
                if users_list:
                    acls = [acl for acl in perms.get("access_control_list") if
                            (acl.get("group_name", "") in self.groups_to_keep) or
                            (acl.get("user_name", "") in users_list) or
                            (acl.get("group_name", "" == "users"))]
                    if acls:
                        perms["access_control_list"] = acls
                        acl_fp.write(json.dumps(perms) + '\n')
                else:
                    acl_fp.write(json.dumps(perms) + '\n')

    def log_instance_pools(self, log_file='instance_pools.log'):
        pool_log = self.get_export_dir() + log_file
        pools = self.get('/instance-pools/list').get('instance_pools', None)
        if pools:
            with open(pool_log, "w", encoding="utf-8") as fp:
                for x in pools:
                    fp.write(json.dumps(x) + '\n')

    def log_instance_profiles(self, log_file='instance_profiles.log'):
        ip_log = self.get_export_dir() + log_file
        ips = self.get('/instance-profiles/list').get('instance_profiles', None)
        if ips:
            with open(ip_log, "w", encoding="utf-8") as fp:
                for x in ips:
                    fp.write(json.dumps(x) + '\n')

    @staticmethod
    def is_excluded_cluster(cluster_name):
        # model endpoint clusters start with `mlflow-model-` prefix
        ml_model_pattern = "mlflow-model-"
        # dlt cluster names start with the `dlt-execution-` prefix
        dlt_execution_pattern = "dlt-execution-"
        # job clusters have specific format for legacy single task jobs, job-JOBID-run-RUNID
        re_expr_old = re.compile("job-\d+-run-\d+$")
        # job clusters have specific format for mutli-task jobs, job-JOBID-run-RUNID-{TASK_CLUSTER_NAME}
        re_expr_mtj = re.compile("job-\d+-run-\d+-.+$")
        if (re_expr_old.match(cluster_name) 
            or re_expr_mtj.match(cluster_name) 
            or cluster_name.startswith(ml_model_pattern)
            or cluster_name.startswith(dlt_execution_pattern)):
            return True
        return False

    def remove_automated_clusters(self, cluster_list, log_file='skipped_clusters.log'):
        """
        Automated clusters like job clusters or model endpoints should be excluded
        :param cluster_list: list of cluster configurations
        :return: cleaned list with automated clusters removed
        """
        clean_cluster_list = []
        with open(self.get_export_dir() + log_file, 'w', encoding="utf-8") as log_fp:
            for cluster in cluster_list:
                cluster_name = cluster['cluster_name']
                if self.is_excluded_cluster(cluster_name):
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
                logging.info(resp.get('message', None))
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
            logging.error(command)
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
            logging.error(end_results.get('summary', None))
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

