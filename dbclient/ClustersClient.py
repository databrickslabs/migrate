import time, json
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
                  'cluster_id'}
  
  def get_spark_versions(self):
    return self.get("/clusters/spark-versions", printJson = True)

  def get_cluster_list(self, alive = True):
    """ Returns an array of json objects for the running clusters. Grab the cluster_name or cluster_id """
    cl = self.get("/clusters/list", printJson = False)
    if alive:
      running = filter(lambda x: x['state'] == "RUNNING", cl['clusters'])
      return list(running)
    else:
      return cl['clusters']  
  
  def log_cluster_configs(self, log_file='clusters.log'):
    cluster_log = self._export_dir + log_file
    # pinned by cluster_user is a flag per cluster
    cl = self.get_cluster_list(False)
    # filter on these items as MVP of the cluster configs 
    # https://docs.databricks.com/api/latest/clusters.html#request-structure
    with open(cluster_log, "w") as log_fp:
      for x in cl:
        run_properties = set(list(x.keys())) - self.create_configs
        for p in run_properties:
          del x[p]
        log_fp.write(json.dumps(x)+'\n')

  def log_instance_profiles(self, log_file='instance_profiles.log'):
    ip_log = self._export_dir + log_file
    ips = self.get('/instance-profiles/list')['instance_profiles']
    with open(ip_log, "w") as fp:
      for x in ips:
        fp.write(json.dumps(x) + '\n')
 
  def log_instance_pools(self, log_file='instance_pools.log'):
    ip_log = self._export_dir + log_file
    pools = self.get('/instance-pools/list')['instance_pools']
    with open(ip_log, "w") as fp:
      for x in pools:
        fp.write(json.dumps(x) + '\n')

  def get_global_init_scripts(self):
    """ return a list of global init scripts. Currently not logged """ 
    ls = self.get('/dbfs/list', {'path': '/databricks/init/'}).get('files', None)
    if ls is None:
      return []
    else:
      global_scripts = [{'path' : x['path']} for x in ls if x['is_dir'] == False] 
      return global_scripts
    
