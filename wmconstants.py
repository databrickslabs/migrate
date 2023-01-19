# List of all objectTypes that we export / import in WM
USER_OBJECT = "users"
INSTANCE_PROFILE_OBJECT = "instance_profiles"
GROUP_OBJECT = "groups"
WORKSPACE_ITEM_LOG_OBJECT = "workspace_item_log"
WORKSPACE_NOTEBOOK_PATH_OBJECT = "notebook_paths"
WORKSPACE_NOTEBOOK_OBJECT = "notebooks"
WORKSPACE_DIRECTORY_OBJECT = "directories"
WORKSPACE_REPO_OBJECT = "repos"
WORKSPACE_NOTEBOOK_ACL_OBJECT = "acl_notebooks"
WORKSPACE_DIRECTORY_ACL_OBJECT = "acl_directories"
WORKSPACE_REPO_ACL_OBJECT = "acl_repos"
METASTORE_TABLES = "metastore"
METASTORE_TABLES_ACL = "metastore_acl"
CLUSTER_OBJECT = "clusters"
INSTANCE_POOL_OBJECT = "instance_pools"
JOB_OBJECT = "jobs"
JOB_ACL_OBJECT = "acl_jobs"
SECRET_OBJECT = "secrets"
MLFLOW_EXPERIMENT_OBJECT = "mlflow_experiments"
MLFLOW_EXPERIMENT_PERMISSION_OBJECT = "mlflow_experiments_permissions"
MLFLOW_RUN_OBJECT = "mlflow_runs"

# Migration pipeline placeholder constants
MIGRATION_PIPELINE_OBJECT_TYPE = "tasks"
IGNORE_ERROR_LIST = ['RESOURCE_ALREADY_EXISTS', 'FEATURE_DISABLED']

# Actions
WM_EXPORT = "export"
WM_IMPORT = "import"
WM_VALIDATE = "validate"

# List of task objects in a pipeline
INSTANCE_PROFILES = "instance_profiles"
USERS = "users"
GROUPS = "groups"
WORKSPACE_ITEM_LOG = "workspace_item_log"
WORKSPACE_ACLS = "workspace_acls"
NOTEBOOKS = "notebooks"
SECRETS = "secrets"
CLUSTERS = "clusters"
INSTANCE_POOLS = "instance_pools"
JOBS = "jobs"
METASTORE = "metastore"
METASTORE_TABLE_ACLS = "metastore_table_acls"
MLFLOW_EXPERIMENTS = "mlflow_experiments"
MLFLOW_EXPERIMENT_PERMISSION = "mlflow_experiments_permissions"
MLFLOW_RUNS = "mlflow_runs"

TASK_OBJECTS = [
    INSTANCE_PROFILES,
    USERS,
    GROUPS,
    WORKSPACE_ITEM_LOG,
    WORKSPACE_ACLS,
    NOTEBOOKS,
    SECRETS,
    CLUSTERS,
    INSTANCE_POOLS,
    JOBS,
    METASTORE,
    METASTORE_TABLE_ACLS,
    MLFLOW_EXPERIMENTS,
    MLFLOW_RUNS
]
