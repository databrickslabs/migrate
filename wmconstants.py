# List of all objectTypes that we export / import in WM
USER_OBJECT = "users"
INSTANCE_PROFILE_OBJECT = "instance_profiles"
GROUP_OBJECT = "groups"
WORKSPACE_NOTEBOOK_PATH_OBJECT = "notebook_paths"
WORKSPACE_NOTEBOOK_OBJECT = "notebooks"
WORKSPACE_DIRECTORY_OBJECT = "directories"
WORKSPACE_NOTEBOOK_ACL_OBJECT = "acl_notebooks"
WORKSPACE_DIRECTORY_ACL_OBJECT = "acl_directories"
METASTORE_TABLES = "metastore"
METASTORE_TABLES_ACL = "metastore_acl"
CLUSTER_OBJECT = "clusters"
INSTANCE_POOL_OBJECT = "instance_pools"
JOB_OBJECT = "jobs"
SECRET_OBJECT = "secrets"
# Migration pipeline placeholder constants
MIGRATION_PIPELINE_OBJECT_TYPE = "tasks"

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
]
