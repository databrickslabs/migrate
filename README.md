# Databricks Migration Tool

This is a migration package to log all Databricks resources for backup and/or migrating to another Databricks workspace.
Migration allows a Databricks organization to move resources between Databricks Workspaces, 
to move between different cloud providers, or to move to different regions / accounts.  

Packaged is based on python 3.6 and DBR 6.x and 7.x releases.

This package uses credentials from the 
[Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html)

Support Matrix for Import and Export Operations:

| Component         | Export       | Import       |
| ----------------- | ------------ | ------------ |
| Users / Groups    | Supported    | Supported    |
| Clusters (w/ ACLs)| Supported    | Supported    |
| Notebooks         | Supported    | Supported    |
| Notebooks ACLs    | Supported    | Supported    |
| Metastore         | Supported    | Supported    |
| Jobs (w/ ACLs)    | Supported    | Supported    |
| Libraries         | Supported    | Unsupported  |
| Secrets           | Unsupported  | Unsupported  |
| ML Models         | Unsupported  | Unsupported  |
| Table ACLs        | Unsupported  | Unsupported  |

**Note:** MLFlow objects cannot be exported / imported with this tool.
For more details, please look [here](https://github.com/amesar/mlflow-tools/tree/master/mlflow_tools/export_import)

## Order of Operations
1. Export users and groups 
2. Export cluster templates
3. Export notebook metadata (listing of all notebooks)
4. Export notebook content 
5. Export job templates
6. Export Hive Metastore data 

**Note:** During user / group import, users will be notified of the new workspace and account. This is required 
for them to set up their credentials to access the new workspace. We need the user to exist before loading their 
artifacts like notebooks, clusters, etc. 

By default, artifacts are stored in the `logs/` directory, and `azure_logs/` for Azure artifacts. 
This is configurable with the `--set-export-dir` flag to specify the log directory.

While exporting Libraries is supported, we do not have an implementation to import library definitions. 
## Table of Contents
- [Users and Groups](#users-and-groups)
- [Clusters](#Clusters)
- [Notebooks](#Notebooks)
- [Jobs](#Jobs)
- [Export Help Text](#export-help-text)
- [Import Help Text](#import-help-text)

### Users and Groups
This section uses the [SCIM API](https://docs.databricks.com/dev-tools/api/latest/scim/index.html) to export / import 
user and groups.  
[Instance Profiles API](https://docs.databricks.com/dev-tools/api/latest/instance-profiles.html) used 
to export instance profiles that are tied to user/group entitlements.   
For AWS users, this section will log the instance profiles used for IAM access to resources. 

To export users / groups, use the following:
```bash
python export_db.py --profile DEMO --users
```

To import these users:
```bash
python import_db.py --profile NEW_DEMO --users
```

If you plan to use this tool to export multiple workspaces, you can set the `--set-export-dir` directory to log 
artifacts into separate logging directories. 


### Clusters
The section uses the [Clusters APIs](https://docs.databricks.com/dev-tools/api/latest/clusters.html)  

```bash
python export_db.py --profile DEMO --clusters
```
This will export the following:
1. Cluster templates + ACLs
2. Instance pool definitions
3. Cluster policies + ACLs

```bash
python import_db.py --profile NEW_DEMO --clusters
```

### Notebooks
This section uses the [Workspace API](https://docs.databricks.com/dev-tools/api/latest/workspace.html)

This part is a 3 part process. 
1. Download all notebook locations and paths
2. Download all notebook contents for every path
3. Download all workspace ACLs

```bash
python export_db.py --profile DEMO --workspace
python export_db.py --profile DEMO --download
python export_db.py --profile DEMO --workspace-acls
```

To import into a new workspace:
```bash
python import_db.py --profile NEW_DEMO --workspace [--archive-missing]
```
If users have left your organization, their artifacts (notebooks / job templates) still exists. However, their user 
object no longer exists. During the migration, we can keep the old users notebooks into the top level 
directory `/Archive/{username}@domain.com`
Use the `--archive-missing` option to put these artifacts in the archive folder. 

**Single User Export/Import**  
The tool supports exporting single user workspaces using the following command:
```bash
# export a single users workspace
python export_db.py --profile DEMO --export-home example@foobar.com
```

The corollary is the `--import-home` option:
```bash
python import_db.py --profile NEW_DEMO --import-home example@foobar.com
```

### Jobs
This section uses the [Jobs API](https://docs.databricks.com/dev-tools/api/latest/jobs.html)  
Job ACLs are exported and imported with this option.

```bash
python export_db.py --profile DEMO --jobs
```
If we're unable to find old cluster ids that are no longer available, we'll reset the job template 
to use a new default cluster. 

### Hive Metastore
This section uses an API to remotely run Spark commands on a cluster, this API is called 
[Execution Context](https://docs.databricks.com/dev-tools/api/1.2/index.html#execution-context)

By default, this will launch an small cluster in the `data/` folder to export the Hive Metastore data. 
If you need a specific IAM role to export the metastore, use the `--cluster-name` option to connect to 
a specific cluster. 

By default, we will edit the cluster for every defined IAM role to loop through all failed exports in case the tool was 
missing IAM permissions. To disable looping through all failed exports, you can pass in `--skip-failed`

```bash
# export all metastore entries and brute force loop through all instance profiles / IAM roles
python export_db.py --profile DEMO --metastore

# export all metastore entries on the default cluster without retries
python export_db.py --profile DEMO --metastore --skip-failed 

# export all metastore entries on a specific cluster
python export_db.py --profile DEMO --metastore --cluster-name "Test"

# export all tables within a specific database
python export_db.py --profile DEMO --metastore --cluster-name "Test" --database "my_db"
```

#### Export Help Text
```
$ python export_db.py --help
usage: export_db.py [-h] [--users] [--workspace] [--download] [--libs]
                    [--clusters] [--jobs] [--metastore]
                    [--cluster-name CLUSTER_NAME] [--database DATABASE]
                    [--iam IAM] [--skip-failed] [--mounts] [--azure]
                    [--profile PROFILE] [--export-home EXPORT_HOME]
                    [--workspace-acls] [--silent] [--no-ssl-verification]
                    [--debug] [--set-export-dir SET_EXPORT_DIR]
                    [--pause-all-jobs] [--unpause-all-jobs]

Export full workspace artifacts from Databricks

optional arguments:
  -h, --help            show this help message and exit
  --users               Download all the users and groups in the workspace
  --workspace           Log all the notebook paths in the workspace. (metadata
                        only)
  --download            Download all notebooks for the environment
  --libs                Log all the libs for the environment
  --clusters            Log all the clusters for the environment
  --jobs                Log all the job configs for the environment
  --metastore           log all the metastore table definitions
  --cluster-name CLUSTER_NAME
                        Cluster name to export the metastore to a specific
                        cluster. Cluster will be started.
  --database DATABASE   Database name to export for the metastore. Single
                        database name supported
  --iam IAM             IAM Instance Profile to export metastore entires
  --skip-failed         Skip retries for any failed hive metastore exports.
  --mounts              Log all mount points.
  --azure               Run on Azure. (Default is AWS)
  --profile PROFILE     Profile to parse the credentials
  --export-home EXPORT_HOME
                        User workspace name to export, typically the users
                        email address
  --workspace-acls      Permissions for workspace objects to export
  --silent              Silent all logging of export operations.
  --no-ssl-verification
                        Set Verify=False when making http requests.
  --debug               Enable debug logging
  --set-export-dir SET_EXPORT_DIR
                        Set the base directory to export artifacts
  --pause-all-jobs      Pause all scheduled jobs
  --unpause-all-jobs    Unpause all scheduled jobs
```

#### Import Help Text
```
$ python import_db.py --help
usage: import_db.py [-h] [--users] [--workspace] [--workspace-acls]
                    [--import-home IMPORT_HOME] [--archive-missing] [--libs]
                    [--clusters] [--jobs] [--metastore]
                    [--cluster-name CLUSTER_NAME] [--skip-failed] [--azure]
                    [--profile PROFILE] [--no-ssl-verification] [--silent]
                    [--debug] [--set-export-dir SET_EXPORT_DIR]
                    [--pause-all-jobs] [--unpause-all-jobs]

Import full workspace artifacts into Databricks

optional arguments:
  -h, --help            show this help message and exit
  --users               Import all the users and groups from the logfile.
  --workspace           Import all notebooks from export dir into the
                        workspace.
  --workspace-acls      Permissions for workspace objects to import
  --import-home IMPORT_HOME
                        User workspace name to import, typically the users
                        email address
  --archive-missing     Import all missing users into the top level /Archive/
                        directory.
  --libs                Import all the libs from the logfile into the
                        workspace.
  --clusters            Import all the cluster configs for the environment
  --jobs                Import all job configurations to the environment.
  --metastore           Import the metastore to the workspace.
  --cluster-name CLUSTER_NAME
                        Cluster name to import the metastore to a specific
                        cluster. Cluster will be started.
  --skip-failed         Skip missing users that do not exist when importing
                        user notebooks
  --azure               Run on Azure. (Default is AWS)
  --profile PROFILE     Profile to parse the credentials
  --no-ssl-verification
                        Set Verify=False when making http requests.
  --silent              Silent all logging of import operations.
  --debug               Enable debug logging
  --set-export-dir SET_EXPORT_DIR
                        Set the base directory to import artifacts if the
                        export dir was a customized
  --pause-all-jobs      Pause all scheduled jobs
  --unpause-all-jobs    Unpause all scheduled jobs
```

#### FAQs / Limitations
**Note**: To disable ssl verification pass the flag `--no-ssl-verification`.
If still getting SSL Error add the following to your current bash shell -
```
export REQUESTS_CA_BUNDLE=""
export CURL_CA_BUNDLE=""
```

Limitations:
* Instance profiles (AWS only): Group access to instance profiles will take precedence. If a user is added to the role directly, and has access via a group, only the group access will be granted during a migration.  
* Clusters: Cluster creator will be seen as the single admin user who migrated all the clusters. (Relevant for billing purposes)
  * Cluster creator tags cannot be updated. Added a custom tag named `OriginalCreator` with the original cluster creator for DBU tracking. 
* Jobs: Job owners will be seen as the single admin user who migrate the job configurations. (Relevant for billing purposes)
  * Jobs with existing clusters that no longer exist will be reset to the default cluster type
  * Jobs with older legacy instances will fail with unsupported DBR or instance types. See release notes for the latest supported releases. 

