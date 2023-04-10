# Databricks Migration Tool

This is a migration package to log all Databricks resources for backup and/or migrating to another Databricks workspace.
Migration allows a Databricks organization to move resources between Databricks Workspaces,
to move between different cloud providers, or to move to different regions / accounts.

This package is based on python 3.6 and DBR 6.x+ releases.  
Python 3.7 or above is recommended if one is also exporting/importing MLflow objects.

> **Note:** This tool does not support windows currently since path resolution is different from mac / linux.  

This package also uses credentials from the 
[Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html).

## Table of Contents
- [Pre-Requisites](#pre-requisites)
- [Setup](#setup)
- [Migration Components](#migration-components)
- [Import using pipeline](#import-using-pipeline)
  - [Pipeline parameters](#pipeline-parameters)
  - [Exporting the Workspace](#exporting-the-workspace)
  - [Recommended parameters and checkpointing](#recommended-parameters-and-checkpointing)
  - [Updating the AWS Account ID](#updating-the-aws-account-id)
  - [Importing the Workspace](#importing-the-workspace)
  - [Validation](#validation)
- [Limitations](#limitations)

## Pre-Requisites
To use this migration tool, you'll need:  
* An environment running linux with python, pip, git, and the databricks CLI installed.
* Admin access to both the old and new databricks accounts in the form of a [Personal Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html).

## Setup

> Click to expand & collapse tasks
<details><summary><strong>1. Generate Tokens </strong></summary>

**Generate Access Tokens for both the old and new databricks accounts**

1. Click ![settings icon](https://docs.databricks.com/_images/user-settings-icon.png)User Settings Icon Settings in the lower left corner of your Databricks workspace
2. Click on **Access Tokens** tab
3. Click on **Generate New Token** button. ![generate token](https://docs.databricks.com/_images/generate-token.png)
4. Copy the generated token and store in a secure location.

Be sure to keep a file with the url for both the old and new databricks account
Add the old and new token and the old and new Instance ID if applicable.  You'll need easy access to all of these things when running the migration tool.
</details>
<details><summary><strong>2. Setup databricks-cli profiles</strong></summary>

**In order to run the migration tool from your linux shell**

Create a profile for the old workspace by typing:

```bash
databricks configure --token --profile oldWS
```
In this case oldWS is the profile name you'll refer to for running the migration tool `export_db.py` file within the old databricks account.

**When you use the databricks cli configure command, you'll be prompted for 2 things**

1. `Databricks Host (should begin with https://)`: When this happens, enter the old databricks workspace URL that you captured in your file above.
2. `Token`: When this happens, paste in the token you generated for the old databricks account.


Repeat the steps above for the new databricks account and change the `oldWS` profile name to something like `newWS` in order to keep track of which account you're exporting FROM and which account you're importing TO.

Create a profile for the New workspace by typing:

```bash
databricks configure --token --profile newWS
```
In this case newWS is the profile name you'll refer to for running the migration tool `import_db.py` file within the new databricks account.
</details>
<details><summary><strong>3. Install package dependencies</strong></summary>

In order to set up the python environment, clone this repository and `python3 setup.py install` from the top-level project directory. 
</details>

---

## Migration Components
To use the migration tool see the details below to start running the tool in the order recommended to properly migrate files.

Support Matrix for Import and Export Operations:

| Component          | Export     | Import      |
|--------------------|------------|-------------|
| Users / Groups     | Supported  | Supported   |
| Clusters (w/ ACLs) | Supported  | Supported   |
| Notebooks (w/ ACLs)| Supported  | Supported   |
| Repos (w/ ACLs)    | Supported  | Supported*  |
| Metastore          | Supported  | Supported   |
| Jobs (w/ ACLs)     | Supported  | Supported   |
| Libraries          | Supported  | Unsupported |
| Secrets            | Supported  | Supported   |
| Table ACLs         | Supported  | Supported   |
| DBFS Mounts        | Supported  | Unsupported |
| ML Models          | Supported* | Supported*  |

> **Note on MLFlow Migration:**  
> MLFlow asset migration is currently only partially supported; Feature Store and Model Registry will not be migrated, for example. See [mlflow-export-import](https://github.com/mlflow/mlflow-export-import) for comprehensive MLflow migrations.

> **Note on DBFS Data Migration:**  
> DBFS is a protected object storage location on AWS and Azure.
> Please contact your Databricks support team for information about migrating DBFS resources.

> **Note on User Migration:**  
> During user / group import, users will be notified of the new workspace and account by default.
> To disable this behavior, please contact your Databricks account team. 

> **Note on Repos:**
> Private repos cannot be imported. These should be added manually using the original user credentials.

---

## Import using pipeline

The recommended method of exporting and importing is by using the Pipeline contained in `migration_pipeline.py`. This pipeline performs all export and import steps sequentially, and includes checkpointing parallelization features. 

### Pipeline parameters

```
python migration_pipeline.py -h
usage: migration_pipeline.py [-h] [--profile PROFILE] [--azure or gcp] [--silent] [--no-ssl-verification] [--debug] [--set-export-dir SET_EXPORT_DIR]
                             [--cluster-name CLUSTER_NAME] [--notebook-format {DBC,SOURCE,HTML}] [--overwrite-notebooks] [--archive-missing]
                             [--repair-metastore-tables] [--metastore-unicode] [--skip-failed] [--session SESSION] [--dry-run] [--export-pipeline] [--import-pipeline]
                             [--validate-pipeline] [--validate-source-session VALIDATE_SOURCE_SESSION] [--validate-destination-session VALIDATE_DESTINATION_SESSION]
                             [--use-checkpoint] [--skip-tasks SKIP_TASKS [SKIP_TASKS ...]] [--num-parallel NUM_PARALLEL] [--retry-total RETRY_TOTAL]
                             [--retry-backoff RETRY_BACKOFF] [--start-date START_DATE]
                             [--exclude-work-item-prefixes EXCLUDE_WORK_ITEM_PREFIXES [EXCLUDE_WORK_ITEM_PREFIXES ...]]

Export user(s) workspace artifacts from Databricks

optional arguments for import/export pipeline:
  -h, --help            show this help message and exit
  --profile PROFILE     Profile to parse the credentials
  --azure or --gcp      Run on Azure or GCP (Default is AWS)
  --silent              Silent all logging of export operations.
  --no-ssl-verification
                        Set Verify=False when making http requests.
  --debug               Enable debug logging
  --no-prompt           Skip interactive prompt/confirmation for workspace import.
  --set-export-dir SET_EXPORT_DIR
                        Set the base directory to export artifacts
  --cluster-name CLUSTER_NAME
                        Cluster name to export the metastore to a specific cluster. Cluster will be started.
  --notebook-format {DBC,SOURCE,HTML}
                        Choose the file format to download the notebooks (default: DBC)
  --overwrite-notebooks
                        Flag to overwrite notebooks to forcefully overwrite during notebook imports
  --archive-missing     Import all missing users into the top level /Archive/ directory.
  --repair-metastore-tables
                        Repair legacy metastore tables
  --metastore-unicode   log all the metastore table definitions including unicode characters
  --skip-failed         Skip retries for any failed hive metastore exports.
  --skip-missing-users  Skip failed principles during ACL import; for missing principles, this will result in open ACLs
  --session SESSION     If set, pipeline resumes from latest checkpoint of given session; Otherwise, pipeline starts from beginning and creates a new session.
  --dry-run             Dry run the pipeline i.e. will not execute tasks if true.
  --export-pipeline     Execute all export tasks.
  --import-pipeline     Execute all import tasks.
  --use-checkpoint      use checkpointing to restart from previous state
  --skip-tasks SKIP_TASK [SKIP_TASK ...]
                        Space-separated list of tasks to skip from the pipeline. Valid options are:
                         instance_profiles, users, groups, workspace_item_log, workspace_acls, notebooks, secrets,
                         clusters, instance_pools, jobs, metastore, metastore_table_acls, mlflow_experiments, mlflow_runs
  --keep-tasks KEEP_TASK [KEEP_TASK ...]
                        Space-separated list of tasks to run from the pipeline. See valid options in --skip-tasks. Overrides skip-tasks.
  --num-parallel NUM_PARALLEL
                        Number of parallel threads to use to export/import
  --retry-total RETRY_TOTAL
                        Total number or retries when making calls to Databricks API
  --retry-backoff RETRY_BACKOFF
                        Backoff factor to apply between retry attempts when making calls to Databricks API
  --start-date START_DATE
                        start-date format: YYYY-MM-DD. If not provided, defaults to past 30 days. Currently, only used for exporting ML runs objects.
  --groups-to-keep group [group ...]
                        List of groups to keep if selectively exporting assets. Only users (and their assets) belonging to these groups will be exported.
                        
options for validation pipeline:
  --validate-pipeline   Validate exported data between source and destination.
  --validate-source-session VALIDATE_SOURCE_SESSION
                        Session used by exporting source workspace. Only used for --validate-pipeline.
  --validate-destination-session VALIDATE_DESTINATION_SESSION
                        Session used by exporting destination workspace. Only used for --validate-pipeline.
```

### Exporting the Workspace

To export a workspace, run:

`python3 migration_pipeline.py --profile $SRC_PROFILE --export-pipeline --use-checkpoint [--session $SESSION_ID]`

Where `$SRC_PROFILE` is the Databricks profile for the source workspace, as configured during Setup, and `$SESSION_ID` is an optional session identifier used for subsequent checkpoint runs. All data is exported to a folder named according to the `$SESSION_ID` value under the logs folder - “logs/`$SESSION_ID`”. If `$SESSION_ID` is not specified, a random value will be generated.

#### Recommended parameters and checkpointing

As a starting point, we recommend using the following parameter values:
* retry-total=30
* num-parallel=8
* retry-backoff=1.0

These can be adjusted per your scenario if needed; in general, if API limits are being hit, you can increase `retry-backoff`, decrease `num-parallel`, or both. 

If script failure occurs, you can safely rerun the same command with --use-checkpoint and --session $SESSION_ID to let the migration pick up from the previous checkpoint and rerun.

#### Updating the AWS Account ID
If your source and destination workspaces are in different accounts, you will need to update the Instance Profile ARN accordingly during the migration. To do this, run the following command after exporting the workspace assets:

```
python3 export_db.py --profile $SRC_PROFILE --use-checkpoint --old-account-id $OLD_AWS_ACCT_ID --update-account-id $NEW_AWS_ACCT_ID --set-export-dir $EXPORT_DIR/$SESSION_ID
```

Where `EXPORT_DIR/SESSION_ID` is the directory and session ID used by your export job, `SRC_PROFILE` is the profile used to export the source workspace, `OLD_AWS_ACCT_ID` is the source AWS account ID, and `NEW_AWS_ACCT_ID` is the destination AWS account ID. Note that this will only update the ARN in the Instance Profiles; the same instance profiles must still exist in the destination workspace.

### Importing the Workspace

To import into a target workspace, run:

`python3 migration_pipeline.py --profile $DST_PROFILE --import-pipeline --use-checkpoint [--session $SESSION_ID]`

The same recommended parameters as above apply in the import workflow, and similarly, if a failure occurs, `--use-checkpoint` can be used to rerun from the last checkpoint.

### Validation

Simple workspace object validation can be performed once the import is completed by first exporting the contents of the *target* workspace:

`python3 migration_pipeline.py --profile $DST_PROFILE --export-pipeline --use-checkpoint --cluster-name`

And then running the `validate_pipeline.sh` script:

`./validate_pipeline.sh $SRC_EXPORT_SESSION_ID $DST_EXPORT_SESSION_ID`

Once this completes, check the console summary, as well as the logs folder (where a new folder should be generated).

---

<details><summary><strong>Import using step-by-step tools (not recommended)</strong></summary>

If desired, `export_db.py` and `import_db.py` can be run in a stepwise fashion. This is the legacy mode of running the tools, and in general is not recommended. If running the scripts separately, the following order of operations applies:
1. Export users and groups 
2. Export cluster templates
3. Export notebook metadata (listing of all notebooks)
4. Export notebook content 
5. Export job templates
6. Export Hive Metastore data 
7. Export Table ACLs

By default, artifacts are stored in the `logs/` directory, and `azure_logs/` for Azure artifacts. 
This is configurable with the `--set-export-dir` flag to specify the log directory.

#### Export Help Text
```
$ python export_db.py --help
usage: export_db.py [-h] [--users] [--workspace]
                    [--notebook-format {DBC,SOURCE,HTML}] [--download]
                    [--libs] [--clusters] [--jobs] [--metastore] [--secrets]
                    [--metastore-unicode] [--cluster-name CLUSTER_NAME]
                    [--database DATABASE] [--iam IAM] [--skip-failed]
                    [--mounts] [--azure] [--profile PROFILE]
                    [--single-user SINGLE_USER] [--export-home EXPORT_HOME]
                    [--export-groups EXPORT_GROUPS] [--workspace-acls]
                    [--workspace-top-level-only] [--silent]
                    [--no-ssl-verification] [--debug] [--reset-exports]
                    [--set-export-dir SET_EXPORT_DIR] [--pause-all-jobs]
                    [--unpause-all-jobs]
                    [--update-account-id UPDATE_ACCOUNT_ID]
                    [--old-account-id OLD_ACCOUNT_ID]
                    [--replace-old-email REPLACE_OLD_EMAIL]
                    [--update-new-email UPDATE_NEW_EMAIL]
                    [--bypass-windows-check]
                    
Export full workspace artifacts from Databricks

optional arguments:
  -h, --help            show this help message and exit
  --users               Download all the users and groups in the workspace
  --workspace           Log all the notebook paths in the workspace. (metadata
                        only)
  --notebook-format {DBC,SOURCE,HTML}
                        Choose the file format to download the notebooks
                        (default: DBC)
  --download            Download all notebooks for the environment
  --libs                Log all the libs for the environment
  --clusters            Log all the clusters for the environment
  --jobs                Log all the job configs for the environment
  --metastore           log all the metastore table definitions
  --metastore-unicode   log all the metastore table definitions including
                        unicode characters
  --table-acls          log all table ACL grant and deny statements
  --cluster-name CLUSTER_NAME
                        Cluster name to export the metastore to a specific
                        cluster. Cluster will be started.
  --database DATABASE   Database name to export for the metastore and table
                        ACLs. Single database name supported
  --iam IAM             IAM Instance Profile to export metastore entires
  --skip-failed         Skip retries for any failed hive metastore exports.
  --mounts              Log all mount points.
  --azure               Run on Azure. (Default is AWS)
  --profile PROFILE     Profile to parse the credentials
  --export-home EXPORT_HOME
                        User workspace name to export, typically the users
                        email address
  --export-groups EXPORT_GROUPS
                        Group names to export as a set. Includes group, users,
                        and notebooks.
  --workspace-acls      Permissions for workspace objects to export
  --workspace-top-level-only
                        Download only top level notebook directories
  --silent              Silent all logging of export operations.
  --no-ssl-verification
                        Set Verify=False when making http requests.
  --debug               Enable debug logging
  --reset-exports       Clear export directory
  --set-export-dir SET_EXPORT_DIR
                        Set the base directory to export artifacts
  --pause-all-jobs      Pause all scheduled jobs
  --unpause-all-jobs    Unpause all scheduled jobs
  --update-account-id UPDATE_ACCOUNT_ID
                        Set the account id for instance profiles to a new
                        account id
  --old-account-id OLD_ACCOUNT_ID
                        Old account ID to filter on
  --replace-old-email REPLACE_OLD_EMAIL
                        Old email address to update from logs
  --update-new-email UPDATE_NEW_EMAIL
                        New email address to replace the logs
```

#### Import Help Text
```
$ python import_db.py --help
usage: import_db.py [-h] [--users] [--workspace] [--workspace-top-level]
                    [--workspace-acls] [--notebook-format {DBC,SOURCE,HTML}]
                    [--import-home IMPORT_HOME] [--import-groups]
                    [--archive-missing] [--libs] [--clusters] [--jobs]
                    [--metastore] [--metastore-unicode] [--get-repair-log]
                    [--cluster-name CLUSTER_NAME] [--skip-failed] [--azure]
                    [--profile PROFILE] [--single-user SINGLE_USER]
                    [--no-ssl-verification] [--silent] [--debug]
                    [--set-export-dir SET_EXPORT_DIR] [--pause-all-jobs]
                    [--unpause-all-jobs] [--import-pause-status]
                    [--delete-all-jobs] [--last-session]
                                        
Import full workspace artifacts into Databricks

optional arguments:
  -h, --help            show this help message and exit
  --users               Import all the users and groups from the logfile.
  --workspace           Import all notebooks from export dir into the
                        workspace.
  --workspace-top-level
                        Import all top level notebooks from export dir into
                        the workspace. Excluding Users dirs
  --notebook-format {DBC,SOURCE,HTML}
                        Choose the file format of the notebook to import
                        (default: DBC)
  --workspace-acls      Permissions for workspace objects to import
  --import-home IMPORT_HOME
                        User workspace name to import, typically the users
                        email address
  --import-groups       Groups to import into a new workspace. Includes group
                        creation and user notebooks.
  --archive-missing     Import all missing users into the top level /Archive/
                        directory.
  --libs                Import all the libs from the logfile into the
                        workspace.
  --clusters            Import all the cluster configs for the environment
  --jobs                Import all job configurations to the environment.
  --metastore           Import the metastore to the workspace.
  --metastore-unicode   Import all the metastore table definitions with
                        unicode characters
  --table-acls          Import table acls to the workspace.
  --get-repair-log      Report on current tables requiring repairs
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
  --import-pause-status Import the pause status from jobs in the old workspace
  --delete-all-jobs     Delete all jobs
  --last-session        
                        The session to compare against. If set, the script compares current sesssion with the last session and only import updated and new notebooks. 
```

---

### Users and Groups

This section uses the [SCIM API](https://docs.databricks.com/dev-tools/api/latest/scim/index.html) to export / import 
user and groups.  
[Instance Profiles API](https://docs.databricks.com/dev-tools/api/latest/instance-profiles.html) used 
to export instance profiles that are tied to user/group entitlements.   
For AWS users, this section will log the instance profiles used for IAM access to resources. 

To export users / groups, use the following: (The profile name DEMO will be replaced with the profile you defined for your old databricks account)
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
<p style=color:red>If you experience errors when you try to import the clusters, it may be that you need to modify the clusters file from the logs directory to include the new instance profile if it's not the same as the one in the old databricks account.</p>

**To make changes to a cluster name to match the new databricks account**

you must edit the clusters log file after export. You do this by looking at the clusters file and identifying the old cluster instance profile which will include the old account number and the name of the instance profile.

OLD profile text from an AWS Databricks account:
`arn:aws:iam::111111111111:instance-profile/profileName`

The account number (111111111111) and profileName need to be found and replaced to migrate to the new account which may have a different account number and instance profile.

**To modify the clusters.log file run this sed operation**

```bash
sed -i 's/old-text/new-text/g' input.txt
```
https://unix.stackexchange.com/questions/32907/what-characters-do-i-need-to-escape-when-using-sed-in-a-sh-script

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
python import_db.py --profile NEW_DEMO --workspace-acls 
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
This will include notebooks, directories, and their corresponding ACLs. 

### Jobs

This section uses the [Jobs API](https://docs.databricks.com/dev-tools/api/latest/jobs.html)  
Job ACLs are exported and imported with this option.

```bash
python export_db.py --profile DEMO --jobs
```
If we're unable to find old cluster ids that are no longer available, we'll reset the job template 
to use a new default cluster. 

```bash
python import_db.py --profile NEW_DEMO --jobs
```

Imported jobs into the new workspace are paused by default. We do not want to have 2 jobs run simultaneously. 
Admins must pause their jobs with Databricks defined schedules using the following option:
```bash
python export_db.py --profile DEMO --pause-all-jobs
```

Un-pause all jobs in the new workspace:
```bash
python import_db.py --profile NEW_DEMO --unpause-all-jobs
```

If you want to unpause only the jobs which were not paused in the old workspace, you can use the following option:
```bash
python import_db.py --profile NEW_DEMO --import-pause-status
```

### Hive Metastore

This section uses an API to remotely run Spark commands on a cluster, this API is called 
[Execution Context](https://docs.databricks.com/dev-tools/api/1.2/index.html#execution-context)

By default, this will launch a small cluster in the `data/` folder to export the Hive Metastore data. 
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

# import all metastore entries
```bash
python import_db.py --profile newDEMO --metastore
```
To find legacy Hive tables that need to be repaired after a successful import, run the following:
```
python import_db.py --profile newDEMO --get-repair-log
```
Once completed, it will upload a log to the destination location. 
Use this [repair notebook](data/repair_tables_for_migration.py) to import into the destination environment to repair 
all tables. 

### Table ACLs
The Table ACLs component includes all objects to which access is controlled using
`DENY` and `GRANT` SQL statements:
- Catalog: included if all databases are exported
  - Database: included
    - Table: included
    - View: included (they are treated like tables with ObjectType `TABLE`)
- Anonymous Function: included 
- Any File: included

Unsupported object type:
- User Function: not included yet

This section uses the API to run notebooks on a cluster to perform the export and import.
(For details, please refer to the [export table ACL notebook](data/notebooks/Export_Table_ACLs.py) 
 or the [import table ACL notebook](data/notebooks/Import_Table_ACLs.py))

By default, this will launch a small cluster in the `data/` folder with `acls` suffix to export the table ACL data. 
(This cluster needs to have table ACLs enabled, and it must be run with an admin user)

```bash
# export all table ACL entries 
python export_db.py --profile DEMO --table-acls

# export all table ACL entries within a specific database
python export_db.py --profile DEMO --table-acls --database "my_db"
```

For large workspaces it is not uncommon to encounter some ACLs that cause problems when
exporting: in such cases, a special log entry is made (marked with `ERROR_!!!`) and the export
continues. At the end error counts will be provided, and the notebooks mentioned above
contain detailed information on how to investigate any issues. Most errors are encountered
with objects that are no longer functional anyway.

### Export Groups by Name
This functionality exports group(s), their members, and corresponding notebooks.  
This assumes an empty export directory to simplify the number of operations needed.  
This does **not** include IAM roles as those likely change while moving across workspaces. 

```bash
# reset the export directory and export a set of groups
python export_db.py --reset-export && python export_db.py --profile DEMO --export-groups 'groupA,groupB'

# import the groups that were exported
python import_db.py --profile newDEMO --import-groups
```

### Export / Import Top Level Notebooks
This will export all notebooks that are not a part of the `/Users/` directories to help migrate notebooks that are 
outside of personal workspace directories.  Usually, these will be notebooks in the '/Shared/' directory.

```bash
# reset the export directory and export the top level directories / notebooks
python export_db.py --profile DEMO --reset-export && python export_db.py --profile DEMO --workspace-top-level-only
# if ACLs are enabled, export the ACLs as well
python export_db.py --profile DEMO --workspace-acls

# import the groups that were exported
python import_db.py --profile newDEMO --workspace-top-level
# apply acls if needed 
python import_db.py --profile newDEMO --workspace-acls
```

### Export / Import of Secrets
This will export secret to allow migration of secrets to a new workspace.  
There is a limit to the size of the secret value which will print an error if this fails.  
```bash
# to export you must use a cluster
python export_db.py --profile DEMO --secrets --cluster-name "my_cluster"
# to import, you do not need a cluster
python import_db.py --profile newDEMO --secrets
```

### (Alpha version) Export / Import of MLflow experiments, experiment permissions, and runs objects
Note: Registered model, model version, and metric history are not supported yet.
Please see [mlflow-export-import](https://github.com/amesar/mlflow-export-import) for standalone MLflow migrations.

This will export and import the specified MLflow objects. Because MLflow objects depend on other object types such as
workspace directories, notebooks, etc. this command should run after the other objects are successfully exported/imported.

mlflow-runs are by default only exported for the past 30 days worth of data. The user can specify other dates but should
be aware of the performance impacts.

export
```
python3 export_db.py --profile $SRC --mlflow-experiments --use-checkpoint --num-parallel 4 
python3 export_db.py --profile $SRC --mlflow-experiments-permissions --use-checkpoint --num-parallel 4
python3 export_db.py --profile $SRC --mlflow-runs --use-checkpoint --num-parallel 4 --start-date 2022-02-26
```

import
```
python3 import_db.py --profile $DST --src-profile $SRC --mlflow-experiments --use-checkpoint --num-parallel 4
python3 import_db.py --profile $DST --src-profile $SRC  --mlflow-experiments-permissions --use-checkpoint --num-parallel 4 
python3 import_db.py --profile $DST --src-profile $SRC  --mlflow-runs --use-checkpoint --num-parallel 4
```
</details>

---

## Limitations:
**Note**: To disable ssl verification pass the flag `--no-ssl-verification`.
If still getting SSL Error add the following to your current bash shell:
```
export REQUESTS_CA_BUNDLE=""
export CURL_CA_BUNDLE=""
```
* Instance profiles (AWS only): Group access to instance profiles will take precedence. If a user is added to the role 
directly, and has access via a group, only the group access will be granted during a migration.  
* Clusters: Cluster creator will be seen as the single admin user who migrated all the clusters. (Relevant for billing 
purposes)
  * Cluster creator tags cannot be updated. Added a custom tag named `OriginalCreator` with the original cluster creator
   for DBU tracking.
* Jobs: Job owners will be seen as the single admin user who migrate the job configurations. (Relevant for billing 
purposes)
  * Jobs with existing clusters that no longer exist will be reset to the default cluster type
  * Jobs with older legacy instances will fail with unsupported DBR or instance types. See release notes for the latest
   supported releases. 

