## Databricks Migration Tools

This is a migration package to log all Databricks resources for backup and migration purposes. 
Packaged is based on python 3.6

This package uses credentials from the [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html)

Support Matrix for Import and Export Operations:

| Component      | Export       | Import       |
| -------------- | ------------ | ------------ |
| Notebooks      | Supported    | Supported    |
| Users / Groups | Supported    | Supported    |
| Metastore      | Supported    | Supported    |
| Clusters       | Supported    | Supported    |
| Jobs           | Supported    | Supported    |
| Libraries      | Supported    | Unsupported  |
| Secrets        | Unsupported  | Unsupported  |
| ML Models      | Unsupported  | Unsupported  |
| Table ACLs     | Unsupported  | Unsupported  |

MLFlow: `https://github.com/amesar/mlflow-tools/tree/master/mlflow_tools/export_import`

**Note**: To download **notebooks**, run `--workspace` first to log all notebook paths so we can easily scan and download all notebooks. 
Once complete, run `--download` to download the full set of logged notebooks. 

**Note**: Please verify that Workspace Access Control is enabled prior to migrating users to the new environment.

**Note**: To disable ssl verification pass the flag `--no-ssl-verification`.


Usage example:
```
# export the cluster profiles to the demo environment profile in the Databricks CLI
$ python export_db.py --profile DEMO --clusters

# export a single users workspace
$ python export_db.py --profile DEMO --export-home example@foobar.com
```

Export help text:
```
$ python export_db.py --help
usage: export_db.py [-h] [--users] [--workspace] [--download] [--libs]
                    [--clusters] [--jobs] [--metastore] [--azure]
                    [--profile PROFILE]

Export user workspace artifacts from Databricks

optional arguments:
  -h, --help            show this help message and exit
  --users               Download all the users and groups in the workspace
  --workspace           Log all the notebook paths in the workspace. (metadata
                        only)
  --download            Download all notebooks for the environment
  --libs                Log all the libs for the environment
  --clusters            Log all the clusters for the environment
  --jobs                Log all the job configs for the environment
  --metastore           Log all the metastore table definitions
  --azure               Run on Azure. (Default is AWS)
  --profile PROFILE     Profile to parse the credentials
  --export-home EXPORT_HOME
                        User workspace name to export, typically the users
                        email address
  --no-ssl-verification
                        Disable SSL verification
  --silent              Silent all logging of export operations.
```

Import help text:
```
$ python import_db.py --help
usage: import_db.py [-h] [--users] [--workspace] [--libs] [--clusters]
                    [--jobs] [--metastore] [--azure] [--profile PROFILE]

Import user workspace artifacts into Databricks

optional arguments:
  -h, --help         show this help message and exit
  --users            Import all the users and groups from the logfile.
  --workspace        Import all notebooks from export dir into the workspace.
  --archive-missing  Import all missing users into the top level /Archive/
                     directory.
  --libs             Import all the libs from the logfile into the workspace.
  --clusters         Import all the cluster configs for the environment
  --jobs             Import all job configurations to the environment.
  --metastore        Import the metastore to the workspace.
  --azure            Run on Azure. (Default is AWS)
  --profile PROFILE  Profile to parse the credentials
  --no-ssl-verification
                     Disable SSL verification
  --silent           Silent all logging of import operations.
```


Limitations:
* Instance profiles (AWS only): Group access to instance profiles will take precedence. If a user is added to the role directly, and has access via a group, only the group access will be granted during a migration.  
* Notebooks: ACLs to folders will need to be reconfigured by users. By default, it will be restricted if Notebook ACLs are enabled. 
* Clusters: Cluster creator will be seen as the single admin user who migrated all the clusters. (Relevant for billing purposes)
  * Cluster permissions would need to manually be modified (Possibly available via private preview APIs)
  * Cluster creator tags cannot be updated. Added a custom tag with the original cluster creator for DBU tracking. 
* Jobs: Job owners will be seen as the single admin user who migrate the job configurations. (Relevant for billing purposes)
  * Jobs with existing clusters that no longer exist will be reset to the default cluster type
  * Jobs with older legacy instances will fail with unsupported DBR or instance types. See release notes for the latest supported releases. 

