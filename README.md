# Databricks Workspace Migration Tools

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
| DBFS Mounts    | Supported    | Unsupported  |
| Secrets        | Unsupported  | Unsupported  |
| ML Models      | Unsupported  | Unsupported  |
| Table ACLs     | Unsupported  | Unsupported  |

**Note**: To download **notebooks**, run `--workspace` first to log all notebook paths so we can easily scan and download all notebooks. 
Once complete, run `--download` to download the full set of logged notebooks. 

**Note**: Please verify that Workspace Access Control is enabled prior to migrating users to the new environment.

**Note**: To disable ssl verification pass the flag `--no-ssl-verification`.
If still getting SSL Error add the following to your current bash shell -
```
export REQUESTS_CA_BUNDLE=""
export CURL_CA_BUNDLE=""
```

## Installation steps

Installation requires:
* python >= 3.6

```bash
$ git clone https://github.com/databrickslabs/workspace-migration-tool
$ cd workspace-migration-tool
$ pip install .
```

## Usage

Example:
```
# export the cluster profiles to the demo environment profile in the Databricks CLI
$ databricks-migrate -v debug export --profile demo --clusters

# export a single users workspace
$ databricks-migrate -v debug export --profile demo --export-home example@foobar.com
```

Base help text:
```bash
$ databricks-migrate --help
Usage: databricks-migrate [OPTIONS] COMMAND [ARGS]...

Options:
  --version            1.0.0
  -v, --verbosity LVL  Either CRITICAL, ERROR, WARNING, INFO or DEBUG
  --help               Show this message and exit.

Commands:
  export           Export user workspace artifacts from Databricks
  import           Import user workspace artifacts into Databricks
  test_connection  Test cli connection

```

Export help text:
```
$ databricks-migrate export --help
Usage: databricks-migrate export [OPTIONS]

  Export user workspace artifacts from Databricks

Options:
  --users                Download all the users and groups in the workspace
  --workspace            Log all the notebook paths in the workspace.
                         (metadata only)

  --download             Download all notebooks for the environment
  --libs                 Log all the libs for the environment
  --clusters             Log all the clusters for the environment
  --jobs                 Log all the job configs for the environment
  --metastore            Log all the metastore table definitions
  --database TEXT        Database name to export for the metastore. Single
                         database name supported

  --iam TEXT             IAM Instance Profile to export metastore entires
  --mounts               Log all mount points.
  --export-home TEXT     User workspace name to export, typically the users
                         email address

  --skip-failed          Skip retries for any failed exports.
  --azure                Run on Azure. (Default is AWS)
  --no-ssl-verification  Set Verify=False when making http requests.
  --debug                Debug Mode. Shows full stack trace on error.
  --profile TEXT         CLI connection profile to use. The default profile is
                         "DEFAULT".

  -h, --help             Show this message and exit.
```

Import help text:
```bash
$ databricks-migrate import --help
Usage: databricks-migrate import [OPTIONS]

  Import user workspace artifacts into Databricks

Options:
  --users                Import all the users and groups from the logfile.
  --workspace            Import all notebooks from export dir into the
                         workspace.

  --archive-missing      Import all missing users into the top level /Archive/
                         directory.

  --libs                 Import all the libs from the logfile into the
                         workspace.

  --clusters             Import all the cluster configs for the environment
  --jobs                 Import all job configurations to the environment.
  --metastore            Import the metastore to the workspace.
  --skip-failed          Skip retries for any failed exports.
  --azure                Run on Azure. (Default is AWS)
  --no-ssl-verification  Set Verify=False when making http requests.
  --debug                Debug Mode. Shows full stack trace on error.
  --profile TEXT         CLI connection profile to use. The default profile is
                         "DEFAULT".

  -h, --help             Show this message and exit.
```


## Known Limitations

* Instance profiles (AWS only): Group access to instance profiles will take precedence. If a user is added to the role directly, and has access via a group, only the group access will be granted during a migration.  
* Notebooks: ACLs to folders will need to be reconfigured by users. By default, it will be restricted if Notebook ACLs are enabled. 
* Clusters: Cluster creator will be seen as the single admin user who migrated all the clusters. (Relevant for billing purposes)
  * Cluster permissions would need to manually be modified (Possibly available via private preview APIs)
  * Cluster creator tags cannot be updated. Added a custom tag with the original cluster creator for DBU tracking. 
* Jobs: Job owners will be seen as the single admin user who migrate the job configurations. (Relevant for billing purposes)
  * Jobs with existing clusters that no longer exist will be reset to the default cluster type
  * Jobs with older legacy instances will fail with unsupported DBR or instance types. See release notes for the latest supported releases. 
