## Databricks Migration Tools

This is a migration package to log all Databricks resources for backup and migration purposes. 
Packaged is based on python 3.6

This package uses credentials from the [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html)

Support Matrix for Import and Export Operations:

| Component      | Export       | Import       |
| -------------- | ------------ | ------------ |
| Notebooks      | Supported    | Supported    |
| Users / Groups | Supported    | Unsupported  |
| Metastore      | Supported    | Supported    |
| Clusters       | Supported    | Unsupported  |
| Jobs           | Supported    | Unsupported  |
| Libraries      | Supported    | Unsupported  |
| Secrets        | Unsupported  | Unsupported  |
| ML Models      | Unsupported  | Unsupported  |
| Table ACLs     | Unsupported  | Unsupported  |

Usage example:
```
# export the cluster profiles to the demo environment profile in the Databricks CLI
$ python export_db.py --profile DEMO --clusters
```

Export help text:
```
$ python export_db.py --help
usage: export_db.py [-h] [--users] [--workspace] [--download] [--libs]
                    [--clusters] [--jobs] [--metastore] [--azure]
                    [--profile PROFILE]

Migrate user workspaces in Databricks

optional arguments:
  -h, --help         show this help message and exit
  --users            Collect all the users and groups in the workspace
  --workspace        Collect all the users workspaces for the environment
  --download         Download all notebooks for the environment
  --libs             Collect all the libs for the environment
  --clusters         Collect all the clusters for the environment
  --jobs             Collect all the job configs for the environment
  --metastore        Collect all the metastore details
  --azure            Run on Azure. (Default is AWS)
  --profile PROFILE  Profile to parse the credentials
```

Import help text:
```
$ python import_db.py --help
usage: import_db.py [-h] [--users] [--workspace] [--download] [--libs]
                    [--clusters] [--jobs] [--metastore] [--azure]
                    [--profile PROFILE]

Migrate user workspaces in Databricks

optional arguments:
  -h, --help         show this help message and exit
  --users            Collect all the users and groups in the workspace
  --workspace        Collect all the users workspaces for the environment
  --download         Download all notebooks for the environment
  --libs             Collect all the libs for the environment
  --clusters         Collect all the clusters for the environment
  --jobs             Collect all the job configs for the environment
  --metastore        Collect all the metastore details
  --azure            Run on Azure. (Default is AWS)
  --profile PROFILE  Profile to parse the credentials
```
