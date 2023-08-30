# OS agnostic runtime for Databricks Migrate
[Databricks Migrate](https://github.com/databrickslabs/migrate) is a tool from [Databricks Labs](https://github.com/databrickslabs) that facilitates migration of objects in one Databricks workspace to another. The tool requires a Unix/Linux OS environment to operate. The packaging in this repo allows the Migrate tool to be run in other OS environments (i.e. Windows) using Docker.

## Local Environment Prerequisites
The set of scripts located in this directory have the following local dependencies:
* [Docker Desktop](https://docs.docker.com/desktop/install/windows-install/)

## Getting Started
_Note: For the following steps, the legacy workspace that is being migrated `from` is aliased as `oldWS`. The new workspace that is being migrated `to` is aliased as `newWS`._

1. Open the `.\databricks\.databrickscfg` file in this repo. There are entries for `oldWS` and `newWS` in this file. Replace the value for each `host` with the appropriate URL for that workspace. The URL will be of the format `https://<workspace-host>`.
2. For each workspace, [generate an access token](https://docs.databricks.com/dev-tools/auth.html#personal-access-tokens-for-users) for a user that has admin privileges.
3. Copy the generated token into the `.\databricks\.databrickscfg` file in this repo. Replace the value for each `token` in the file with the appropriate token for that workspace.
4. Save the changes to `.\databricks\.databrickscfg`.


## Environment Setup
With the docker daemon running, navigate to this directory in your local shell, and execute the following command.
```
docker build -t databricks-migrate .
```
Once the image is done building, execute the following command from your shell to start the container. Your shell will be redirected to `/opt/migrate` inside the running container. The tool is now ready for use.
```
docker run -it --name databricks-migrate -v .\databricks\.databrickscfg:/root/.databrickscfg -v .\databricks:/databricks databricks-migrate
```

## Example Usage
The following commands can be used to handle migration of specific Databricks objects.

Export objects from `oldWS`:
```
python migration_pipeline.py --azure --profile oldWS --export-pipeline --set-export-dir $SESSIONS_DIR --notebook-format SOURCE --keep-tasks users groups workspace_item_log workspace_acls notebooks clusters instance_pools jobs
```
Import objects to `newWS`:

> Note: the `--session` parameter needs to be set to that generated from the export session above. Here it is set as a environment variable $EXPORT_SESSION

```
python migration_pipeline.py --azure --profile newWS --import-pipeline --set-export-dir $SESSIONS_DIR --notebook-format SOURCE --session $EXPORT_SESSION --keep-tasks users groups workspace_item_log workspace_acls notebooks clusters instance_pools jobs
```

## Output and Logging
The `.\databricks` directory in this repo is mounted to the running container. This in combination with the preset $SESSIONS_DIR environment variable (see Usage above) allows the output of the migration pipeline to be available and persisted on the local host.
