# Guide to copy job from one workspace to another using databricks-cli
1. Install
    - install python3
    - install databricks-cli with: `pip install databricks-cli`
2. set up authentication (either Azure Ad or Databricks token)
    1. Azure AD token
        - Unix: `export DATABRICKS_AAD_TOKEN=<Azure-AD-token>`
        - Windows: `setx DATABRICKS_AAD_TOKEN "<Azure-AD-token>" /M`
        - run: `databricks configure --aad-token -profile <profile-name>`
            - eg.:
            - source profile `databricks configure --aad-token -profile bp_source`
            - destination profile `configure --aad-token -profile bp_target`
    2. databricks PAT 
        - `databricks configure —token -profile <profile-name>`
        - Host: `https://adb-<workspace-id>.<random-number>.azuredatabricks.net`
        - Token: `...`
    - I think you don’t need to set up different profiles if you connect it with azure-ad that has access to all the workspaces
3. Export
    1. new workspace → cluster id-s 
        - save the cluster id’s that you want to associate with the job you’re creating
        - `databricks --profile bp_target clusters list`
    2. old workspace → export notebooks and job settings
        1. notebooks
            - `databricks --profile <profile-name> export_dir <source_path> <target_path>`
            - eg.: `databricks --profile bp_source export_dir /HiveUpgrade/migrateJob migrateJob`
        2. jobs
            - requirements:
                - run `databricks jobs configure --profile <profile-name> --version=2.1`
            - `databricks --profile bp_source jobs list`
            - `databricks --profile bp_source jobs get --job-id <job-id> > hive_job.json`
            - edit the `hive_jobs.json` file to accurately reflect the new workspace’s settings
                1. extract the dictionary assigned to settings key in the json (remove the other keys)
                2. change notebook paths for `<target_path>` notebook paths
                3. replace old cluster id-s with the new cluster ids
4. Import
    1. notebooks
        - `databricks --profile <profile-name> import_dir <source_path> <target_path>`
        - eg.: `databricks --profile bp import_dir migrateJob /HiveUpgrade`
    2. jobs
        - `databricks --profile bp jobs create --json-file hive_job.json`