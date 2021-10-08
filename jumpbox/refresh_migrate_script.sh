# Use this script to pull the latest version of the workspace migration repo.
# Use this script under migreate/ or migreate/$shardName
#!/bin/bash
set -ex
if [ $# -eq 0 ]; then
        cd migrate
elif [ $# -eq 1 ]; then
        cd $1/migrate
else
        echo "Usage: ./refresh_migrate_script.sh [SHARD_NAME]"
        exit 1
fi
git fetch origin master
git merge origin/master
