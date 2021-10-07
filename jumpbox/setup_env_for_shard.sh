#!/bin/bash
# ./setup_env_for_shard.sh SRC_SHARD_NAME
set -ex

if [ $# -ne 1 ]; then
        echo "Usage: ./setup_env_for_shard.sh SRC_SHARD_NAME"
        echo "e.g. ./setup_env_for_shard.sh shard-qa"
        exit 1
fi

SRC_SHARD_NAME=$1

mkdir $SRC_SHARD_NAME
cp refresh_migrate_script.sh $SRC_SHARD_NAME
cd $SRC_SHARD_NAME
git clone https://github.com/databrickslabs/migrate

# Commented the below lines because it will be a better user interface to let users pass in the host and token directly to python command line
# which can properly writes ~/.databrickscfg file. (The python script must rewrite ~/.databrickscfg upon token expiration anyway.
#
#printf '[%s]\nhost = %s\ntoken = %s\n\n' $SRC_SHARD_NAME $SRC_HOST $SRC_TOKEN >> ~/.databrickscfg
#
#if [ $# -eq 6 ]; then
#        DST_SHARD_NAME=$4
#        DST_HOST=$5
#        DST_TOKEN=$6
#        printf '[%s]\nhost = %s\ntoken = %s\n\n' $DST_SHARD_NAME $DST_HOST $DST_TOKEN >> ~/.databrickscfg
#fi
#
#echo "Your shard working directory $SRC_SHARD_NAME is properly setup."
