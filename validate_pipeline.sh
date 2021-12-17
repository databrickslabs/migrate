#!/bin/bash

set -e

SRC_SESSION=$1
DST_SESSION=$2

if [ $# -ne 2 ]; then
  echo "Usage: ./validate_pipeline.sh SRC_SESSION DST_SESSION"
  echo "e.g. ./validate_pipeline.sh E20211209153920 E20211209154049"
  exit 1
fi

gunzip logs/$SRC_SESSION/table_acls/*.json.gz -kf
gunzip logs/$DST_SESSION/table_acls/*.json.gz -kf

python3 migration_pipeline.py --validate-pipeline \
  --validate-source-session=$SRC_SESSION \
  --validate-destination-session=$DST_SESSION

echo "############################ Validate notebook artifacts ####################################"
diff -ur logs/$SRC_SESSION/artifacts/  logs/$DST_SESSION/artifacts/ &> validation_notebooks.log || true
diffstat -s validation_notebooks.log
echo "See validation_notebooks.log for details."

echo "############################ Validate metastore tables ######################################"
diff -ur logs/$SRC_SESSION/metastore/  logs/$DST_SESSION/metastore/ &> validation_metastores.log || true
diffstat -s validation_metastores.log
echo "See validation_metastores.log for details."