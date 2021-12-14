#!/bin/bash
#
# Used to create a key-pair which can be used to launch a jumpbox via ./launch_cluster.sh
# Creates a key-pair of name KEY_NAME in the region REGION. The script saves the generated private key to
# local file: $KEY_NAME-$REGION.pem
# Requirements:
# - aws cli
#   - e.g. brew install awscli
#
# - jq
#   - e.g. brew install jq

set -e
KEY_NAME=$1
REGION=$2
AWS_PROFILE=$3

if [ $# -ne 3 ]; then
    echo "Usage: ./create_key_pairs.sh KEY_NAME REGION AWS_PROFILE"
    echo "e.g. ./create_key_pairs.sh kevin_testing us-west-2 aws-some-profile"
    exit 1
fi

PRIVATE_KEY=$(aws ec2 create-key-pair --key-name $KEY_NAME --region $REGION --profile $AWS_PROFILE | jq .KeyMaterial)
if [ -z "$PRIVATE_KEY" ]; then
    echo "Failed to create a key-pair."
else
    echo $PRIVATE_KEY | jq -r . > $KEY_NAME-$REGION.pem
    chmod 400 $KEY_NAME-$REGION.pem
    echo "======================================================================================================="
    echo "Successfully created key-pair $KEY_NAME in AWS $REGION. Saved the private key as $KEY_NAME-$REGION.pem"
    echo "======================================================================================================="
fi
