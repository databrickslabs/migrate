#!/bin/bash
set -e

KEY_NAME=$1
REGION=$2
AWS_PROFILE=$3

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
