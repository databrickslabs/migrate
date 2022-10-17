#!/bin/bash
#
# Used to launch a jumpbox EC2 instance with an AMI_NAME (e.g. wm-jumpbox-v1.02) into $REGION with $INSTANCE_TYPE.
# One can ssh into the jumpbox with the provided $KEY_NAME.
# If the invoker already has a running Jumpbox, the script does not create a new one.
# The script prints the command to ssh into the jumpbox at the end.
# Requirements:
# - aws cli
#   - e.g. brew install awscli
#
# - jq
#   - e.g. brew install jq
set -e

AMI_NAME=$1
REGION=$2
AWS_PROFILE=$3
KEY_NAME=$4
INSTANCE_TYPE=$5

if [ $# -ne 5 ] && [ $# -ne 4 ]; then
    echo "Usage: ./launch_jumpbox.sh AMI_NAME REGION AWS_PROFILE KEY_NAME [INSTANCE_TYPE]"
    echo "e.g. ./launch_jumpbox.sh wm-jumpbox-v1.02 us-west-2 aws-field-eng_databricks-power-user kevin_testing t2.micro"
    exit 1
fi

if [ -z $INSTNACE_TYPE ]; then
    INSTANCE_TYPE="t2.micro"
fi

echo "First checking if the user already has a running jumpbox in the region..."
whoami="$(whoami)"
ALREADY_EXISTING_JUMPBOX_ID=$(aws ec2 describe-instances --query 'Reservations[*].Instances[*].[InstanceId]' --filters "Name=tag:Name,Values=wm-$whoami" "Name=instance-state-name,Values=running,pending,stopping,stopped" --region $REGION --profile $AWS_PROFILE --output text)
if [ ! -z "$ALREADY_EXISTING_JUMPBOX_ID" ]; then
  echo "Jumpbox wm-$whoami already exists with id of $ALREADY_EXISTING_JUMPBOX_ID. The instance is in running, pending, stopping, or stopped state."
  echo "Terminate the instance if you want to launch a new jumpbox instance."
  exit 1
fi
echo "User does not have a running jumpbox in the $REGION."

echo "Checking if an AMI $AMI_NAME exists in $REGION..."
EXIST_IMAGES=$(aws ec2 describe-images --filters "Name=name,Values=$AMI_NAME" --region $REGION --profile $AWS_PROFILE | jq .Images)
if [ -z "$EXIST_IMAGES" ]; then
    echo "A call to describe-images failed. Exiting the script.. Please try again with the proper --profile"
    exit 1
elif [ "$EXIST_IMAGES" == '[]' ]; then
    echo "Image $AMI_NAME does not exist in region: $REGION. Please copy the image, or use an availalbe image names."
    exit 1
else
    IMAGE_ID="$(echo $EXIST_IMAGES | jq -r .'[].ImageId')"
fi

echo "Launch jumpbox using AMI: $IMAGE_ID, in region: $REGION..."
LAUNCHED_INSTANCE=$(aws ec2 run-instances --image-id $IMAGE_ID --instance-type $INSTANCE_TYPE --count 1 --key-name $KEY_NAME  --region $REGION --profile $AWS_PROFILE --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=wm-$whoami}]")

INSTANCES="$(echo $LAUNCHED_INSTANCE | jq .Instances)"
INSTANCE_ID="$(echo $INSTANCES | jq -r .'[].InstanceId')"
echo "Launched the jumpbox. Instance Id: $INSTANCE_ID."

# loop this command until | jq .InstanceStatuses | jq .'[].InstanceState.Name' == "running"
INSTANCE_STATE="Not Ready"
while [[ $INSTANCE_STATE != "running" ]]
do
    echo "Instance $INSTANCE_ID is in $INSTANCE_STATE state. Waiting for the instance to be in running state. Checking again in 5 seconds.."
    INSTANCE_STATE=$(aws ec2 describe-instance-status --instance-ids $INSTANCE_ID --region $REGION --profile $AWS_PROFILE | jq .InstanceStatuses | jq -r .'[].InstanceState.Name')
    if [ -z $INSTANCE_STATE ]; then
      INSTANCE_STATE="Not Ready"
    fi
    sleep 5
done

echo "Instance $INSTANCE_ID is in running state!"

PUBLIC_DNS=$(aws ec2 describe-instances --instance-ids $INSTANCE_ID --region $REGION --profile $AWS_PROFILE | jq .Reservations | jq .'[].Instances' | jq -r .'[].PublicDnsName')
echo "==============================================================="
echo "Ssh into your jumpbox using the following command: "
echo "ssh -i \$KEY_NAME.pem ec2-user@$PUBLIC_DNS"
echo "e.g. ssh -i $KEY_NAME-$REGION.pem ec2-user@$PUBLIC_DNS"
echo "==============================================================="
