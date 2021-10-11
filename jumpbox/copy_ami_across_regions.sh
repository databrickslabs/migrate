#!/bin/bash
#
# Used for copying the source AMI to all the available regions for ST shards.
# ./copy_ami_across_regions.sh SRC_AMI_ID SRC_REGION AMI_NAME [AWS_PROFILE]
# Requirements:
# - aws cli
#   - e.g. brew install awscli
#
# - jq
#   - e.g. brew install jq
set -e

SRC_AMI_ID=$1
SRC_REGION=$2
AMI_NAME=$3
AWS_PROFILE=$4

if [ $# -ne 3 ] && [ $# -ne 4 ]; then
    echo "Usage: ./copy_ami_across_regions.sh SRC_AMI_ID SRC_REGION AMI_NAME [AWS_PROFILE]"
    echo "e.g. ./copy_ami_across_regions.sh ami-019974f1631980b80 us-west-2 wm-jumpbox-v1.01 aws-dev_databricks-power-user"
    exit 1
fi

# All available ST regions
declare -a StRegions=("us-east-1" "us-west-1" "us-west-2" "ap-south-1" "ap-northeast-2" "ap-southeast-1" "ap-southeast-2" "ap-northeast-1" "ca-central-1" "eu-central-1" "eu-west-1" "eu-west-3" "sa-east-1")

# Read the array values with space
for DST_REGION in "${StRegions[@]}"; do
    # Set profile argument if AWS_PROFILE is passed in to the commandline
    if [ ! -z $AWS_PROFILE ]; then
        PROFILE_ARG=" --profile $AWS_PROFILE"
    else
        PROFILE_ARG=""
    fi

    # For each of the destination regions,
    #   1. Skip the region if it is same as the source region.
    #   2. Exit the script if aws ec2 describe-images fails (e.g. Empty variable)
    #   3. Copy the image if describe-images returns [].
    #   4. Otherwise skip copying as the region already has the AMI_NAME
    if [ $DST_REGION != $SRC_REGION ]; then
        echo "$DST_REGION: "
        EXIST_IMAGES=$(aws ec2 describe-images --filters "Name=name,Values=$AMI_NAME" --region $DST_REGION $PROFILE_ARG | jq .Images)
        if [ -z "$EXIST_IMAGES" ]; then
            echo "A call to describe-images failed. Exiting the script.."
            exit 1
        elif [ "$EXIST_IMAGES" == '[]' ]; then
            aws ec2 copy-image --source-image-id $SRC_AMI_ID --source-region $SRC_REGION --region $DST_REGION --name $AMI_NAME $PROFILE_ARG
        else
            echo "Image $AMI_NAME already exists. Skipping $DST_REGION.."
            echo "$EXIST_IMAGES" | jq .'[].ImageId'
        fi
    else
        echo "Skipping $DST_REGION as it is the source region"
    fi
    echo $'\n\n'
done

