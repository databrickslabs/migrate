# Workspace Migration JumpBox

This JumpBox is built off a light-weight AMI which was manually built off of an already configured EC2 instance.

## AMI Development

Because the configuration is straightforward, simple and does not expect frequent changes, we are not using any automated AMI build pipeline.
If necessary to formalize the AMI build process, we recommend investing in AMI build tools such as [Ansible](https://docs.ansible.com/ansible/latest/collections/amazon/aws/ec2_ami_module.html) and [Packer](https://learn.hashicorp.com/tutorials/packer/aws-get-started-build-image?in=packer/aws-get-started).

To make changes on top of the current AMI, one can just spin up an EC2 instance off of the current AMI and build a new AMI.


###  AMI history (all in us-west-2) (starting from v1.02, AMIs are managed in aws-field-eng account)
wm-jumpbox-v1.01 (ami-019974f1631980b80 10/6/2021): Initial AMI with basic scripts and libraries
```
sudo yum install git -y
sudo yum install pip -y
pip install databricks-cli
python3 -m pip install requests
```

wm-jumpbox-v1.02 (ami-05f50c59b73d00e54 10/12/2021): Add copy_ami_across_regions.sh and jq library
lets users copy AMIs inside the jumpbox
```
sudo yum install jq -y
```

### AMI copy
One can copy the source AMI to all the ST available regions by running copy_ami_across_regions.sh script
`./copy_ami_across_regions.sh $SRC_AMI_ID $SRC_REGION $AMI_NAME [$AWS_PROFILE]`
e.g. `./copy_ami_across_regions.sh ami-05f50c59b73d00e54 us-west-2 wm-jumpbox-v1.02 aws-field-eng_databricks-power-user`

One can run this from locally or inside the jumpbox (as long as the aws configuration is set)

## Script Usage

### ./setup_env_for_shard.sh $SRC_SHARD_NAME
e.g. `./setup_env_for_shard.sh shard-qa`
Sets up a new working directory for the SRC_SHARD_NAME. The new directory will have Workspace Migration script the user can use to migrate the workspace.


### ./refresh_migrate_script.sh [SRC_SHARD_NAME]
e.g. `./refresh_migrate_script.sh shard-qa`
Fetches and merges the latest changes of the workspace migration script.
When using under the SRC_SHARD_NAME working directory, one does not need to pass in any argument.

###  TODO: python entry point
Takes
SRC_SHARD_NAME,  SRC_HOST,  SRC_TOKEN,
DST_SHARD_NAME, DST_HOST, DST_TOKEN
