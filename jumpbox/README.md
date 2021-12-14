# Workspace Migration JumpBox

This JumpBox is built off a light-weight AMI which was manually built off of an already configured EC2 instance.

## Prerequisites
One needs to have jq and aws-cli installed on the machine.
e.g. for mac,
```
brew install awscli
brew install jq
```

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

### How to create an instance off of the AMI and use?

#### Using ./create_key_pairs.sh and ./launch_jumpbox.sh
First if you don't have a key_pair uploaded in the region of the AWS, use ./create_key_pairs.sh to create a key-pair.
```
./create_key_pairs.sh KEY_NAME REGION AWS_PROFILE
```
It creates the key-pair and saves the private_key into a local file (e.g. $KEY_NAME-$REGION.pem)

Now, use the ./launch_jumpbox.sh to launch a jumpbox.
```
./launch_jumpbox.sh AMI_NAME REGION AWS_PROFILE KEY_NAME [INSTANCE_TYPE]
```
Follow the command output to ssh into your jumpbox.

#### Manual way
Go to the AWS console and the corresponding region.
Click EC2 page and click on the AMIs under Images tab on the left panel.
Choose the "Private images" and look for the latest AMI (e.g. wm-jumpbox-v1.02)
Click the image and "launch"
Follow the usual process of launching an EC2 instance (make sure to use your own private-key/public-key for SSh purpose)

Once the instance is launched, go to the EC2 instance and note the public IPv4 DNS (it's under Networking tab)
Using the public DNS and the keys you used to launch the EC2 instance, SSH into the instance:

ssh -i $YOUR_PRIVATE_KEY ec2-user@$EC@_PUBLIC_DNS
e.g. ssh -i kevin-kim-wm-builder-in-field-eng.pem ec2-user@ec2-54-202-97-209.us-west-2.compute.amazonaws.com

### ./setup_env_for_shard.sh $SRC_SHARD_NAME
e.g. `./setup_env_for_shard.sh shard-qa`
Sets up a new working directory for the SRC_SHARD_NAME. The new directory will have Workspace Migration script the user can use to migrate the workspace.


### ./refresh_migrate_script.sh [SRC_SHARD_NAME]
e.g. `./refresh_migrate_script.sh shard-qa`
Fetches and merges the latest changes of the workspace migration script.
When using under the SRC_SHARD_NAME working directory, one does not need to pass in any argument.

### Script execution (TODO)
```
python3 migration_pipeline.py --profile $SRC_PROFILE --export-pipeline --use-checkpoint --cluster-name $CLUSTER_NAME [--session $SESSION_ID]
python3 migration_pipeline.py --profile $DST_PROFILE --import-pipeline --use-checkpoint --session $SESSION_ID
```

