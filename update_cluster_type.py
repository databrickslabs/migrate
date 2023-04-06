import os
import shutil
import fileinput
import re

from dbclient import parser


def create_backup(log_dir):
    backup_folder = log_dir + "backup"
    if not os.path.exists(backup_folder):
        shutil.copytree(log_dir, backup_folder)
        print(f"Created backup folder: {backup_folder}")


def translate_cluster_types(log_dir):
    logs_to_update = ['clusters.log', 'cluster_policies.log', 'instance_pools.log', 'jobs.log']

    target_cluster_type = 'i3.xlarge'

    # FIXME: this mapping should be updated to a more meaningful translation
    azure_aws_type_mapping = {
        'Standard_DS3_v2': target_cluster_type,
        'Standard_DS4_v2': target_cluster_type,
        'Standard_DS5_v2': target_cluster_type,
        'Standard_D8_v3': target_cluster_type,
        'Standard_NC4as_T4_v3': target_cluster_type
    }

    for logfile in logs_to_update:
        # copying the file
        source_path = log_dir + logfile

        with fileinput.FileInput(source_path, inplace=True) as fp:
            for line in fp:
                for (old, new) in azure_aws_type_mapping.items():
                    line = line.replace(old, new)
                # the inline replacement of FileInput writes back the changes on the line with calling "print"
                print(line, end='')

    print("Translating cluster types complete")


def translate_instance_pool_attributes(log_dir):
    logs_to_update = ['instance_pools.log', 'jobs.log']

    for logfile in logs_to_update:
        # copying the file
        source_path = log_dir + logfile

        with fileinput.FileInput(source_path, inplace=True) as fp:
            for line in fp:
                # adapt attribute prefix
                line = line.replace("azure_attributes", "aws_attributes")

                # remove Azure suffix
                line = line.replace("_AZURE", "")

                # adapt spot bid price configuration to AWS syntax with default value of 100%
                line = re.sub("\"spot_bid_max_price\": -?\d*\.?\d*", "\"spot_bid_price_percent\": 100", line)

                # the inline replacement of FileInput writes back the changes on the line with calling "print"
                print(line, end='')

    print("Translating instance pool attributes complete")


def update_email_addresses(log_dir, replace_email):
    # parse list of e-mail mapping pairs. Format is:  old1@email.com:new1@e-mail.com,old2email.com:new2@email.com
    email_pairs = replace_email.split(',')
    print(str(len(email_pairs)) + ' emails found to replace')
    for email_pair in email_pairs:
        if len(email_pair.split(':')) < 2:
            print(
                'Syntax error in e-mail ' + email_pair + '. Old e-mail address and new e-mail address new to be separated by a :')
        else:
            old_email = email_pair.split(':')[0]
            new_email = email_pair.split(':')[1]
            print('Replacing old e-mail: ' + old_email + ' with new e-mail ' + new_email)
            _update_email_address(log_dir, old_email, new_email)


def _update_email_address(log_dir, old_email_address, new_email_address):
    # NOTE: the original implementation of `update_email_addresses` only updates a subset of the files below, but we
    # expect that the email addresses need to be adapted in all files where they can be found.
    logs_to_update = [
            'users.log', 'acl_jobs.log', 'acl_clusters.log',
            'acl_cluster_policies.log', 'acl_notebooks.log', 'acl_directories.log',
            'acl_repos.log', 'clusters.log', 'jobs.log', 'repos.log', 'secret_scopes_acls.log',
            'user_dirs.log', 'user_name_to_user_id.log', 'user_workspace.log'
        ]

    for logfile in logs_to_update:
        # copying the file
        source_path = log_dir + logfile

        if os.path.exists(source_path):
            _update_email_address_in_file(source_path, old_email_address, new_email_address)

    # update the path for user notebooks in bulk export mode
    bulk_export_dir = log_dir + 'artifacts/Users/'
    old_bulk_export_dir = bulk_export_dir + old_email_address
    new_bulk_export_dir = bulk_export_dir + new_email_address
    if os.path.exists(old_bulk_export_dir):
        os.rename(old_bulk_export_dir, new_bulk_export_dir)

    # update the path for user notebooks in single user export mode
    single_user_dir = log_dir + 'user_exports/'
    old_single_user_dir = single_user_dir + old_email_address
    new_single_user_dir = single_user_dir + new_email_address
    if os.path.exists(old_single_user_dir):
        os.rename(old_single_user_dir, new_single_user_dir)
    old_single_user_nbs_dir = new_single_user_dir + '/user_artifacts/Users/' + old_email_address
    new_single_user_nbs_dir = new_single_user_dir + '/user_artifacts/Users/' + new_email_address
    if os.path.exists(old_single_user_nbs_dir):
        os.rename(old_single_user_nbs_dir, new_single_user_nbs_dir)

    # update email in groups
    groups_path = log_dir + "groups"
    group_files = [f for f in os.listdir(groups_path) if os.path.isfile(os.path.join(groups_path, f))]
    for f in group_files:
        group_file_path = os.path.join(groups_path, f)
        _update_email_address_in_file(group_file_path, old_email_address, new_email_address)

    print(f"Update email address {old_email_address} to {new_email_address}")


def _update_email_address_in_file(source_path, old_email_address, new_email_address):
    with fileinput.FileInput(source_path, inplace=True) as fp:
        for line in fp:
            # the inline replacement of FileInput writes back the changes on the line with calling "print"
            print(line.replace(old_email_address, new_email_address), end='')


if __name__ == '__main__':
    # parse arguments
    parser = parser.get_export_preparation_parser()
    args = parser.parse_args()

    # enforce correct path syntax
    export_dir = args.set_export_dir
    if args.set_export_dir.rstrip()[-1] != '/':
        export_dir = export_dir + '/'
    else:
        export_dir = export_dir

    # add session
    log_dir = export_dir + args.session + '/'

    create_backup(log_dir)

    translate_cluster_types(log_dir)

    translate_instance_pool_attributes(log_dir)

    if args.replace_email:
        update_email_addresses(log_dir, args.replace_email)
