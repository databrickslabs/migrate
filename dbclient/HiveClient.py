import ast
import os
import base64
import random
import wmconstants
import time
from datetime import timedelta
from timeit import default_timer as timer
import logging
import logging_utils
import re
from dbclient import *


class HiveClient(ClustersClient):

    def __init__(self, configs, checkpoint_service):
        super().__init__(configs, checkpoint_service)
        self._checkpoint_service = checkpoint_service

    @staticmethod
    def is_delta_table(local_path):
        with open(local_path, 'r', encoding="utf-8") as fp:
            for line in fp:
                lower_line = line.lower()
                if lower_line.startswith('using delta'):
                    return True
        return False

    @staticmethod
    def get_ddl_by_keyword_group(local_path):
        """
        return a list of DDL strings that are grouped by keyword arguments and their parameters
        """
        ddl_statement = []
        parameter_group = []
        with open(local_path, 'r', encoding="utf-8") as fp:
            for line in fp:
                raw = line.rstrip()
                if not raw:
                    # make sure it's not an empty line, continue if empty
                    continue
                if raw[0] == ' ' or raw[0] == ')':
                    parameter_group.append(raw)
                else:
                    if parameter_group:
                        ddl_statement.append(''.join(parameter_group))
                    parameter_group = [raw]
            ddl_statement.append(''.join(parameter_group))
        return ddl_statement

    @staticmethod
    def get_path_option_if_available(stmt):
        # parse the OPTIONS keyword and pull out the `path` parameter if it exists
        params = re.search(r'\((.*?)\)', stmt).group(1)
        params_list = list(map(lambda p: p.lstrip().rstrip(), params.split(',')))
        for x in params_list:
            if x.startswith('path'):
                return f'OPTIONS ( {x} )'
        return ''

    def is_table_location_defined(self, local_table_path):
        """ check if LOCATION or OPTIONS(path ..) are defined for the table
        """
        ddl_statement = self.get_ddl_by_keyword_group(local_table_path)
        for keyword_param in ddl_statement:
            if keyword_param.startswith('OPTIONS'):
                options_param = self.get_path_option_if_available(keyword_param)
                if options_param:
                    # if the return is not empty, the path option is provided which means its an external table
                    return True
            elif keyword_param.startswith('LOCATION'):
                # if LOCATION is defined, we know the external table location
                return True
        return False

    def get_local_tmp_ddl_if_applicable(self, current_local_ddl_path):
        """
        method to identify if we should update the current DDL if OPTIONS or TBLPROPERTIES keywords exist
        """
        ddl_statement = self.get_ddl_by_keyword_group(current_local_ddl_path)
        tmp_ddl_path = self.get_export_dir() + 'tmp_ddl.txt'
        return_tmp_file = False
        with open(tmp_ddl_path, 'w', encoding="utf-8") as fp:
            for keyword_param in ddl_statement:
                if keyword_param.startswith('OPTIONS'):
                    return_tmp_file = True
                    options_param = self.get_path_option_if_available(keyword_param)
                    if options_param:
                        fp.write(options_param + ' ')
                    continue
                elif keyword_param.startswith('TBLPROPERTIES'):
                    return_tmp_file = True
                    continue
                fp.write(keyword_param + ' ')
        if return_tmp_file:
            return tmp_ddl_path
        else:
            os.remove(tmp_ddl_path)
            return current_local_ddl_path

    def update_table_ddl(self, local_table_path, db_path):
        # check if the database location / path is the default DBFS path
        table_name = os.path.basename(local_table_path)
        is_db_default_path = db_path.startswith('dbfs:/user/hive/warehouse')
        ddl_statement = self.get_ddl_by_keyword_group(local_table_path)
        if (not is_db_default_path) and (not self.is_table_location_defined(local_table_path)) and (not self.is_ddl_a_view(ddl_statement)):
            # the LOCATION attribute is not defined and the Database has a custom location defined
            # therefore we need to add it to the DDL, e.g. dbfs:/db_path/table_name
            table_path = db_path + '/' + table_name
            location_stmt = f"\nLOCATION '{table_path}'"
            with open(local_table_path, 'a', encoding="utf-8") as fp:
                fp.write(location_stmt)
            return True
        return False

    def apply_table_ddl(self, local_table_path, ec_id, cid, db_path, has_unicode=False):
        """
        Run DDL command on destination workspace
        :param local_table_path: local file path to the table DDL
        :param ec_id: execution context id to run remote commands
        :param cid: cluster id to connect to
        :param db_path: database S3 / Blob Storage / ADLS path for the Database
        :param has_unicode: Whether the table definitions have unicode characters.
        :return: rest api response
        """
        # get file size in bytes
        updated_table_status = self.update_table_ddl(local_table_path, db_path)
        # update local table ddl to a new temp file with OPTIONS and TBLPROPERTIES removed from the DDL for delta tables
        if self.is_delta_table(local_table_path):
            local_table_path = self.get_local_tmp_ddl_if_applicable(local_table_path)

        f_size_bytes = os.path.getsize(local_table_path)
        if f_size_bytes > 1024 or has_unicode:
            # upload first to tmp DBFS path and apply
            # generate a random hash since we are multi-threaded and would collide otherwise
            file_hash = "%032x" % random.getrandbits(128)
            dbfs_path = f'/tmp/migration/tmp_import_ddl_{file_hash}.txt'
            path_args = {'path': dbfs_path}
            del_resp = self.post('/dbfs/delete', path_args)
            if self.is_verbose():
                logging.info(del_resp)
            file_content_json = {'files': open(local_table_path, 'r', encoding="utf-8")}
            put_resp = self.post('/dbfs/put', path_args, files_json=file_content_json)
            if self.is_verbose():
                logging.info(put_resp)
            spark_big_ddl_cmd = f'with open("/dbfs{dbfs_path}", "r", encoding="utf-8") as fp: tmp_ddl = fp.read(); spark.sql(tmp_ddl)'
            ddl_results = self.submit_command(cid, ec_id, spark_big_ddl_cmd)

            # clean up the temp file
            del_resp = self.post('/dbfs/delete', path_args)
            if self.is_verbose():
                logging.info(del_resp)

            return ddl_results
        else:
            with open(local_table_path, "r", encoding="utf-8") as fp:
                ddl_statement = fp.read()
                spark_ddl_statement = self.get_spark_ddl(ddl_statement)
                ddl_results = self.submit_command(cid, ec_id, spark_ddl_statement)
                return ddl_results

    def check_if_instance_profiles_exists(self, log_file='instance_profiles.log'):
        ip_log = self.get_export_dir() + log_file
        ips = self.get('/instance-profiles/list').get('instance_profiles', None)
        if ips:
            with open(ip_log, "w", encoding="utf-8") as fp:
                for x in ips:
                    fp.write(json.dumps(x) + '\n')
            return True
        return False

    def create_database_db(self, db_name, ec_id, cid, db_attributes):
        location = db_attributes.get('Location', '')
        if not location.startswith('dbfs:/user/hive/warehouse/'):
            create_stmt = f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{location}'"
        else:
            create_stmt = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        create_db_sql = f'spark.sql("{create_stmt}")'
        db_results = self.submit_command(cid, ec_id, create_db_sql)
        return db_results

    def get_database_detail_dict(self, db_log='database_details.log'):
        db_logfile = self.get_export_dir() + db_log
        all_db_json = {}
        with open(db_logfile, 'r', encoding="utf-8") as fp:
            for x in fp:
                db_json = json.loads(x)
                db_json_keys = db_json.keys()
                if 'Database Name' in db_json_keys:
                    db_name = db_json.pop('Database Name')
                else:
                    db_name = db_json.pop('Namespace Name')
                all_db_json[db_name] = db_json
        return all_db_json

    def set_desc_database_helper(self, cid, ec_id):
        """
        define the helper function on the cluster
        :param cid: cluster id to run against
        :param ec_id: execution id, aka spark session id
        :return: api response object
        """
        # replacement strings
        helper_func_cmd1 = """def get_db_json(db_name): import json; rows = spark.sql(f"DESC DATABASE EXTENDED \
            {db_name}").toJSON().collect(); return list(map(lambda x: json.loads(x), rows))"""
        helper_func_cmd2 = """def format_db_json(db_list): return dict(list(map(lambda x: \
            (x.get('database_description_item'), x.get('database_description_value')), db_list)))"""
        helper_func_cmd3 = "def get_db_details(db_name): return format_db_json(get_db_json(db_name))"
        resp1 = self.submit_command(cid, ec_id, helper_func_cmd1)
        resp2 = self.submit_command(cid, ec_id, helper_func_cmd2)
        resp3 = self.submit_command(cid, ec_id, helper_func_cmd3)
        return resp3

    def get_desc_database_details(self, db_name, cid, ec_id):
        """
        Returns a dict object of the `desc database extended {db_name}` command to include location, comment, etc fields
        :param db_name: database name to fetch
        :param cid: cluster id
        :param ec_id: execution id aka spark context id
        :return: database json object
        """
        desc_database_cmd = f'print(get_db_details(\"{db_name}\"))'
        results = self.submit_command(cid, ec_id, desc_database_cmd)
        if results['resultType'] != 'text':
            print(json.dumps(results) + '\n')
            raise ValueError("Desc database extended failure")
        db_json = ast.literal_eval(results['data'])
        return db_json

    def export_database(self, db_name, cluster_name=None, iam_role=None, metastore_dir='metastore/',
                        success_log='success_metastore.log',
                        has_unicode=False, db_log='database_details.log'):
        """
        :param db_name:  database name
        :param cluster_name: cluster to run against if provided
        :param iam_role: iam role to launch the cluster with
        :param metastore_dir: directory to store all the metadata
        :param has_unicode: whether the metadata has unicode characters to export
        :param db_log: specific database properties logfile
        :return:
        """
        # check if instance profile exists, ask users to use --users first or enter yes to proceed.
        start = timer()
        if cluster_name:
            cid = self.start_cluster_by_name(cluster_name)
            current_iam = self.get_iam_role_by_cid(cid)
        else:
            current_iam = iam_role
            cid = self.launch_cluster(current_iam)
        end = timer()
        logging.info("Cluster creation time: " + str(timedelta(seconds=end - start)))
        time.sleep(5)
        ec_id = self.get_execution_context(cid)
        checkpoint_metastore_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.METASTORE_TABLES)
        # if metastore failed log path exists, cleanup before re-running
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.METASTORE_TABLES, self.get_export_dir())
        success_metastore_log_path = self.get_export_dir() + success_log
        if os.path.exists(success_metastore_log_path):
            os.remove(success_metastore_log_path)
        database_logfile = self.get_export_dir() + db_log
        resp = self.set_desc_database_helper(cid, ec_id)
        if self.is_verbose():
            logging.info(resp)
        with open(database_logfile, 'w', encoding="utf-8") as fp:
            db_json = self.get_desc_database_details(db_name, cid, ec_id)
            fp.write(json.dumps(db_json) + '\n')
        os.makedirs(self.get_export_dir() + metastore_dir + db_name, exist_ok=True)
        self.log_all_tables(db_name, cid, ec_id, metastore_dir, error_logger,
                            success_metastore_log_path, current_iam, checkpoint_metastore_set, has_unicode)

    def export_hive_metastore(self, cluster_name=None, metastore_dir='metastore/', db_log='database_details.log',
                              success_log='success_metastore.log', has_unicode=False):
        start = timer()
        checkpoint_metastore_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.METASTORE_TABLES)
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.METASTORE_TABLES, self.get_export_dir())
        instance_profiles = self.get_instance_profiles_list()
        if cluster_name:
            cid = self.start_cluster_by_name(cluster_name)
            current_iam_role = self.get_iam_role_by_cid(cid)
        elif instance_profiles:
            # if any instance profile exists, lets start w/ this on the first cluster to launch and export
            current_iam_role = instance_profiles[0]
            cid = self.launch_cluster(iam_role=current_iam_role)
        else:
            current_iam_role = None
            cid = self.launch_cluster()
        end = timer()
        logging.info("Cluster creation time: " + str(timedelta(seconds=end - start)))
        time.sleep(5)
        ec_id = self.get_execution_context(cid)
        # if metastore failed log path exists, cleanup before re-running
        success_metastore_log_path = self.get_export_dir() + success_log
        database_logfile = self.get_export_dir() + db_log
        if os.path.exists(success_metastore_log_path):
            os.remove(success_metastore_log_path)
        all_dbs = self.get_all_databases(error_logger, cid, ec_id)
        resp = self.set_desc_database_helper(cid, ec_id)
        if self.is_verbose():
            logging.info(resp)
        with open(database_logfile, 'w', encoding="utf-8") as fp:
            for db_name in all_dbs:
                logging.info(f"Fetching details from database: {db_name}")
                os.makedirs(self.get_export_dir() + metastore_dir + db_name, exist_ok=True)
                db_json = self.get_desc_database_details(db_name, cid, ec_id)
                fp.write(json.dumps(db_json) + '\n')
                self.log_all_tables(db_name, cid, ec_id, metastore_dir, error_logger,
                                    success_metastore_log_path, current_iam_role, checkpoint_metastore_set, has_unicode)

        failed_log_file = logging_utils.get_error_log_file(
            wmconstants.WM_EXPORT, wmconstants.METASTORE_TABLES, self.get_export_dir())
        total_failed_entries = self.get_num_of_lines(failed_log_file)
        if (not self.is_skip_failed()) and self.is_aws() and total_failed_entries > 0:
            logging.info("Retrying failed metastore export with registered IAM roles")
            remaining_iam_roles = instance_profiles[1:]
            self.retry_failed_metastore_export(cid, failed_log_file, error_logger, remaining_iam_roles,
                                               success_metastore_log_path, has_unicode, checkpoint_metastore_set)
            logging.info("Failed count before retry: " + str(total_failed_entries))
            logging.info("Total Databases attempted export: " + str(len(all_dbs)))
        else:
            logging.error("Failed count: " + str(total_failed_entries))
            logging.info("Total Databases attempted export: " + str(len(all_dbs)))

    @staticmethod
    def get_num_of_lines(filename):
        if not os.path.exists(filename):
            return 0
        else:
            i = 0
            with open(filename, encoding="utf-8") as fp:
                for line in fp:
                    i += 1
            return i

    @staticmethod
    def get_spark_ddl(table_ddl):
        """
        Formats the provided DDL into spark.sql() command to run remotely
        """
        spark_ddl = 'spark.sql(""" {0} """)'.format(table_ddl)
        return spark_ddl

    @staticmethod
    def is_ddl_a_view(ddl_list):
        first_statement = ddl_list[0]
        if first_statement.startswith('CREATE VIEW'):
            return True
        return False

    def move_table_view(self, db_name, tbl_name, local_table_ddl, views_dir='metastore_views/'):
        metastore_view_dir = self.get_export_dir() + views_dir
        ddl_statement = self.get_ddl_by_keyword_group(local_table_ddl)
        if self.is_ddl_a_view(ddl_statement):
            dst_local_ddl = metastore_view_dir + db_name + '/' + tbl_name
            os.rename(local_table_ddl, dst_local_ddl)
            return True
        return False

    def import_hive_metastore(self, cluster_name=None, metastore_dir='metastore/', views_dir='metastore_views/',
                              has_unicode=False, should_repair_table=False):
        metastore_local_dir = self.get_export_dir() + metastore_dir
        metastore_view_dir = self.get_export_dir() + views_dir
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.METASTORE_TABLES, self.get_export_dir())
        checkpoint_metastore_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_IMPORT, wmconstants.METASTORE_TABLES)
        os.makedirs(metastore_view_dir, exist_ok=True)
        (cid, ec_id) = self.get_or_launch_cluster(cluster_name)
        # get local databases
        db_list = self.listdir(metastore_local_dir)
        # make directory in DBFS root bucket path for tmp data
        self.post('/dbfs/mkdirs', {'path': '/tmp/migration/'})
        # iterate over the databases saved locally
        all_db_details_json = self.get_database_detail_dict()
        for db_name in db_list:
            # create a dir to host the view ddl if we find them
            os.makedirs(metastore_view_dir + db_name, exist_ok=True)
            # get the local database path to list tables
            local_db_path = metastore_local_dir + db_name
            # get a dict of the database attributes
            database_attributes = all_db_details_json.get(db_name, {})
            if not database_attributes:
                logging.info(all_db_details_json)
                raise ValueError('Missing Database Attributes Log. Re-run metastore export')
            create_db_resp = self.create_database_db(db_name, ec_id, cid, database_attributes)
            if logging_utils.log_response_error(error_logger, create_db_resp):
                logging.error(f"Failed to create database {db_name} during metastore import. Check "
                              f"failed_import_metastore.log for more details.")
                continue
            db_path = database_attributes.get('Location')
            if os.path.isdir(local_db_path):
                # all databases should be directories, no files at this level
                # list all the tables in the database local dir
                tables = self.listdir(local_db_path)
                for tbl_name in tables:
                    # build the path for the table where the ddl is stored
                    full_table_name = f"{db_name}.{tbl_name}"
                    if not checkpoint_metastore_set.contains(full_table_name):
                        logging.info(f"Importing table {full_table_name}")
                        local_table_ddl = metastore_local_dir + db_name + '/' + tbl_name
                        if not self.move_table_view(db_name, tbl_name, local_table_ddl):
                            # we hit a table ddl here, so we apply the ddl
                            resp = self.apply_table_ddl(local_table_ddl, ec_id, cid, db_path, has_unicode)
                            if not logging_utils.log_response_error(error_logger, resp):
                                checkpoint_metastore_set.write(full_table_name)
                        else:
                            logging.info(f'Moving view ddl to re-apply later: {db_name}.{tbl_name}')
            else:
                logging.error("Error: Only databases should exist at this level: {0}".format(db_name))
            self.delete_dir_if_empty(metastore_view_dir + db_name)
        views_db_list = self.listdir(metastore_view_dir)
        for db_name in views_db_list:
            local_view_db_path = metastore_view_dir + db_name
            database_attributes = all_db_details_json.get(db_name, '')
            db_path = database_attributes.get('Location')
            if os.path.isdir(local_view_db_path):
                views = self.listdir(local_view_db_path)
                for view_name in views:
                    full_view_name = f'{db_name}.{view_name}'
                    if not checkpoint_metastore_set.contains(full_view_name):
                        logging.info(f"Importing view {full_view_name}")
                        local_view_ddl = metastore_view_dir + db_name + '/' + view_name
                        resp = self.apply_table_ddl(local_view_ddl, ec_id, cid, db_path, has_unicode)
                        if logging_utils.log_response_error(error_logger, resp):
                            checkpoint_metastore_set.write(full_view_name)
                        logging.info(resp)

        # repair legacy tables
        if should_repair_table:
            self.report_legacy_tables_to_fix()
            self.repair_legacy_tables(cluster_name)

    def get_all_databases(self, error_logger, cid, ec_id):
        # submit first command to find number of databases
        # DBR 7.0 changes databaseName to namespace for the return value of show databases
        all_dbs_cmd = 'all_dbs = [x.databaseName for x in spark.sql("show databases").collect()]; print(len(all_dbs))'
        results = self.submit_command(cid, ec_id, all_dbs_cmd)
        if logging_utils.log_response_error(error_logger, results):
            raise ValueError("Cannot identify number of databases due to the above error")
        num_of_dbs = ast.literal_eval(results['data'])
        batch_size = 100    # batch size to iterate over databases
        num_of_buckets = (num_of_dbs // batch_size) + 1     # number of slices of the list to take

        all_dbs = []
        for m in range(0, num_of_buckets):
            db_slice = 'print(all_dbs[{0}:{1}])'.format(batch_size*m, batch_size*(m+1))
            results = self.submit_command(cid, ec_id, db_slice)
            db_names = ast.literal_eval(results['data'])
            for db in db_names:
                all_dbs.append(db)
                logging.info("Database: {0}".format(db))
        return all_dbs

    def log_all_tables(self, db_name, cid, ec_id, metastore_dir, error_logger, success_log_path, iam,
                       checkpoint_metastore_set, has_unicode=False):
        logging.info(f"Fetching tables from database: {db_name}")
        all_tables_cmd = 'all_tables = [x.tableName for x in spark.sql("show tables in {0}").collect()]'.format(db_name)
        results = self.submit_command(cid, ec_id, all_tables_cmd)
        results = self.submit_command(cid, ec_id, 'print(len(all_tables))')
        num_of_tables = ast.literal_eval(results['data'])

        batch_size = 100    # batch size to iterate over databases
        num_of_buckets = (num_of_tables // batch_size) + 1     # number of slices of the list to take

        with open(success_log_path, 'a', encoding="utf-8") as sfp:
            for m in range(0, num_of_buckets):
                tables_slice = 'print(all_tables[{0}:{1}])'.format(batch_size*m, batch_size*(m+1))
                results = self.submit_command(cid, ec_id, tables_slice)
                table_names = ast.literal_eval(results['data'])
                for table_name in table_names:
                    full_table_name = f'{db_name}.{table_name}'
                    if checkpoint_metastore_set.contains(full_table_name):
                        is_successful = True
                    else:
                        is_successful = self.log_table_ddl(cid, ec_id, db_name, table_name, metastore_dir,
                                                           error_logger, has_unicode)
                        logging.info(f"Exported {full_table_name}")

                    if is_successful:
                        success_item = {'table': full_table_name, 'iam': iam}
                        sfp.write(json.dumps(success_item))
                        sfp.write('\n')
                        self._persist_to_disk(sfp)
                        checkpoint_metastore_set.write(full_table_name)
                    else:
                        logging.info("Logging failure")
        return True

    def log_table_ddl(self, cid, ec_id, db_name, table_name, metastore_dir, error_logger, has_unicode):
        """
        Log the table DDL to handle large DDL text
        :param cid: cluster id
        :param ec_id: execution context id (rest api 1.2)
        :param db_name: database name
        :param table_name: table name
        :param metastore_dir: metastore export directory name
        :param err_log_path: log for errors
        :param has_unicode: export to a file if this flag is true
        :return: True for success, False for error
        """
        set_ddl_str_cmd = f'ddl_str = spark.sql("show create table {db_name}.{table_name}").collect()[0][0]'
        ddl_str_resp = self.submit_command(cid, ec_id, set_ddl_str_cmd)

        if ddl_str_resp['resultType'] != 'text':
            ddl_str_resp['table'] = '{0}.{1}'.format(db_name, table_name)
            error_logger.error(json.dumps(ddl_str_resp))
            return False
        get_ddl_str_len = 'ddl_len = len(ddl_str); print(ddl_len)'
        len_resp = self.submit_command(cid, ec_id, get_ddl_str_len)
        ddl_len = int(len_resp['data'])
        if ddl_len <= 0:
            len_resp['table'] = '{0}.{1}'.format(db_name, table_name)
            error_logger.error(json.dumps(len_resp) + '\n')
            return False
        # if (len > 2k chars) OR (has unicode chars) then export to file
        table_ddl_path = self.get_export_dir() + metastore_dir + db_name + '/' + table_name
        if ddl_len > 2048 or has_unicode:
            # create the dbfs tmp path for exports / imports. no-op if exists
            resp = self.post('/dbfs/mkdirs', {'path': '/tmp/migration/'})
            if logging_utils.log_response_error(error_logger, resp):
                return False
            # save the ddl to the tmp path on dbfs
            save_ddl_cmd = "with open('/dbfs/tmp/migration/tmp_export_ddl.txt', 'w', encoding='utf-8') as fp: fp.write(ddl_str)"
            save_resp = self.submit_command(cid, ec_id, save_ddl_cmd)
            if logging_utils.log_response_error(error_logger, save_resp):
                return False
            # read that data using the dbfs rest endpoint which can handle 2MB of text easily
            read_args = {'path': '/tmp/migration/tmp_export_ddl.txt'}
            read_resp = self.get('/dbfs/read', read_args)
            with open(table_ddl_path, "w", encoding="utf-8") as fp:
                fp.write(base64.b64decode(read_resp.get('data')).decode('utf-8'))
            return True
        else:
            export_ddl_cmd = 'print(ddl_str)'
            ddl_resp = self.submit_command(cid, ec_id, export_ddl_cmd)
            with open(table_ddl_path, "w", encoding="utf-8") as fp:
                fp.write(ddl_resp.get('data'))
            return True

    def retry_failed_metastore_export(self, cid, failed_metastore_log_path, error_logger, iam_roles_list, success_metastore_log_path,
                                      has_unicode, checkpoint_metastore_set, metastore_dir='metastore/'):
        # check if instance profile exists, ask users to use --users first or enter yes to proceed.
        if self.is_aws() and iam_roles_list:
            do_instance_profile_exist = True
        else:
            do_instance_profile_exist = False
        # get total failed entries
        total_failed_entries = self.get_num_of_lines(failed_metastore_log_path)
        if do_instance_profile_exist:
            logging.info("Instance profiles exist, retrying export of failed tables with each instance profile")
            err_log_list = []
            with open(failed_metastore_log_path, 'r', encoding="utf-8") as err_log:
                for table in err_log:
                    err_log_list.append(table)

            with open(success_metastore_log_path, 'a', encoding="utf-8") as sfp:
                for iam_role in iam_roles_list:
                    self.edit_cluster(cid, iam_role)
                    ec_id = self.get_execution_context(cid)
                    for table in err_log_list:
                        table_json = json.loads(table)
                        db_name = table_json['table'].split(".")[0]
                        table_name = table_json['table'].split(".")[1]

                        is_successful = self.log_table_ddl(cid, ec_id, db_name, table_name, metastore_dir,
                                                           error_logger, has_unicode)
                        if is_successful:
                            err_log_list.remove(table)
                            logging.info(f"Exported {db_name}.{table_name}")
                            success_item = {'table': f'{db_name}.{table_name}', 'iam': iam_role}
                            sfp.write(json.dumps(success_item))
                            sfp.write('\n')
                            self._persist_to_disk(sfp)
                            checkpoint_metastore_set.write(f'{db_name}.{table_name}')
                        else:
                            logging.error('Failed to get ddl for {0}.{1} with iam role {2}'.format(db_name, table_name,
                                                                                           iam_role))

                    os.remove(failed_metastore_log_path)
                    with open(failed_metastore_log_path, 'w', encoding="utf-8") as fm:
                        for table in err_log_list:
                            fm.write(table)
                    failed_count_after_retry = self.get_num_of_lines(failed_metastore_log_path)
                    logging.error("Failed count after retry: " + str(failed_count_after_retry))
        else:
            logging.info("No registered instance profiles to retry export")

    def report_legacy_tables_to_fix(self, metastore_dir='metastore/', fix_table_log='repair_tables.log'):
        metastore_local_dir = self.get_export_dir() + metastore_dir
        fix_log = self.get_export_dir() + fix_table_log
        db_list = self.listdir(metastore_local_dir)
        num_of_tables = 0
        with open(fix_log, 'w+', encoding="utf-8") as fp:
            for db_name in db_list:
                local_db_path = metastore_local_dir + db_name
                if os.path.isdir(local_db_path):
                    # all databases should be directories, no files at this level
                    # list all the tables in the database local dir
                    tables = self.listdir(local_db_path)
                    for tbl_name in tables:
                        local_table_ddl = local_db_path + '/' + tbl_name
                        if self.is_legacy_table_partitioned(local_table_ddl):
                            num_of_tables += 1
                            logging.info(f'Table needs repair: {db_name}.{tbl_name}')
                            fp.write(f'{db_name}.{tbl_name}\n')
        # once completed, check if the file exists
        log_size = os.stat(fix_log).st_size
        if log_size > 0:
            # repair log exists, upload to the platform to repair these tables
            logging.info(f"Total number of tables needing repair: {num_of_tables}")
            dbfs_path = '/tmp/migration/repair_ddl.log'
            logging.info(f"Uploading repair log to DBFS: {dbfs_path}")
            path_args = {'path': dbfs_path, 'overwrite': 'true'}
            file_content_json = {'files': open(fix_log, 'r', encoding="utf-8")}
            put_resp = self.post('/dbfs/put', path_args, files_json=file_content_json)
            if self.is_verbose():
                logging.info(put_resp)
        else:
            os.remove(fix_log)

    def repair_legacy_tables(self, cluster_name=None, fix_table_log='repair_tables.log',
                             failed_fix_table_log='failed_repair_tables.log'):
        (cid, ec_id) = self.get_or_launch_cluster(cluster_name)
        repair_table_list = self.get_export_dir() + fix_table_log
        failed_repair_table_log = self.get_export_dir() + failed_fix_table_log
        failed_repairs = 0
        if not os.path.exists(repair_table_list):
            logging.info("No tables to repair")
            return
        with open(repair_table_list, 'r') as fp, open(failed_repair_table_log, 'w', encoding="utf-8") as failed_log_p:
            for line in fp:
                fqdn_table = line.strip()
                repair_cmd = f"""spark.sql("MSCK REPAIR TABLE {fqdn_table}")"""
                resp = self.submit_command(cid, ec_id, repair_cmd)
                if resp.get('resultType', None) == 'error':
                    failed_repairs += 1
                    logging.info(f'Table failed repair: {fqdn_table}')
                    failed_log_p.write(json.dumps(resp) + "\n")

        logging.error(f"{failed_repairs} table(s) failed to repair. See errors in {failed_repair_table_log}")

    def is_legacy_table_partitioned(self, table_local_path):
        if not self.is_delta_table(table_local_path):
            ddl_group = self.get_ddl_by_keyword_group(table_local_path)
            for kw in ddl_group:
                kw_lower = kw.lower()
                if kw_lower.startswith('partitioned by'):
                    return True
        return False

    # We need to wait for the cluster to be ready to get the execution context, otherwise the API will fail.
    # Once the cluster is available via the clusters api, we still need to wait a few seconds for the driver to come online.
    def get_or_launch_cluster(self, cluster_name=None):
        if cluster_name:
            cid = self.start_cluster_by_name(cluster_name)
        else:
            cid = self.launch_cluster()
        time.sleep(2)
        ec_id = self.get_execution_context(cid)
        return cid, ec_id

    # Flush data to persist on disk before checkpoint write. There is risk of data loss if the data
    # is not persisted and checkpointed on system crash.
    def _persist_to_disk(self, fp):
        fp.flush()
        os.fsync(fp.fileno())
