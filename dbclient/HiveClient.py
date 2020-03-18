import ast
import json
import os
import time
from datetime import timedelta
from timeit import default_timer as timer

from dbclient import *


class HiveClient(dbclient):

    def get_latest_spark_version(self):
        versions = self.get('/clusters/spark-versions')['versions']
        v_sorted = sorted(versions, key=lambda i: i['key'], reverse=True)
        for x in v_sorted:
            img_type = x['key'].split('-')[1][0:5]
            if img_type == 'scala':
                return x

    def wait_for_cluster(self, cid):
        c_state = self.get('/clusters/get', {'cluster_id': cid})
        while c_state['state'] != 'RUNNING':
            c_state = self.get('/clusters/get', {'cluster_id': cid})
            print('Cluster state: {0}'.format(c_state['state']))
            time.sleep(2)
        return cid

    def get_cluster_id_by_name(self, cname):
        cl = self.get('/clusters/list')
        running = list(filter(lambda x: x['state'] == "RUNNING", cl['clusters']))
        for x in running:
            if cname == x['cluster_name']:
                return x['cluster_id']
        return None

    def launch_cluster(self):
        """ Launches a cluster to get DDL statements.
        Returns a cluster_id """
        version = self.get_latest_spark_version()
        import os
        real_path = os.path.dirname(os.path.realpath(__file__))
        if self.is_aws():
            with open(real_path+'/../data/aws_cluster.json', 'r') as fp:
                cluster_json = json.loads(fp.read())
        else:
            with open(real_path+'/../data/azure_cluster.json', 'r') as fp:
                cluster_json = json.loads(fp.read())
        # set the latest spark release regardless of defined cluster json
        cluster_json['spark_version'] = version['key']
        cluster_name = cluster_json['cluster_name']
        existing_cid = self.get_cluster_id_by_name(cluster_name)
        if existing_cid:
            return existing_cid
        else:
            c_info = self.post('/clusters/create', cluster_json)
            if c_info['http_status_code'] != 200:
                raise Exception("Could not launch cluster. Verify that the --azure flag or cluster config is correct.")
            self.wait_for_cluster(c_info['cluster_id'])
            return c_info['cluster_id']
    
    def edit_cluster(self, cid, iam_role):
        """Edits the existing metastore cluster
        Returns cluster_id"""
        version = self.get_latest_spark_version()
        json_payload = {
            "cluster_name": "API_Metastore_Work_Leave_Me_Alone",
            "cluster_id": cid, 
            "spark_version": version['key'],
            "num_workers": 1,
            "node_type_id": "m4.xlarge",
            "aws_attributes": {
                "instance_profile_arn": iam_role,
                "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                "ebs_volume_count": 1,
                "ebs_volume_size": 100
                }
            }
        self.post('/clusters/edit', json_payload)
        self.wait_for_cluster(cid)
        return cid

    def get_execution_context(self, cid):
        print("Creating remote Spark Session")
        time.sleep(5)
        ec_payload = {"language": "python",
                      "clusterId": cid}
        ec = self.post('/contexts/create', json_params=ec_payload, version="1.2")
        # Grab the execution context ID
        ec_id = ec.get('id', None)
        if not ec_id:
            print('Unable to establish remote session')
            print(ec)
            raise Exception("Remote session error")
        return ec_id

    def submit_command(self, cid, ec_id, cmd):
        # This launches spark commands and print the results. We can pull out the text results from the API
        command_payload = {'language': 'python',
                           'contextId': ec_id,
                           'clusterId': cid,
                           'command': cmd}
        command = self.post('/commands/execute',
                            json_params=command_payload,
                            version="1.2")

        com_id = command['id']
        # print('command_id : ' + com_id)
        result_payload = {'clusterId': cid, 'contextId': ec_id, 'commandId': com_id}

        resp = self.get('/commands/status', json_params=result_payload, version="1.2")
        is_running = resp['status']

        # loop through the status api to check for the 'running' state call and sleep 1 second
        while (is_running == "Running") or (is_running == 'Queued'):
            resp = self.get('/commands/status', json_params=result_payload, version="1.2")
            is_running = resp['status']
            time.sleep(1)
        end_results = resp['results']
        if end_results.get('resultType', None) == 'error':
            print("ERROR: ")
            print(end_results.get('summary', None))
        return end_results

    def log_all_databases(self, cid, ec_id, ms_dir):
        # submit first command to find number of databases
        all_dbs_cmd = 'all_dbs = [x.databaseName for x in spark.sql("show databases").collect()]; print(len(all_dbs))'
        results = self.submit_command(cid, ec_id, all_dbs_cmd)
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
                print("Database: {0}".format(db))
                os.makedirs(self._export_dir + ms_dir + db, exist_ok=True)
        return all_dbs

    def log_all_tables(self, db_name, cid, ec_id, ms_dir, err_log_path):
        all_tables_cmd = 'all_tables = [x.tableName for x in spark.sql("show tables in {0}").collect()]'.format(db_name)
        results = self.submit_command(cid, ec_id, all_tables_cmd)
        results = self.submit_command(cid, ec_id, 'print(len(all_tables))')
        num_of_tables = ast.literal_eval(results['data'])

        batch_size = 100    # batch size to iterate over databases
        num_of_buckets = (num_of_tables // batch_size) + 1     # number of slices of the list to take

        all_tables = []
        with open(err_log_path, 'a') as err_log:
            for m in range(0, num_of_buckets):
                tables_slice = 'print(all_tables[{0}:{1}])'.format(batch_size*m, batch_size*(m+1))
                results = self.submit_command(cid, ec_id, tables_slice)
                table_names = ast.literal_eval(results['data'])
                for table_name in table_names:
                    print("Table: {0}".format(table_name))
                    ddl_stmt = 'print(spark.sql("show create table {0}.{1}").collect()[0][0])'.format(db_name,
                                                                                                      table_name)
                    results = self.submit_command(cid, ec_id, ddl_stmt)
                    with open(self._export_dir + ms_dir + db_name + '/' + table_name, "w") as fp:
                        if results['resultType'] == 'text':
                            fp.write(results['data'])
                        else:
                            results['table'] = '{0}.{1}'.format(db_name, table_name)
                            err_log.write(json.dumps(results) + '\n')

    def export_hive_metastore(self, ms_dir='metastore/'):
        start = timer()
        cid = self.launch_cluster()
        end = timer()
        print("Cluster creation time: " + str(timedelta(seconds=end - start)))
        time.sleep(5)
        ec_id = self.get_execution_context(cid)
        # if metastore failed log path exists, cleanup before re-running
        failed_metastore_log_path = self._export_dir + 'failed_metastore.log'
        if os.path.exists(failed_metastore_log_path):
            os.remove(failed_metastore_log_path)
        all_dbs = self.log_all_databases(cid, ec_id, ms_dir)
        for db_name in all_dbs:
            self.log_all_tables(db_name, cid, ec_id, ms_dir, failed_metastore_log_path)

        instance_profile_log_path = self._export_dir + 'instance_profiles.log'

        with open(instance_profile_log_path, 'r') as iam_log:
            for role in iam_log:
                role_json = json.loads(role)
                iam_role = role_json["instance_profile_arn"]
                self.edit_cluster(cid, iam_role)
                ec_id = self.get_execution_context(cid)
                with open(failed_metastore_log_path, 'r') as err_log:
                    for table in err_log:
                        table_json = json.loads(table)
                        db_name = table_json['table'].split(".")[0]
                        table_name = table_json['table'].split(".")[1]
                        ddl_stmt = 'print(spark.sql("show create table {0}.{1}").collect()[0][0])'.format(db_name,table_name)
                        results = self.submit_command(cid, ec_id, ddl_stmt)
                        with open(self._export_dir + ms_dir + db_name + '/' + table_name, "w") as fp:
                            if results['resultType'] == 'text':
                                fp.write(results['data'])
                            else:
                                print('failed to get ddl for {0}.{1} with iam role {2}'.format(db_name, table_name, iam_role))
        print(all_dbs)

    def create_database_db(self, db_name, ec_id, cid):
        create_db_statement = 'spark.sql("CREATE DATABASE IF NOT EXISTS {0}")'.format(db_name.replace('\n', ''))
        db_results = self.submit_command(cid, ec_id, create_db_statement)
        return db_results

    @staticmethod
    def get_spark_ddl(table_ddl):
        spark_ddl = 'spark.sql(""" {0} """)'.format(table_ddl)
        return spark_ddl

    def apply_table_ddl(self, local_table_dir, ec_id, cid):
        with open(local_table_dir, "r") as fp:
            ddl_statement = fp.read()
            spark_ddl_statement = self.get_spark_ddl(ddl_statement)
            ddl_results = self.submit_command(cid, ec_id, spark_ddl_statement)
            return ddl_results

    def import_hive_metastore(self, ms_dir='metastore'):
        ms_local_dir = self._export_dir + ms_dir
        cid = self.launch_cluster()
        time.sleep(2)
        ec_id = self.get_execution_context(cid)
        # get local databases
        db_list = os.listdir(ms_local_dir)
        # iterate over the databases saved locally
        for db in db_list:
            # get the local database path to list tables
            local_db_path = ms_local_dir + '/' + db
            self.create_database_db(db, ec_id, cid)
            if os.path.isdir(local_db_path):
                # all databases should be directories, no files at this level
                # list all the tables in the database local dir
                tables = os.listdir(local_db_path)
                for x in tables:
                    # build the path for the table where the ddl is stored
                    print("Importing table {0}.{1}".format(db, x))
                    local_table_ddl = ms_local_dir + '/' + db + '/' + x
                    is_successful = self.apply_table_ddl(local_table_ddl, ec_id, cid)
                    print(is_successful)
            else:
                print("Error: Only databases should exist at this level: {0}".format(db))

