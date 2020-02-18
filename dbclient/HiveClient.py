import time, json, os, ast
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
            time.sleep(2)
        return cid

    def launch_cluster(self):
        """ Launches a cluster to get DDL statements.
        Returns a cluster_id """
        version = self.get_latest_spark_version()
        if self.is_aws():
            with open('data/aws_cluster.json', 'r') as fp:
                cluster_json = json.loads(fp.read())
        else:
            with open('data/azure_cluster.json', 'r') as fp:
                cluster_json = json.loads(fp.read())
        # set the latest spark release regardless of defined cluster json
        cluster_json['spark_version'] = version['key']
        c_info = self.post('/clusters/create', cluster_json)
        if c_info['http_status_code'] != 200:
            raise Exception("Could not launch cluster. Verify that the --azure flag or cluster config is correct.")
        self.wait_for_cluster(c_info['cluster_id'])
        return c_info['cluster_id']

    def get_execution_context(self, cid):
        print("Creating remote Spark Session")
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
        command = self.post('/commands/execute', \
                            json_params=command_payload, \
                            version="1.2")

        com_id = command['id']
        # print('command_id : ' + com_id)
        result_payload = {'clusterId': cid, 'contextId': ec_id, 'commandId': com_id}

        resp = self.get('/commands/status', json_params=result_payload, version="1.2")
        is_running = resp['status']

        # loop through the status api to check for the 'running' state call and sleep 1 second
        while (is_running == "Running" or is_running == 'Queued'):
            resp = self.get('/commands/status', json_params=result_payload, version="1.2")
            is_running = resp['status']
            time.sleep(1)
        return resp['results']

    def log_all_databases(self, cid, ec_id, ms_dir):
        results = self.submit_command(cid, ec_id,
                                      'print([x.databaseName for x in spark.sql("show databases").collect()])')
        all_dbs = ast.literal_eval(results['data'])
        for db in all_dbs:
            print("Database: {0}".format(db))
            os.makedirs(self._export_dir + ms_dir + db, exist_ok=True)
        return all_dbs

    def log_all_tables(self, db_name, cid, ec_id, ms_dir):
        results = self.submit_command(cid, ec_id,
                                      'print([x.tableName for x in spark.sql("show tables in {0}").collect()])'.format(
                                          db_name))
        all_tables = ast.literal_eval(results['data'])
        with open(self._export_dir + 'failed_metastore.log', 'a') as err_log:
            for table_name in all_tables:
                print("Table: {0}".format(table_name))
                ddl_stmt = 'print(spark.sql("show create table {0}.{1}").collect()[0][0])'.format(db_name, table_name)
                results = self.submit_command(cid, ec_id, ddl_stmt)
                with open(self._export_dir + ms_dir + db_name + '/' + table_name, "w") as fp:
                    if results['resultType'] == 'text':
                        fp.write(results['data'])
                    else:
                        err_log.write(json.dumps(results) + '\n')

    def export_hive_metastore(self, ms_dir='metastore/'):
        cid = self.launch_cluster()
        time.sleep(2)
        ec_id = self.get_execution_context(cid)
        all_dbs = self.log_all_databases(cid, ec_id, ms_dir)
        for db_name in all_dbs:
            self.log_all_tables(db_name, cid, ec_id, ms_dir)
        print(all_dbs)

    def create_database_db(self, db_name, ec_id, cid):
        create_db_statement = 'spark.sql("CREATE DATABASE IF NOT EXISTS {0}")'.format(db_name.replace('\n', ''))
        db_results = self.submit_command(cid, ec_id, create_db_statement)
        return db_results

    def apply_table_ddl(self, local_table_dir, ec_id, cid):
        with open(local_table_dir, "r") as fp:
            ddl_statement = fp.read()
            ddl_results = self.submit_command(cid, ec_id, ddl_statement)
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
                    local_table_ddl = ms_local_dir + '/' + db + '/' + x
                    is_successful = self.apply_table_ddl(local_table_ddl, ec_id, cid)
            else:
                print("Error: Only databases should exist at this level: {0}".format(db))

