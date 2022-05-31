import unittest
import os
import sqlite3
from dbclient import MLFlowClient
from dbclient.test.TestUtils import TEST_CONFIG
from checkpoint_service import CheckpointService, CheckpointKeyMap
import json
import concurrent.futures
from unittest.mock import MagicMock
from mlflow.entities import Metric, Param, RunTag, RunData, RunInfo, Run

MLFLOW_TEST_FILE = "dbclient/test/mlflow_runs_test.db"

class MLFlowClientTest(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(MLFLOW_TEST_FILE):
            os.remove(MLFLOW_TEST_FILE)

    def _generate_run(self, i, runs_dict):
        """
        Generate a run object and save to runs_dict keyed by run_id.
        Most of data just depends on i, and some data are hard-coded for simplicityGenerate n number of runs. Most of
        data just depends on n, and some data are hard-coded for simplicity.
        """
        key = f"key{i}"
        value = f"value{i}"
        start_time = 123456 * i
        end_time = start_time + (1000 * i)
        run_id = f"run_id_{i}"

        metrics = [Metric(key, value, start_time, "stage")]
        params = [Param(key, value)]
        tags = [RunTag(key, value)]
        run_info = RunInfo(run_id, "experiment_id", "user_id", "status", start_time, end_time, "lifecycle_stage")
        run = Run(
                    run_info=run_info,
                    run_data=RunData(
                        metrics=metrics,
                        params=params,
                        tags=tags))
        runs_dict[run_id] = run
        return run

    def _run_to_dict(self, run):
        info = run.info
        data = run.data
        metrics = data.metrics
        params = data.params
        tags = data.tags
        return {
            "info": {
                "experiment_id": info.experiment_id
            },
            "tags": tags,
            "metrics": metrics,
            "params": params
        }

    def _insert_run_data(self, run):
        con = sqlite3.connect(MLFLOW_TEST_FILE, timeout=30)
        with con:
            MLFlowClient._save_run_data_to_sql(con, run)


    def test_save_run_data_to_sql(self):
        # Input data
        run = self._generate_run(1, {})

        con = sqlite3.connect(MLFLOW_TEST_FILE, timeout=10)
        with con:
            con.execute('''
              DROP TABLE IF EXISTS runs
            ''')
            con.execute('''
              CREATE TABLE runs (id TEXT UNIQUE, start_time INT, run_obj TEXT)
            ''')
            self._insert_run_data(run)
            cur = con.execute('''
              SELECT * FROM runs
            ''')
        fetched_run = cur.fetchone()
        con.close()

        fetched_run_id = fetched_run[0]
        fetched_start_time = fetched_run[1]
        fetched_run_obj = json.loads(fetched_run[2])

        fetched_run_info = fetched_run_obj['info']
        fetched_run_metrics = fetched_run_obj['metrics']
        fetched_run_params = fetched_run_obj['params']
        fetched_run_tags = fetched_run_obj['tags']

        assert(run.info.run_id == fetched_run_id)
        assert(run.info.start_time == fetched_start_time)
        assert(dict(run.info) == fetched_run_info)
        assert(dict(run.data.metrics) == fetched_run_metrics)
        assert(dict(run.data.params) == fetched_run_params)
        assert(dict(run.data.tags) == fetched_run_tags)

    def test_save_run_data_to_sql_multiple_times(self):
        # Input data
        run = self._generate_run(1, {})

        for _ in range(10):
            con = sqlite3.connect(MLFLOW_TEST_FILE, timeout=10)
            with con:
                con.execute('''
                  DROP TABLE IF EXISTS runs
                ''')
                con.execute('''
                  CREATE TABLE runs (id TEXT UNIQUE, start_time INT, run_obj TEXT)
                ''')
                self._insert_run_data(run)
                cur = con.execute('''
                  SELECT * FROM runs
                ''')
            con.close()

        con = sqlite3.connect(MLFLOW_TEST_FILE, timeout=10)
        with con:
            self._insert_run_data(run)
            cur = con.execute('''
              SELECT * FROM runs
            ''')
        fetched_run = cur.fetchone()
        con.close()

        fetched_run_id = fetched_run[0]
        fetched_start_time = fetched_run[1]
        fetched_run_obj = json.loads(fetched_run[2])

        fetched_run_info = fetched_run_obj['info']
        fetched_run_metrics = fetched_run_obj['metrics']
        fetched_run_params = fetched_run_obj['params']
        fetched_run_tags = fetched_run_obj['tags']

        assert(run.info.run_id == fetched_run_id)
        assert(run.info.start_time == fetched_start_time)
        assert(dict(run.info) == fetched_run_info)
        assert(dict(run.data.metrics) == fetched_run_metrics)
        assert(dict(run.data.params) == fetched_run_params)
        assert(dict(run.data.tags) == fetched_run_tags)

    def test_parallel_save_run_data_to_sql(self):
        con = sqlite3.connect(MLFLOW_TEST_FILE, timeout=10)
        with con:
            con.execute('''
              DROP TABLE IF EXISTS runs
            ''')
            con.execute('''
              CREATE TABLE runs (id TEXT UNIQUE, start_time INT, run_obj TEXT)
            ''')

        runs_dict = {}
        num_runs = 2000
        runs = [self._generate_run(i, runs_dict) for i in range(1, 1 + num_runs)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(self._insert_run_data(run)) for run in runs]
            concurrent.futures.wait(futures, return_when="FIRST_EXCEPTION")

        with con:
            cur = con.execute('''
              SELECT * FROM runs
            ''')
        fetched_runs = cur.fetchmany(num_runs)
        con.close()
        assert(len(fetched_runs) == num_runs)

        for fetched_run in fetched_runs:
            fetched_run_id = fetched_run[0]
            fetched_start_time = fetched_run[1]
            fetched_run_obj = json.loads(fetched_run[2])

            fetched_run_info = fetched_run_obj['info']
            fetched_run_metrics = fetched_run_obj['metrics']
            fetched_run_params = fetched_run_obj['params']
            fetched_run_tags = fetched_run_obj['tags']

            id = fetched_run_id
            assert(runs_dict[id].info.run_id == fetched_run_id)
            assert(runs_dict[id].info.start_time == fetched_start_time)
            assert(dict(runs_dict[id].info) == fetched_run_info)
            assert(dict(runs_dict[id].data.metrics) == fetched_run_metrics)
            assert(dict(runs_dict[id].data.params) == fetched_run_params)
            assert(dict(runs_dict[id].data.tags) == fetched_run_tags)

    def test_create_run_and_log_thread_safety(self):
        checkpoint_service = MagicMock()
        mlflow_client = MLFlowClient(TEST_CONFIG, checkpoint_service)

        runs_dict = {}
        unique_num_runs = 100
        # Create duplicate run_id runs
        runs = [self._generate_run(i, runs_dict) for i in range(1, 1 + unique_num_runs)]
        runs += [self._generate_run(i, runs_dict) for i in range(1, 1 + unique_num_runs - 50)]
        runs += [self._generate_run(i, runs_dict) for i in range(1, 1 + unique_num_runs)]
        runs += [self._generate_run(i, runs_dict) for i in range(1, 1 + unique_num_runs - 25)]

        # sort it by the run_id, so that threads have higher chance to access the same run_id during testing.
        runs = sorted(runs, key=lambda run: run.info.run_id)
        experiment_id_map = {
            "experiment_id": "experiment_id_new"
        }
        error_logger = None
        checkpointer = CheckpointKeyMap("dbclient/test/test_ml_run_import_checkpoint_temp.log")
        mlflow_client._create_run_and_log_helper = MagicMock()
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            futures = [executor.submit(mlflow_client._create_run_and_log(mlflow_client, "", run.info.run_id, run.info.start_time, self._run_to_dict(run), experiment_id_map, "", error_logger, checkpointer, checkpointer)) for run in runs]
            concurrent.futures.wait(futures)

        assert(mlflow_client._create_run_and_log_helper.call_count == unique_num_runs)

        os.remove("dbclient/test/test_ml_run_import_checkpoint_temp.log")

    # TODO(kevin): Add more unit tests later
    def test_export_mlflow_experiment_acls_skip(self):
        checkpoint_service = MagicMock()
        checkpoint_key_set = MagicMock()
        checkpoint_key_set.contains = MagicMock(return_value=False)
        mlflow_client = MLFlowClient(TEST_CONFIG, checkpoint_service)
        mlflow_client.get = MagicMock()
        experiment_str = json.dumps({
            "experiment_id": "experiment_id_test",
            "tags": {
                "mlflow.experimentType": "NOTEBOOK"
            }
        })
        mlflow_client._get_mlflow_experiment_acls(None, experiment_str, checkpoint_key_set, None)
        # because experimentType != MLFLOW_EXPERIMENT, it doesn't export permissions
        assert(mlflow_client.get.call_count == 0)

    def test_export_mlflow_experiment_acls(self):
        checkpoint_service = MagicMock()
        checkpoint_key_set = MagicMock()
        checkpoint_key_set.contains = MagicMock(return_value=False)
        mlflow_client = MLFlowClient(TEST_CONFIG, checkpoint_service)
        experiment_id = "experiment_id_test_2"
        experiment_str2 = json.dumps({
            "experiment_id": experiment_id,
            "tags": {
                "mlflow.experimentType": "MLFLOW_EXPERIMENT"
            }
        })
        perm_response = {
            "object_id": experiment_id,
            "object_type": "mlflowExperiment",
            "http_status_code": 200
        }
        mlflow_client.get = MagicMock(return_value=perm_response)
        error_logger = MagicMock()
        error_logger.error = MagicMock()
        acl_log_file_writer = MagicMock()
        acl_log_file_writer.write = MagicMock()
        checkpoint_key_set.write = MagicMock()
        mlflow_client._get_mlflow_experiment_acls(acl_log_file_writer, experiment_str2, checkpoint_key_set, error_logger)
        acl_log_file_writer.write.assert_called_with(json.dumps(perm_response) + '\n')
        checkpoint_key_set.write.assert_called_with(experiment_id)

    def test_put_mlflow_experiment_acl(self):
        checkpoint_service = MagicMock()
        checkpoint_key_set = MagicMock()
        checkpoint_key_set.contains = MagicMock(return_value=False)
        checkpoint_key_set.write = MagicMock()
        mlflow_client = MLFlowClient(TEST_CONFIG, checkpoint_service)
        error_logger = MagicMock()
        error_logger.error = MagicMock()

        experiment_id = "experiment_id_for_test"
        new_experiment_id = "new_experiment_id_for_test"
        acl_str = json.dumps({
            "object_id": f"/experiments/{experiment_id}",
            "access_control_list": [
                {
                    "user_name": "example@email.com",
                    "all_permissions": [
                        {
                            "permission_level": "CAN_EDIT",
                            "inherited": "false"
                        }
                    ]
                }
            ]
        })
        experiment_id_map = {
            experiment_id: new_experiment_id
        }
        perm_response = {
            "http_status_code": 200
        }
        acl_args = [
            {
                'user_name': 'example@email.com',
                'permission_level': 'CAN_EDIT'
            }
        ]
        mlflow_client.put = MagicMock(return_value=perm_response)
        mlflow_client.build_acl_args = MagicMock(return_value=acl_args)

        mlflow_client._put_mlflow_experiment_acl(acl_str, experiment_id_map, checkpoint_key_set, error_logger)

        mlflow_client.put.assert_called_with(f'/permissions/experiments/{new_experiment_id}', {'access_control_list': acl_args})
        checkpoint_key_set.write.assert_called_with(experiment_id)
