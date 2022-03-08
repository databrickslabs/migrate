import unittest
import os
import sqlite3
from dbclient import MLFlowClient
import json
import concurrent.futures
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
