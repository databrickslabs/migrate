import os
import json
from datetime import timedelta
from timeit import default_timer as timer
import logging
import logging_utils
from mlflow.tracking import MlflowClient
from mlflow.entities import ViewType
from mlflow.exceptions import RestException
import wmconstants
from thread_safe_writer import ThreadSafeWriter
import concurrent
from concurrent.futures import ThreadPoolExecutor
import sqlite3

class MLFlowClient:
    def __init__(self, configs, checkpoint_service):
        self._checkpoint_service = checkpoint_service
        self.export_dir = configs['export_dir']
        self.client = MlflowClient(f"databricks://{configs['profile']}")

    def export_mlflow_runs(self, log_sql_file='mlflow_runs.db', experiment_log='mlflow_experiments.log', num_parallel=4):
        """
        Exports the Mlflow run objects. This can be run only after export_mlflow_experiments is complete.
        Unlike other objects, we save the data into sqlite tables, given the possible scale of runs objects.
        """
        experiments_logfile = self.export_dir + experiment_log
        mlflow_runs_checkpointer = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.MLFLOW_RUNS
        )

        start = timer()
        con = sqlite3.connect(self.export_dir + log_sql_file)
        with con:
            con.execute('''
              CREATE TABLE IF NOT EXISTS runs (id TEXT UNIQUE, start_time INT, run_obj TEXT)
            ''')
        con.close()

        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.MLFLOW_RUN_OBJECT, self.export_dir
        )
        with open(experiments_logfile, 'r') as fp:
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = [executor.submit(self._export_runs_in_an_experiment, log_sql_file, experiment_str, mlflow_runs_checkpointer, error_logger) for experiment_str in fp]
                results = concurrent.futures.wait(futures, return_when="FIRST_EXCEPTION")
                for result in results.done:
                    if result.exception() is not None:
                        raise result.exception()

        end = timer()
        logging.info("Complete MLflow Runs Export Time: " + str(timedelta(seconds=end - start)))


    '''
    # Later we may consume the exported runs as the following
    def import_mlflow_runs(...):
        con = sqlite3.connect(log_sql_file, check_same_thread=False)
        cur = con.cursor()

        # Later, we might enforce start_time > xxx
        cur.execute("SELECT * FROM runs")
        runs = cur.fetchmany(1000)
        while(len(runs) > 0):
            # parallelize here
            for run in runs:
                run_id = run[0]
                start_time = run[1]

                run_obj = json.loads(run[2])
                info = run_object['info']
                metrics = run_object['metrics']
                params = run_object['params']
                tags = run_object['tags']
    '''


    def _export_runs_in_an_experiment(self, log_sql_file, experiment_str, checkpointer, error_logger):
        experiment_id = json.loads(experiment_str).get('experiment_id')
        logging.info("Working on runs for experiment_id: " + experiment_id)
        # We checkpoint by experiment_id
        if checkpointer.contains(experiment_id):
            return
        page_continue = True
        token = None
        is_there_exception = False
        # Unlike experiments which usually don't have too much number of data,
        # we must do page_token handling for runs.
        while page_continue:
            try:
                runs = self.client.search_runs(experiment_id, run_view_type=ViewType.ACTIVE_ONLY, max_results=3000, page_token=token)
            except RestException as error:
                logging.info(f"search runs failed for id: {experiment_id}. Logging it to error file...")
                error_logger.error(error.json)
                is_there_exception = True
            else:
                # With multi-threading, we need to give enough timeout for each thread's connection to be made
                # Upon DatabaseLock timeout exception, one can simply rerun the command since the checkpoint saves the
                # progress
                con = sqlite3.connect(self.export_dir + log_sql_file, timeout=180)
                with con:
                    for run in runs:
                        self._save_run_data_to_sql(con, run)
                con.close()
                token = runs.token
                page_continue = token is not None

        if not is_there_exception:
            checkpointer.write(experiment_id)

    @classmethod
    def _save_run_data_to_sql(cls, con, run):
        run_id = run.info.run_id
        start_time = run.info.start_time
        run_object = {
            "info": dict(run.info),
            "metrics": dict(run.data.metrics),
            "params": dict(run.data.params),
            "tags": dict(run.data.tags)
        }
        # qmark style to avoid sql injection
        con.execute("INSERT OR REPLACE INTO runs VALUES (?, ?, ?)", (run_id, start_time, json.dumps(run_object)))


    def export_mlflow_experiments(self, log_file='mlflow_experiments.log', log_dir=None):
        mlflow_experiments_dir = log_dir if log_dir else self.export_dir
        os.makedirs(mlflow_experiments_dir, exist_ok=True)
        start = timer()
        # We do not do any pagination since ST workspaces do not have that many experiments count.
        # Max is ~6k experiments for the moment.
        # Consider using pagination(https://www.mlflow.org/docs/latest/python_api/mlflow.tracking.html) if
        # a workspace has explosive number of experiments. (e.g. 200K)
        experiments = self.client.list_experiments(view_type=ViewType.ALL)
        experiments_logfile = mlflow_experiments_dir + log_file
        with open(experiments_logfile, 'w') as fp:
            for experiment in experiments:
               fp.write(json.dumps(dict(experiment)) + '\n')
        end = timer()
        logging.info("Complete MLflow Experiments Export Time: " + str(timedelta(seconds=end - start)))

    def import_mlflow_experiments(self, log_file='mlflow_experiments.log', id_map_file='mlflow_experiments_id_map.log',
                                  log_dir=None, num_parallel=4):
        mlflow_experiments_dir = log_dir if log_dir else self.export_dir
        experiments_logfile = mlflow_experiments_dir + log_file
        experiments_id_map_file = mlflow_experiments_dir + id_map_file

        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.MLFLOW_EXPERIMENT_OBJECT, self.export_dir
        )
        mlflow_experiments_checkpointer = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_IMPORT, wmconstants.MLFLOW_EXPERIMENT_OBJECT)
        start = timer()

        id_map_thread_safe_writer = ThreadSafeWriter(experiments_id_map_file, 'a')

        try:
            with open(experiments_logfile, 'r') as fp:
                with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                    futures = [executor.submit(self._create_experiment, experiment_str, id_map_thread_safe_writer, mlflow_experiments_checkpointer, error_logger) for experiment_str in fp]
                    results = concurrent.futures.wait(futures, return_when="FIRST_EXCEPTION")
                    for result in results.done:
                        if result.exception() is not None:
                            raise result.exception()
        finally:
            id_map_thread_safe_writer.close()

        end = timer()
        logging.info("Complete MLflow Experiments Import Time: " + str(timedelta(seconds=end - start)))

    def _create_experiment(self, experiment_str, id_map_writer, checkpointer, error_logger):
        experiment = json.loads(experiment_str)
        id = experiment.get('experiment_id')
        if checkpointer.contains(id):
            return
        artifact_location = self._cleanse_artifact_location(experiment.get('artifact_location', None))
        name = experiment.get('name')
        tags = experiment.get('tags', None)
        dict_tags = dict(tags) if tags else None
        try:
            new_id = self.client.create_experiment(name, artifact_location, dict_tags)
            logging.info(f"Successfully created experiment with name: {name}. id: {new_id}")
        except RestException as error:
            logging.info(f"create experiment failed for id: {id}, name: {name}. Logging it to error file..")
            error_logger.error(error.json)
        else:
            # save id -> new_id
            id_map_writer.write(json.dumps({"old_id": id, "new_id": new_id}) + "\n")

            # checkpoint the original id
            checkpointer.write(id)

    def _cleanse_artifact_location(self, artifact_location):
        """
        There are some paths that are not allowed to be artifact_location. In those cases, we should use None as the
        artifact_location when creating experiment objects.
        """
        if artifact_location is None or \
                artifact_location.startswith("dbfs:/databricks/mlflow-tracking/") or \
                artifact_location.startswith("dbfs:/databricks/mlflow/"):
            return None
        return artifact_location
