import os
import json
from datetime import timedelta
from timeit import default_timer as timer
import logging
import logging_utils
from threading_utils import propagate_exceptions
import shutil
from mlflow.tracking import MlflowClient
from mlflow.entities import ViewType, Metric, Param, RunTag
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
                    concurrent.futures.wait(futures)
                    propagate_exceptions(futures)
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
            # If the resource already exists, then we can consider it successful and checkpoint it.
            if error.json['error_code'] == 'RESOURCE_ALREADY_EXISTS':
                logging.info(error.json['message'] + f" Trying to get the experiment_id of the existing experiment: {name}...")
                try:
                    new_id = self.client.get_experiment_by_name(name).experiment_id
                    logging.info(f"Successfully retrieved an id: {new_id} for experiment: {name} in the target workspace.")
                    # save id -> new_id
                    id_map_writer.write(json.dumps({"old_id": id, "new_id": new_id}) + "\n")
                    checkpointer.write(id)
                except RestException as error:
                    logging.info(f"create experiment failed for id: {id}, name: {name}. Logging it to error file..")
                    error_logger.error(error.json)
            else:
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

    def import_mlflow_runs(self, src_client_config, log_sql_file='mlflow_runs.db', experiment_id_map_log='mlflow_experiments_id_map.log', run_id_map_log='mlflow_runs_id_map.log', ml_run_artifacts_dir='ml_run_artifacts/', num_parallel=4):
        """
        Imports the Mlflow run objects. This can be run only after import_mlflow_experiments is complete.
        Input files are mlflow_runs.db, mlflow_experiments_id_map.log
        Outputs mlflow_runs_id_map.log which has the map of old_run_id -> new_run_id after imports.
        """
        src_client = MlflowClient(f"databricks://{src_client_config['profile']}")
        experiment_id_map = self._load_experiment_id_map(self.export_dir + experiment_id_map_log)
        mlflow_runs_file = self.export_dir + log_sql_file
        os.makedirs(self.export_dir + ml_run_artifacts_dir, exist_ok=True)

        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.MLFLOW_RUN_OBJECT, self.export_dir
        )

        # checkpoint is required since the checkpoint file is copied into mlflow_runs_id_map.log at the end of the step.
        assert self._checkpoint_service.checkpoint_enabled, "import_mlflow_runs requires --use-checkpoint to be enabled. If " \
                                                            " you need to actually rerun, remove the corresponding " \
                                                            "checkpoint file. e.g. logs/checkpoint/import_mlflow_runs.log"

        mlflow_runs_checkpointer = self._checkpoint_service.get_checkpoint_key_map(
            wmconstants.WM_IMPORT, wmconstants.MLFLOW_RUN_OBJECT)

        start = timer()

        con = sqlite3.connect(mlflow_runs_file)
        cur = con.execute("SELECT * FROM runs")
        # TODO(kevin): make this configurable later
        runs = cur.fetchmany(10000)
        while(len(runs) > 0):
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                # run_id = run[0]
                # start_time = run[1]
                # run_obj = json.loads(run[2])
                futures = [executor.submit(self._create_run_and_log, src_client, mlflow_runs_file, run[0], run[1], json.loads(run[2]), experiment_id_map, self.export_dir + ml_run_artifacts_dir, error_logger, mlflow_runs_checkpointer) for run in runs]
                concurrent.futures.wait(futures)
                propagate_exceptions(futures)

            runs = cur.fetchmany(10000)
        shutil.copy(mlflow_runs_checkpointer.get_file_path(), self.export_dir + run_id_map_log)
        con.close()
        end = timer()
        logging.info("Complete MLflow Runs Import Time: " + str(timedelta(end - start)))

    def _create_run_and_log(self, src_client, mlflow_runs_file, run_id, start_time, run_obj, experiment_id_map, ml_run_artifacts_dir, error_logger, checkpointer):
        """
        If "mlflow.parentRunId" does not exist in tags, create the run and then log metrics, params, and tags
        If exists in tags, recursively call _create_run on the parent run object.
        :return: id of the newly imported run
        """
        if checkpointer.check_contains_otherwise_mark_in_use(run_id):
            return checkpointer.get(run_id)
        experiment_id = run_obj['info']['experiment_id']
        if experiment_id not in experiment_id_map:
            message = (f"Run: {run_id} originally belongs to experiment_id {experiment_id}, but {experiment_id} "
                       "does not exist in mlflow_experiments_id_map.log. Make sure the experiment is correctly "
                       f"imported before importing runs.")
            error_logger.error(message)
            return None

        imported_experiment_id = experiment_id_map[run_obj['info']['experiment_id']]
        tags = run_obj['tags']
        metrics = run_obj['metrics']
        params = run_obj['params']
        if "mlflow.parentRunId" in tags:
            parent_run_id = tags["mlflow.parentRunId"]
            con = sqlite3.connect(mlflow_runs_file)
            cur = con.execute("SELECT * FROM runs WHERE id=?", [parent_run_id])
            parent_run = cur.fetchone()
            con.close()

            parent_run_id = parent_run[0]
            parent_start_time = parent_run[1]
            parent_run_obj = json.loads(parent_run[2])

            new_parent_run_id = self._create_run_and_log(src_client, mlflow_runs_file, parent_run_id, parent_start_time, parent_run_obj, experiment_id_map, ml_run_artifacts_dir, error_logger, checkpointer)
            if not new_parent_run_id:
                message = (f"Run: {run_id} failed to be imported as its parent run failed to be imported.")
                error_logger.error(message)
                return None
            tags["mlflow.parentRunId"] = new_parent_run_id

        new_run_id = self._create_run_and_log_helper(src_client, imported_experiment_id, run_id, start_time, metrics, params, tags, ml_run_artifacts_dir)
        logging.info(f"Successfully imported run: {run_id} into target workspace as {new_run_id}")
        checkpointer.write(run_id, new_run_id)
        return new_run_id

    def _create_run_and_log_helper(self, src_client, experiment_id, run_id, start_time, metrics, params, tags, ml_run_artifacts_dir):
        run = self.client.create_run(experiment_id, start_time=start_time, tags={})
        new_run_id = run.info.run_id
        metrics_obj = [Metric(key, val, start_time, step=0) for key, val in metrics.items()]
        params_obj = [Param(key, val) for key, val in params.items()]

        # We filter out the following tags for the imported runs.
        DENY_LIST_TAGS = [
            "mlflow.databricks.notebookID", "mlflow.databricks.notebookRevisionID", "mlflow.databricks.webappURL",
            "mlflow.databricks.runURL", "mlflow.databricks.cluster.id", "mlflow.databricks.workspaceURL",
            "mlflow.databricks.workspaceID", "mlflow.databricks.notebook.commandID", "mlflow.databricks.shellJobID",
            "mlflow.databricks.shellJobRunID", "mlflow.databricks.jobID", "mlflow.databricks.jobRunID",
            "mlflow.databricks.jobType", "mlflow.databricks.jobTypeInfo",
            "mlflow.log-model.history", "mlflow.user", "mlflow.rootRunId"
        ]
        tags_obj = [RunTag(key, val) for key, val in tags.items() if key not in DENY_LIST_TAGS]

        self.client.log_batch(new_run_id, metrics_obj, params_obj, tags_obj)
        self.download_and_upload_run_artifacts(src_client, run_id, new_run_id, ml_run_artifacts_dir)

        return run_id

    def download_and_upload_run_artifacts(self, src_client, old_run_id, new_run_id, ml_run_artifacts_dir):
        # Download artifacts into temp directory: ml_run_artifacts/$run_id_temp and then upload to the destination
        # workspace. After uploading, remove the temp dir along with the artifacts.
        temp_artifact_dir = ml_run_artifacts_dir + old_run_id + "_temp/"
        shutil.rmtree(temp_artifact_dir, ignore_errors=True)
        os.makedirs(temp_artifact_dir)
        artifacts = src_client.list_artifacts(old_run_id)
        if len(artifacts) == 0:
            return

        logging.info(f"Downloading run artifacts for run_id: {old_run_id}")
        src_client.download_artifacts(old_run_id, "", temp_artifact_dir)

        logging.info(f"Uploading run artifacts for run_id: {old_run_id} -> {new_run_id}")
        self.client.log_artifacts(new_run_id, temp_artifact_dir)
        shutil.rmtree(temp_artifact_dir)

    def _load_experiment_id_map(self, experiment_id_map_log):
        id_map = {}
        # Parallelize this operation if this is too slow.
        with open(experiment_id_map_log, 'r') as fp:
            # str = {"old_id": "xxxxxxxxx", "new_id": "xxxxxxxx"}
            for single_id_map_str in fp:
                single_id_map = json.loads(single_id_map_str)
                id_map[single_id_map["old_id"]] = single_id_map["new_id"]
        return id_map

