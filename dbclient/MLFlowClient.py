import os
import json
from datetime import timedelta
from timeit import default_timer as timer
import logging
import logging_utils
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.entities import ViewType
from mlflow.exceptions import RestException
import wmconstants
from thread_safe_writer import ThreadSafeWriter
import concurrent
from concurrent.futures import ThreadPoolExecutor

class MLFlowClient:
    def __init__(self, configs, checkpoint_service):
        self._checkpoint_service = checkpoint_service
        self.export_dir = configs['export_dir']
        self.client = MlflowClient(f"databricks://{configs['profile']}")

    def export_mlflow_experiments(self, log_file='mlflow_experiments.log', log_dir=None):
        mlflow_experiments_dir = log_dir if log_dir else self.export_dir
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.MLFLOW_EXPERIMENT_OBJECT, self.export_dir
        )
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
        if artifact_location == None or \
                artifact_location.startswith("dbfs:/databricks/mlflow-tracking") or \
                artifact_location.startswith("dbfs:/databricks/mlflow"):
            return None
        return artifact_location
