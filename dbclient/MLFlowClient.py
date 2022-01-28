import os
import json
from datetime import timedelta
from timeit import default_timer as timer
import logging
import logging_utils
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.entities import ViewType
import wmconstants

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
