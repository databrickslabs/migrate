from dataclasses import dataclass
from datetime import datetime

from pipeline import Pipeline


@dataclass
class PipelineConfig:
    """Configuration of the workspace migration pipeline.

    Attributes:
    - user: who runs the scripts.
    - workspace: source workspace to be migrated.
    - base_dir: local dir to perform workspace migration.
    - session: if set, it will try to resume the pipeline based on existing checkpoint; otherwise,
    the pipeline will run from beginning and a new session will be generated.
    """
    user: str
    workspace: str
    base_dir: str
    session: str = ''


def generate_session() -> str:
    return datetime.now().strftime('%Y%m%d%H%M%S')


def build_pipeline(config: PipelineConfig) -> Pipeline:
    """Build the pipeline based on the config."""
    config.session = config.session if config.session else generate_session()
    working_dir = f'{config.base_dir}/{config.user}/{config.workspace}/{config.session}'
    pipeline = Pipeline(working_dir)

    # Example:
    #
    # task1 = ...
    # task2 = ...
    # task3 = ...
    # task4 = ...
    # node1 = pipeline.add_task(task1)
    # node2 = pipeline.add_task(task2, [node1])
    # node3 = pipeline.add_task(task3, [node1])
    # node4 = pipeline.add_task(task4, [node2, node3])
    #
    # Execution order: task1 -> task2 & task3 -> task4.
    #
    return pipeline
