from dataclasses import dataclass
from pipeline import Pipeline


@dataclass
class PipelineConfig:
    """Configuration of the workspace migration pipeline."""
    user: str
    workspace: str
    base_dir: str
    session: str = ''


def generate_session() -> str:
    return 'unique_session'


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
