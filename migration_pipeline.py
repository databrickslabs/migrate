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
    # TODO: Add tasks to the pipeline.

    return pipeline
