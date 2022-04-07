import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from timeit import default_timer as timer
from datetime import timedelta
import functools
from typing import List, Optional
import logging_utils
import os

from .task import AbstractTask


class Pipeline:
    """Class that coordinates run of a group of tasks, which form a DAG based on dependencies
    defined in add_task().

    See pipeline_test.py for examples.

    TBD: The pipeline has built-in checkpoint i.e it will skip all complete tasks upon restart."""

    @dataclass
    class Node:
        """Node within a pipeline.

        Attributes:
        - task: The task runs in the node.
        - children: The nodes that should run after the current node completes.

        DON'T create Node instance in any way other than calling add_task.
        """
        task: AbstractTask = None
        children = []

    def __init__(self, working_dir: str, completed_pipeline_steps, dry_run: bool = False):
        """
        :param working_dir: the dir where the pipeline reads / writes checkpoints and outputs logs.
        :param completed_pipeline_steps: CheckpointKeySet of completed pipeline tasks
        """
        self._source = self.Node()
        self._working_dir = working_dir
        self._completed_steps = completed_pipeline_steps
        self._tasks = []
        self._dry_run = dry_run

    def add_task(self, task: AbstractTask, parents: Optional[List[Node]] = None, skip=False) -> Node:
        node = self.Node(task)
        if not parents:
            parents = [self._source]
        for parent in parents:
            parent.children.append(node)

        # Short-term hack: just execute all tasks sequentially in the order of add_task. It's not
        # optimal but the behavior is correct because child will only execute after all parents
        # complete.
        self._tasks.append(task)
        return node

    def run(self):
        """The current implementation runs task sequentially in a thread pool."""
        with ThreadPoolExecutor() as executor:
            for task in self._tasks:
                future = executor.submit(functools.partial(self._run_task, task))
                future.result()

    def _run_task(self, task: AbstractTask):
        if self._completed_steps.contains(f'{task.name}'):
            logging.info(f'Task {task.name} already completed, found in checkpoint')
            return
        start = timer()
        logging.info(f'Start {task.name}')
        if not self._dry_run and not task.skip:
            task.run()
            end = timer()
            logging.info(f'{task.name} Completed. Total time taken: {str(timedelta(seconds=end - start))}')
            failed_task_log = logging_utils.get_error_log_file(task.action_type, task.object_type, self._working_dir)
            if os.path.exists(failed_task_log) and os.path.getsize(failed_task_log) > 0:
                msg = f'{task.name} has failures. Refer to {failed_task_log} to see failures. Use --skip-tasks option to skip this step and continue the pipeline. (available tasks are in wmconstants.py in TASK_OBJECTS Terminating pipeline.'
                logging.info(msg)
                raise RuntimeError(msg)
            self._completed_steps.write(f'{task.name}')
        else:
            logging.info(f'{task.name} Skipped.')

