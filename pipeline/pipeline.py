from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Optional

from .task import AbstractTask


class Pipeline:
    """Class that coordinates run of a group of tasks, which form a DAG based on dependencies
    defined in add_task().

    TBD: The pipeline has built-in checkpoint i.e it will skip all complete tasks upon restart."""

    @dataclass
    class Node:
        """Node within a pipeline.

        Attributes:
        - task: The task runs in the node.
        - children: The nodes that should run after the current node completes.
        """
        task: AbstractTask = None
        children = []

    def __init__(self, working_dir: str):
        """
        :param working_dir: the dir where the pipeline reads / writes checkpoints and outputs logs.
        """
        self._source = self.Node()
        self._working_dir = working_dir
        self._tasks = []

    def add_task(self, task: AbstractTask, parents: Optional[List[Node]] = None) -> Node:
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
                future = executor.submit(task.run)
                future.result()
