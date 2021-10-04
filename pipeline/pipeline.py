from dataclasses import dataclass
from typing import List, Optional

from task import AbstractTask


class Pipeline:
    """Class that coordinates run of a group of tasks, which form a DAG based on dependencies
    defined in add_task().

    The pipeline has built-in checkpoint i.e it will skip all complete tasks upon restart."""

    @dataclass
    class Node:
        """Node within a pipeline.

        task: The task runs in the node.
        children: The nodes that should run after the current node completes.
        """
        task: AbstractTask = None
        children = []

    def __init__(self, working_dir: Optional[str]):
        """
        :param working_dir: the dir where the pipeline reads / writes checkpoints and outputs logs.
        """
        self._source = self.Node()
        self._working_dir = working_dir

    def add_task(self, task: AbstractTask, parents: Optional[List[Node]] = None) -> Node:
        node = self.Node(task)
        if not parents:
            parents = [self._source]
        for parent in parents:
            parent.children.append(node)
        return node

    def run(self):
        pass
