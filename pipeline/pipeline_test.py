import unittest

from .pipeline import Pipeline
from .task import AbstractTask


class AppendTask(AbstractTask):
    result = []

    def __init__(self, name, number: int):
        super().__init__(name)
        self.number = number

    def run(self):
        AppendTask.result.append(self.number)


class PipelineTest(unittest.TestCase):
    def test_run(self):
        task_1 = AppendTask("task1", 1)
        task_2 = AppendTask("task2", 2)
        task_3 = AppendTask("task3", 3)
        task_4 = AppendTask("task4", 4)

        pipeline = Pipeline('test_working_dir')
        node_1 = pipeline.add_task(task_1)
        node_2 = pipeline.add_task(task_2, [node_1])
        node_3 = pipeline.add_task(task_3, [node_1])
        pipeline.add_task(task_4, [node_2, node_3])

        pipeline.run()

        self.assertEqual(AppendTask.result, [1, 2, 3, 4])


if __name__ == '__main__':
    unittest.main()
