import unittest

from .pipeline import Pipeline
from .task import AbstractTask
from checkpoint_service import *

TEST_CHECKPOINT_FILE = 'pipeline/test_data/pipeline_steps.log'

class AppendTask(AbstractTask):
    def __init__(self, name, number: int, result):
        super().__init__(name)
        self.number = number
        self._result = result

    def run(self):
        self._result.append(self.number)

class PipelineTest(unittest.TestCase):

    def setUp(self):
        if os.path.exists(TEST_CHECKPOINT_FILE):
            os.remove(TEST_CHECKPOINT_FILE)

    def test_run(self):
        result = []
        task_1 = AppendTask("task1", 1, result)
        task_2 = AppendTask("task2", 2, result)
        task_3 = AppendTask("task3", 3, result)
        task_4 = AppendTask("task4", 4, result)

        test_pipeline_steps_key_set = CheckpointKeySet(False, TEST_CHECKPOINT_FILE)
        pipeline = Pipeline('test_data', test_pipeline_steps_key_set)
        node_1 = pipeline.add_task(task_1)
        node_2 = pipeline.add_task(task_2, [node_1])
        node_3 = pipeline.add_task(task_3, [node_1])
        pipeline.add_task(task_4, [node_2, node_3])

        pipeline.run()

        self.assertEqual(result, [1, 2, 3, 4])

    def test_run_with_checkpoint(self):
        result = []
        task_1 = AppendTask("task1", 1, result)
        task_2 = AppendTask("task2", 2, result)
        task_3 = AppendTask("task3", 3, result)
        task_4 = AppendTask("task4", 4, result)

        with open(TEST_CHECKPOINT_FILE, 'w+') as wp:
            wp.write(task_1.name + "\n")

        test_pipeline_steps_key_set = CheckpointKeySet(True, TEST_CHECKPOINT_FILE)
        pipeline = Pipeline('test_data', test_pipeline_steps_key_set)
        node_1 = pipeline.add_task(task_1)
        node_2 = pipeline.add_task(task_2, [node_1])
        node_3 = pipeline.add_task(task_3, [node_1])
        pipeline.add_task(task_4, [node_2, node_3])

        pipeline.run()
        self.assertEqual(result, [2, 3, 4])

        # run again after all steps are checkpointed
        result = []
        task_1 = AppendTask("task1", 1, result)
        task_2 = AppendTask("task2", 2, result)
        task_3 = AppendTask("task3", 3, result)
        task_4 = AppendTask("task4", 4, result)
        test_pipeline_steps_key_set = CheckpointKeySet(True, TEST_CHECKPOINT_FILE)
        pipeline = Pipeline('test_data', test_pipeline_steps_key_set)
        node_1 = pipeline.add_task(task_1)
        node_2 = pipeline.add_task(task_2, [node_1])
        node_3 = pipeline.add_task(task_3, [node_1])
        pipeline.add_task(task_4, [node_2, node_3])

        pipeline.run()
        self.assertEqual(result, [])

if __name__ == '__main__':
    unittest.main()
