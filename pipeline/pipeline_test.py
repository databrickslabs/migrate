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
        test_pipeline_steps_key_set = DisabledCheckpointKeySet()
        pipeline = self._create_test_pipeline(test_pipeline_steps_key_set, ["task1", "task2", "task3", "task4"], result)
        pipeline.run()

        self.assertEqual(result, [1, 2, 3, 4])

    def test_run_with_checkpoint(self):
        with open(TEST_CHECKPOINT_FILE, 'w+') as wp:
            wp.write("task1\n")

        result = []
        test_pipeline_steps_key_set = CheckpointKeySet(TEST_CHECKPOINT_FILE)
        pipeline = self._create_test_pipeline(test_pipeline_steps_key_set, ["task1", "task2", "task3", "task4"], result)
        pipeline.run()
        self.assertEqual(result, [2, 3, 4])

        # run again after all steps are checkpointed
        result = []
        test_pipeline_steps_key_set = CheckpointKeySet(TEST_CHECKPOINT_FILE)
        pipeline = self._create_test_pipeline(test_pipeline_steps_key_set, ["task1", "task2", "task3", "task4"], result)
        pipeline.run()
        self.assertEqual(result, [])

    def _create_test_pipeline(self, pipeline_steps_key_set, task_names, result):
        pipeline = Pipeline('test_data', pipeline_steps_key_set)
        parents = []
        for idx, task_name in enumerate(task_names):
            task = AppendTask(task_name, idx+1, result)
            node = pipeline.add_task(task, parents)
            parents = [node]

        return pipeline



if __name__ == '__main__':
    unittest.main()
