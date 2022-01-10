import unittest
import os

from .pipeline import Pipeline
from .task import AbstractTask
from checkpoint_service import CheckpointKeySet, DisabledCheckpointKeySet

TEST_CHECKPOINT_FILE = 'pipeline/test_data/pipeline_steps.log'

class AppendTask(AbstractTask):
    def __init__(self, name, number: int, result, skip=False):
        super().__init__(name, skip=skip)
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

    def test_run_with_skip_tasks(self):
        result = []
        test_pipeline_steps_key_set = DisabledCheckpointKeySet()
        task_skip_boolean_tuples = [("task1", False),
                                    ("task2", True),
                                    ("task3", True),
                                    ("task4", False)]
        pipeline = self._create_test_pipeline_with_skipped_tasks(
            test_pipeline_steps_key_set, task_skip_boolean_tuples, result)
        pipeline.run()
        self.assertEqual(result, [1, 4])

    def test_run_with_skip_all_tasks(self):
        result = []
        test_pipeline_steps_key_set = DisabledCheckpointKeySet()
        task_skip_boolean_tuples = [("task1", True),
                                    ("task2", True),
                                    ("task3", True),
                                    ("task4", True)]
        pipeline = self._create_test_pipeline_with_skipped_tasks(
            test_pipeline_steps_key_set, task_skip_boolean_tuples, result)
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

    def _create_test_pipeline_with_skipped_tasks(self, pipeline_steps_key_set, task_name_skip_boolean_tuples, result):
        pipeline = Pipeline('test_data', pipeline_steps_key_set)
        parents = []
        for idx, (task_name, skip) in enumerate(task_name_skip_boolean_tuples):
            task = AppendTask(task_name, idx+1, result, skip=skip)
            node = pipeline.add_task(task, parents)
            parents = [node]

        return pipeline



if __name__ == '__main__':
    unittest.main()
