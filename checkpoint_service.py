import os
import logging

# List of all objectTypes that we export / import in WM
USER_OBJECT = "users"
GROUP_OBJECT = "groups"
WORKSPACE_NOTEBOOK_OBJECT = "notebooks"
WORKSPACE_DIRECTORY_OBJECT = "directories"
WORKSPACE_NOTEBOOK_ACL_OBJECT = "acl_notebooks"
WORKSPACE_DIRECTORY_ACL_OBJECT = "acl_directories"
METASTORE_TABLES = "metastore"

# Actions
WM_EXPORT = "export"
WM_IMPORT = "import"

class CheckpointKeySet():
    """Class that deals with checkpoint read and write of a particular object type"""

    """
    :param checkpoint_enabled: checkpoint enabled / disabled flag
    :param checkpoint_file: file to read / write object keys for checkpointing
    """
    def __init__(self, checkpoint_enabled, checkpoint_file):
        self._checkpoint_enabled = checkpoint_enabled
        self._checkpoint_file = checkpoint_file
        self._checkpoint_key_set = set()
        self._restore_from_checkpoint_file()

    # writes key to the checkpoint file
    def write(self, key):
        if self._checkpoint_enabled and key not in self._checkpoint_key_set:
            with open(self._checkpoint_file, 'a+') as append_fp:
                append_fp.write(key + "\n")

    # returns True if key is already checkpointed
    def contains(self, key):
        exists = key in self._checkpoint_key_set
        if exists:
            logging.info(f"key: {key} found in checkpoint")
        return exists

    # read keys from checkpoint file into a set
    def _restore_from_checkpoint_file(self):
        if self._checkpoint_enabled and os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, 'r') as read_fp:
                for key in read_fp:
                    self._checkpoint_key_set.add(key.rstrip())

class CheckpointService():
    """
    Class that provides checkpoint utils of different object types
    """

    def __init__(self, configs):
        self._use_checkpoint = configs['use_checkpoint']
        self._checkpoint_dir = configs['export_dir'] + "checkpoint/"
        self._completed_pipeline_steps = set()
        os.makedirs(self._checkpoint_dir, exist_ok=True)

    def get_checkpoint_enabled(self):
        return self._use_checkpoint

    def get_checkpoint_object_set(self, action_type, object_type):
        checkpoint_file = f"{self._checkpoint_dir}/{action_type}_{object_type}.log"
        return CheckpointKeySet(self.get_checkpoint_enabled(), checkpoint_file)

    def get_checkpoint_pipeline_steps(self):
        checkpoint_file = f"{self._checkpoint_dir}/pipeline_steps.log"
        return CheckpointKeySet(self.get_checkpoint_enabled(), checkpoint_file)

