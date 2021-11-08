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

class CheckpointObjectSet():
    """Class that deals with checkpoint read and write of a particular object type"""

    """
    :param action_type: export / import action.
    :param object_type: object type
    :checkpoint_enabled: checkpoint enabled / disabled flag
    :checkpoint_file: file to read / write object keys for checkpointing
    """
    def __init__(self, action_type, object_type, checkpoint_enabled, checkpoint_file):
        self.action_type = action_type
        self.object_type = object_type
        self._checkpoint_enabled = checkpoint_enabled
        self._checkpoint_file = checkpoint_file
        self._checkpoint_object_set = set()
        self._restore_from_checkpoint_file()

    # writes object_key to the checkpoint file
    def write(self, object_key):
        if self._checkpoint_enabled and object_key not in self._checkpoint_object_set:
            with open(self._checkpoint_file, 'a+') as append_fp:
                append_fp.write(object_key + "\n")

    # returns True if object_key is already checkpointed and exists in the set
    def contains(self, object_key):
        exists = object_key in self._checkpoint_object_set
        if exists:
            logging.info(f"action: {self.action_type}, object_type: {self.object_type}, object_key: {object_key} found in checkpoint")
        return exists

    # read object keys from checkpoint file into a set
    def _restore_from_checkpoint_file(self):
        if self._checkpoint_enabled and os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, 'r') as read_fp:
                for key in read_fp:
                    self._checkpoint_object_set.add(key.rstrip())

class CheckpointService():
    """
    Class that provides checkpoint utils of different object types
    """

    def __init__(self, configs):
        self._use_checkpoint = configs['use_checkpoint']
        self._checkpoint_dir = configs['export_dir'] + "checkpoint/"
        os.makedirs(self._checkpoint_dir, exist_ok=True)

    def get_checkpoint_enabled(self):
        return self._use_checkpoint

    def get_checkpoint_object_set(self, action_type, object_type):
        checkpoint_file = self._get_object_checkpoint_file(action_type, object_type)
        return CheckpointObjectSet(action_type, object_type, self.get_checkpoint_enabled(), checkpoint_file)

    def _get_object_checkpoint_file(self, action_type, object_type):
        return f"{self._checkpoint_dir}/{action_type}_{object_type}.log"