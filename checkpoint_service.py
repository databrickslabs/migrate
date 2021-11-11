import os
import logging
from abc import ABC, abstractmethod

# List of all objectTypes that we export / import in WM
USER_OBJECT = "users"
GROUP_OBJECT = "groups"
WORKSPACE_NOTEBOOK_OBJECT = "notebooks"
WORKSPACE_DIRECTORY_OBJECT = "directories"
WORKSPACE_NOTEBOOK_ACL_OBJECT = "acl_notebooks"
WORKSPACE_DIRECTORY_ACL_OBJECT = "acl_directories"
METASTORE_TABLES = "metastore"

# Migration pipeline placeholder constants
MIGRATION_PIPELINE_ACTION_TYPE = "pipeline"
MIGRATION_PIPELINE_OBJECT_TYPE = "tasks"

# Actions
WM_EXPORT = "export"
WM_IMPORT = "import"

class AbstractCheckpointKeySet(ABC):
    """Abstract base class for checkpoint read and write."""

    @abstractmethod
    def write(self, key):
        """Writes key into checkpoint file."""
        pass

    @abstractmethod
    def contains(self, key):
        """Checks if key exists in checkpoint"""
        pass

class CheckpointKeySet(AbstractCheckpointKeySet):
    """Deals with checkpoint read and write."""

    def __init__(self, checkpoint_file):
        """
        :param checkpoint_file: file to read / write object keys for checkpointing
        """
        self._checkpoint_file = checkpoint_file
        self._checkpoint_file_append_fp = open(checkpoint_file, 'a+')
        self._checkpoint_key_set = set()
        self._restore_from_checkpoint_file()

    def write(self, key):
        """Writes key into checkpoint file. This also flushes data after write to prevent loss of checkpoint data on
        system crash.

        CAUTION: Make sure to persist your data before calling write. There is risk of data loss if your data is not persisted
        and checkpointed on system crash.
        """
        if key not in self._checkpoint_key_set:
            self._checkpoint_file_append_fp.write(key + "\n")
            self._checkpoint_file_append_fp.flush()

    def contains(self, key):
        """Checks if key exists in the checkpoint set"""
        exists = key in self._checkpoint_key_set
        if exists:
            logging.info(f"key: {key} found in checkpoint")
        return exists

    def _restore_from_checkpoint_file(self):
        """Reads all checkpoint keys from checkpoint_file into a set at initialization."""
        if os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, 'r') as read_fp:
                for key in read_fp:
                    self._checkpoint_key_set.add(key.rstrip())

    def __del__(self):
        self._checkpoint_file_append_fp.close()

class DisabledCheckpointKeySet(AbstractCheckpointKeySet):
    """Class used to denote disabled checkpointing."""

    def write(self, key):
        pass

    def contains(self, key):
        return False


class CheckpointService():
    """
    Class that provides checkpoint utils of different object types. Checkpoint is used for fault tolerance so that we
     can restart from where we left off.
    """

    def __init__(self, configs):
        self._checkpoint_enabled = configs['use_checkpoint']
        self._checkpoint_dir = configs['export_dir'] + "checkpoint/"
        os.makedirs(self._checkpoint_dir, exist_ok=True)

    @property
    def checkpoint_enabled(self):
        return self._checkpoint_enabled

    def get_checkpoint_key_set(self, action_type, object_type):
        if self._checkpoint_enabled:
            checkpoint_file = f"{self._checkpoint_dir}/{action_type}_{object_type}.log"
            return CheckpointKeySet(checkpoint_file)
        else:
            return DisabledCheckpointKeySet()

