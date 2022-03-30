import os
from abc import ABC, abstractmethod
import logging
import json
import threading
import time
from thread_safe_writer import ThreadSafeWriter

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

class AbstractCheckpointKeyMap(ABC):
    """Abstract base class for checkpoint read and write."""
    @abstractmethod
    def write(self, key):
        """Writes key and value into checkpoint file."""
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
        # By using ThreadSafeWriter, checkpointer is also thread-safe.
        self._checkpoint_file_append_fp = ThreadSafeWriter(checkpoint_file, 'a')
        self._checkpoint_key_set = set()
        self._restore_from_checkpoint_file()

    def write(self, key):
        """Writes key into checkpoint file. This also flushes data after write to prevent loss of checkpoint data on
        system crash.

        CAUTION: Make sure to persist your data before calling write. There is risk of data loss if your data is not persisted
        and checkpointed on system crash.
        """
        if key not in self._checkpoint_key_set:
            self._checkpoint_file_append_fp.write(str(key) + "\n")

    def contains(self, key):
        """Checks if key exists in the checkpoint set"""
        exists = key in self._checkpoint_key_set
        if exists:
            logging.info(f"{key} found in checkpoint")
        return exists

    def _restore_from_checkpoint_file(self):
        """Reads all checkpoint keys from checkpoint_file into a set at initialization."""
        if os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, 'r') as read_fp:
                for key in read_fp:
                    self._checkpoint_key_set.add(key.rstrip('\n'))

    def __del__(self):
        self._checkpoint_file_append_fp.close()


class CheckpointKeyMap(AbstractCheckpointKeyMap):
    """Deals with checkpoint read and write. Unlike CheckpointKeySet, it also saves the value.
    Useful when the corresponding value is needed.
    """
    def __init__(self, checkpoint_file):
        """
        :param checkpoint_file: file to read / write object keys for checkpointing
        """
        self._checkpoint_file = checkpoint_file
        self._checkpoint_key_map = {}
        self._checkpoint_file_append_fp = ThreadSafeWriter(checkpoint_file, 'a')
        self._restore_from_checkpoint_file()

    def write(self, key, value):
        if key not in self._checkpoint_key_map or "IN_USE_BY" in self._checkpoint_key_map[key]:
            self._checkpoint_key_map[key] = value
            self._checkpoint_file_append_fp.write(json.dumps({"key": str(key), "value": str(value)}) + "\n")

    def check_contains_otherwise_mark_in_use(self, key):
        """
        If the key_map does not have the key value yet, mark the key to be IN_USE_BY_$THREAD_ID, and return False.
        If the key_map has the key value,
           if the value is "IN_USE_BY_XXX" wait for the result to be ready and return True (self.contains(key))
           if the value is not "IN_USE_BY_XXX" return True (self.contains(key))

        This method is thread-safe since map.setdefault(key, value) is thread-safe
        The thread that calls this method and gets False (meaning, the value wasn't there and thus this thread is using
        this key) must set the value of this key subsequently to make sure other threads do not wait for this key
        forever.
        """
        in_use_str = f"IN_USE_BY_{threading.get_ident()}"
        # setdefault is thread safe, so only one thread can successfully set the value for the key.
        result = self._checkpoint_key_map.setdefault(key, in_use_str)
        if result == in_use_str:
            return False
        else:
            while key in self._checkpoint_key_map and "IN_USE_BY" in self._checkpoint_key_map[key]:
                logging.info(f"Waiting for {key} result to be available..")
                time.sleep(5)
            return self.contains(key)

    def contains(self, key):
        exists = key in self._checkpoint_key_map
        if exists:
            logging.info(f"{key} found in checkpoint")
        return exists

    def remove(self, key):
        if key in self._checkpoint_key_map:
            self._checkpoint_key_map.pop(key)

    def get(self, key):
        return self._checkpoint_key_map[key]

    def get_file_path(self):
        return self._checkpoint_file

    def _restore_from_checkpoint_file(self):
        if os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, 'r') as read_fp:
                for single_key_value_map_str in read_fp:
                    single_key_value_map = json.loads(single_key_value_map_str)
                    self._checkpoint_key_map[single_key_value_map["key"]] = single_key_value_map["value"]

    def __del__(self):
        self._checkpoint_file_append_fp.close()


class DisabledCheckpointKeySet(AbstractCheckpointKeySet):
    """Class used to denote disabled checkpointing."""

    def write(self, key):
        pass

    def contains(self, key):
        return False


class DisabledCheckpointKeyMap(AbstractCheckpointKeyMap):
    def write(self, key, value):
        raise NotImplementedError("Checkpoint is disabled")

    def contains(self, key):
        return False

    def check_contains_otherwise_mark_in_use(self, key):
        raise NotImplementedError("Checkpoint is disabled")

    def get(self, key):
        raise NotImplementedError("Checkpoint is disabled")

    def get_file_path(self):
        raise NotImplementedError("Checkpoint is disabled")


class CheckpointService():
    """
    Class that provides checkpoint utils of different object types. Checkpoint is used for fault tolerance so that we
     can restart from where we left off.
    """

    def __init__(self, configs):
        self._checkpoint_enabled = configs['use_checkpoint']
        self._checkpoint_dir = configs['export_dir'] + "checkpoint/"
        os.makedirs(self._checkpoint_dir, exist_ok=True)

    def _get_checkpoint_file(self, action_type, object_type):
        return f"{self._checkpoint_dir}/{action_type}_{object_type}.log"

    @property
    def checkpoint_enabled(self):
        return self._checkpoint_enabled

    def checkpoint_file_exists(self, action_type, object_type):
        return os.path.exists(self._get_checkpoint_file(action_type, object_type))

    def get_checkpoint_key_set(self, action_type, object_type):
        if self._checkpoint_enabled:
            checkpoint_file = self._get_checkpoint_file(action_type, object_type)
            return CheckpointKeySet(checkpoint_file)
        else:
            return DisabledCheckpointKeySet()

    def get_checkpoint_key_map(self, action_type, object_type):
        if self._checkpoint_enabled:
            checkpoint_file = self._get_checkpoint_file(action_type, object_type)
            return CheckpointKeyMap(checkpoint_file)
        else:
            return DisabledCheckpointKeyMap()
