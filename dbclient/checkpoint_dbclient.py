from dbclient import *
import logging
import os

# List of all objectTypes that we export / import in WM
USER_OBJECT = "users"
GROUP_OBJECT = "groups"
WORKSPACE_NOTEBOOK_OBJECT = "notebooks"
WORKSPACE_DIRECTORY_OBJECT = "directories"
WORKSPACE_NOTEBOOK_ACL_OBJECT = "acl_notebooks"
WORKSPACE_DIRECTORY_ACL_OBJECT = "acl_directories"

# Actions
WM_EXPORT = "export"
WM_IMPORT = "import"

# Wrapper around dbclient to carry out checkpointing of objects exported and imported
class CheckpointDbClient(dbclient):
    def __init__(self, configs):
        self._checkpoint_dir = configs['checkpoint_dir']
        self._use_checkpoint = configs['use_checkpoint']
        # Dict of objectType -> set of completed object_keys
        self._completed_exported_objects = {}
        self._completed_import_objects = {}
        super(CheckpointDbClient, self).__init__(configs)
        os.makedirs(self._checkpoint_dir, exist_ok=True)

    def get_checkpoint_enabled(self):
        return self._use_checkpoint

    """
    completed object_keys is populated from the export log of the objectType. This is used to start the export from
    the last completed step.
    """
    def restore_checkpoint_objects(self, action_type, object_type):
        checkpoint_file = self._get_object_checkpoint_file(action_type, object_type)
        if self._use_checkpoint and os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as read_fp:
                for key in read_fp:
                    if action_type == WM_EXPORT:
                        self._completed_exported_objects.setdefault(object_type, set()).add(key.rstrip())
                    else:
                        self._completed_import_objects.setdefault(object_type, set()).add(key.rstrip())
    """
    Wrapper around dbclient.get() which does a conditional get based on request already checkpointed or not.
    returns a tuple:
    tuple_1 - denotes if the object_key is available from checkpoint (already exported)
    tuple_2 - actual results from the api call. If tuple_1 is True, this value will be empty
    """
    def checkpoint_get(self, action_type, object_type, object_key, endpoint, json_params=None, version='2.0', print_json=False):
        if self._use_checkpoint:
            checkpoint_file = self._get_object_checkpoint_file(action_type, object_type)
            if self.is_completed_object(action_type, object_type, object_key):
                logging.info(f"Object_type: {object_type}, object_key: {object_key} already exported")
                return True, {}
            else:
                results = self.get(endpoint, json_params, version, print_json)
                with open(checkpoint_file, 'a+') as append_fp:
                    if not self._is_error(results):
                        append_fp.write(object_key + "\n")
                return False, results
        else:
            results = self.get(endpoint, json_params, version, print_json)
            return False, results

    def is_completed_object(self, action_type, object_type, object_key):
        if action_type == WM_EXPORT:
            return object_type in self._completed_exported_objects and object_key in self._completed_exported_objects[object_type]
        else:
            return object_type in self._completed_import_objects and object_key in self._completed_import_objects[object_type]

    def _get_object_checkpoint_file(self, action_type, object_type):
        return f"{self._checkpoint_dir}/{action_type}_{object_type}.log"

    def _is_error(self, result):
        return 'error_code' in result or 'error' in result

