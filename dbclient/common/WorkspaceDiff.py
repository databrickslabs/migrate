import os
from filecmp import cmpfiles, dircmp
import pathlib
from typing import Set


def get_dir_files(dir: str):
    files = []
    for obj in pathlib.Path(dir).rglob("*"):
        if os.path.isfile(obj):
            files.append(str(obj))
    return files

def get_updated_new_files(dir1: str, dir2: str):
    """ 
    Compare old artifacts directory and current artifacts directory 
    and return all noteboooks that have been updated or newly added to the notebook_changes.log file.
    :param dir1: old artifacts directory
    :param dir2: new artifacts directory
    """
    changed_files = []
    dcmp = dircmp(dir1, dir2)
    for file in dcmp.diff_files:
        changed_files.append(os.path.join(dir2, file))
    for obj in dcmp.right_only:
        objpath = os.path.join(dir2, obj)
        if os.path.isdir(objpath):
            new_files = get_dir_files(objpath)
            changed_files += new_files
        else:
            changed_files.append(objpath)
    for obj in dcmp.common_dirs:
        tmp_files = get_updated_new_files(os.path.join(dir1, obj), os.path.join(dir2, obj))
        changed_files += tmp_files
    
    return set(changed_files)


def log_file_changes(changes_since_last: Set[str], log_file):
    """
    Log the set of file names to a log file.
    """
    if not changes_since_last: return None

    with open(log_file, "w") as fp:
        for changed_file in changes_since_last:
            fp.write(changed_file + "\n")

def read_file_changes(log_file):
    """
    Read file names from a log file.
    """
    with open(log_file, "r") as fp:
        changed_files = fp.readlines()
        return set(changed_files)
    
