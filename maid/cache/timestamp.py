import pathlib
import os

import maid.files


def touch_files(filenames):
    for f in filenames:
        pathlib.Path(f).touch()


def _is_any_newer(filenames, target_time, must_exist=True):
    for f in filenames:
        if not must_exist:
            if os.path.exists(f) and (os.path.getmtime(f) > target_time):
                return True
            continue
        if (not os.path.exists(f)) or (os.path.getmtime(f) > target_time):
            return True
    return False


def should_task_run(task):
    # If any outputs don't exist, the task should run.
    oldest_time = float('inf')
    for o in maid.files.get_filenames(task.targets):
        if not os.path.exists(o):
            return (task.name, o)
        oldest_time = min(os.path.getmtime(o), oldest_time)

    # If any outputs are newer than required files, the task
    # should run.
    if _is_any_newer(maid.files.get_filenames(task.required_files), oldest_time):
        return (task.name, '')

    # If any outputs of required tasks have been updated, the
    # task should run.
    for req in task.required_tasks:
        if _is_any_newer(maid.files.get_filenames(req.targets), oldest_time, must_exist=False):
            return (req, '')

    return tuple()
