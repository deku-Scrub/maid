import pathlib
import os


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


def should_task_run(graph):
    def f(task_name):
        task = graph[task_name]
        # If any outputs don't exist, the task should run.
        oldest_time = float('inf')
        for o in task.get_targets():
            if not os.path.exists(o):
                return (task_name, o)
            oldest_time = min(os.path.getmtime(o), oldest_time)

        # If any outputs are newer than required files, the task
        # should run.
        if _is_any_newer(task.get_required_files(), oldest_time):
            return (task_name, '')

        # If any outputs of required tasks have been updated, the
        # task should run.
        for req in task.required_tasks:
            if _is_any_newer(graph[req].get_targets(), oldest_time, must_exist=False):
                return (req, '')

        return tuple()

    return f
