import enum
import os

import maid.cache.filehash
import maid.cache.timestamp


class CacheType(enum.Enum):
    NONE = 0
    HASH = 1
    TIME = 2


class TaskCacher:

    def __init__(self, task):
        self._task = task

    def cache(self):
        '''
        '''
        _update_files(self._task.cache, self._task.targets)
        _update_files(self._task.cache, self._task.required_files)

    def is_up_to_date(self):
        '''
        '''
        # Checks based on file existance.
        if (f := any_files_missing(self._task.targets, must_exist=False)):
            return False, f'missing target `{f}`'
        if self._task.dont_run_if_all_targets_exist:
            return True, ''

        # Checks based on cache type.
        if self._task.cache == CacheType.NONE:
            return False, 'uncached task'
        return self._is_cached()

    def _is_cached(self):
        # Get appropriate decision function.
        should_task_run = maid.cache.filehash.should_task_run
        if self._task.cache == CacheType.TIME:
            should_task_run = maid.cache.timestamp.should_task_run

        if should_task_run(self._get_graph())(self._task.name):
            return False, 'targets out of date'
        return True, ''

    def _get_graph(self):
        '''
        '''
        task = self._task

        # This is the graph required for the time and hash cache
        # decision functions.
        graph = {
            p.name: maid.tasks.Task(
                    p.name,
                    lambda a: a, # This doesn't matter; never runs.
                    targets=p.targets,
                    )
            for p in task.required_tasks.values()
        }
        graph[task.name] = maid.tasks.Task(
                    task.name,
                    lambda a: a, # This doesn't matter; never runs.
                    targets=task.targets,
                    required_files=task.required_files,
                    required_tasks=tuple(
                        [p.name for p in task.required_tasks.values()]
                        ),
                    )
        return graph


def _update_files(cache, filenames):
    if cache == CacheType.TIME:
        maid.cache.timestamp.touch_files(maid.tasks.get_filenames(filenames))
    elif cache == CacheType.HASH:
        maid.cache.filehash.make_hashes(maid.tasks.get_filenames(filenames))


def any_files_missing(filenames, must_exist=True):
    filenames = maid.tasks.get_filenames(filenames, must_exist=must_exist)
    return next((f for f in filenames if not os.path.exists(f)), '')


