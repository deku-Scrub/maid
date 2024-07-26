import enum
import os

import maid.cache.filehash
import maid.cache.timestamp
import maid.files


class CacheType(enum.Enum):
    NONE = 0
    HASH = 1
    TIME = 2


class TaskCacher:

    def __init__(self, task):
        self._task = task

    def cache_targets(self):
        '''
        '''
        if self._task.cache == CacheType.HASH:
            # Cache everything because required files might have
            # been missing.
            self.cache_all()
        elif self._task.cache == CacheType.TIME:
            _update_files(self._task.cache, self._task.targets)

    def cache_all(self):
        _update_files(self._task.cache, self._task.required_files)
        _update_files(self._task.cache, self._task.targets)

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

        if should_task_run(self._task):
            return False, 'targets out of date'
        return True, ''


def _update_files(cache, filenames):
    if cache == CacheType.TIME:
        maid.cache.timestamp.touch_files(maid.files.get_filenames(filenames))
    elif cache == CacheType.HASH:
        maid.cache.filehash.make_hashes(maid.files.get_filenames(filenames))


def any_files_missing(filenames, must_exist=True):
    filenames = maid.files.get_filenames(filenames, must_exist=must_exist)
    return next((f for f in filenames if not os.path.exists(f)), '')
