from abc import abstractmethod
from typing import Self, Sequence, Never, Final, Mapping, Callable

import maid.cache

type TaskBuilder = Callable[[], DependencyGraphTask]


class DependencyGraphTask:

    def __init__(
            self,
            name: str = '',
            *,
            required_tasks: Sequence[TaskBuilder] = tuple(),
            required_files: Sequence[str] = tuple(),
            targets: Sequence[str] = tuple(),
            cache: maid.cache.CacheType = maid.cache.CacheType.NONE,
            dont_run_if_all_targets_exist: bool = False,
            ):
        self.name: Final[str] = name

        self.required_tasks: Final[Mapping[str, DependencyGraphTask]] = {
            t().name: t() for t in required_tasks
        }

        self.required_files: Final[Sequence[str]] = tuple(required_files)
        self.targets: Final[Sequence[str]] = tuple(targets)

        self.dont_run_if_all_targets_exist: Final[bool] = dont_run_if_all_targets_exist

        self.cache: Final[maid.cache.CacheType] = cache

    @abstractmethod
    def dry_run(self, verbose: bool = False) -> str:
        return ''
