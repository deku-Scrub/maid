from abc import abstractmethod
from typing import Self, Sequence, Never, Final, Mapping, Callable

import maid.cache


class DependencyGraphTask:

    def __init__(
            self,
            name: str = '', # o
            *,
            required_tasks: Sequence[Callable[[], 'DependencyGraphTask']] | None = None, # o
            required_files: Sequence[str] | None = None, # o
            targets: Sequence[str] | None = None, # o
            cache: maid.cache.CacheType = maid.cache.CacheType.NONE,
            dont_run_if_all_targets_exist: bool = False,
            ):
        self.name: Final[str] = name

        rp = required_tasks if required_tasks else dict()
        self.required_tasks: Final[Mapping[str, DependencyGraphTask]] = {t().name: t() for t in rp} if required_tasks else dict()

        self.required_files: Final[tuple[str, ...]] = tuple(required_files) if required_files else tuple()
        self.targets: Final[tuple[str, ...]] = tuple(targets) if targets else tuple()

        self.dont_run_if_all_targets_exist: Final[bool] = dont_run_if_all_targets_exist
        self.cache: Final[maid.cache.CacheType] = cache

    @abstractmethod
    def dry_run(self, verbose: bool = False) -> str:
        return ''
