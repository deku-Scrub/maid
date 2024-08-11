import traceback
import os
import sys
import io
import enum
import shutil
import pathlib
import hashlib
import functools
import itertools
import dataclasses
from dataclasses import dataclass, field
from typing import Optional, Iterable, Sequence, Callable, Any, Final

import maid2.compose


class CacheType(enum.Enum):
    NONE = 0
    HASH = 1
    TIME = 2


class RunPhase(enum.Enum):
    NORMAL = 0
    START = 1
    END = 2
    FINALLY = 3


@dataclass(frozen=True, kw_only=True)
class Task:
    recipe: Callable[['Task'], maid2.compose.Recipe] = field(kw_only=False)
    name: str = ''
    targets: Sequence[str] = tuple()
    required_files: Sequence[str] = tuple()
    required_tasks: Sequence['Task'] = tuple()
    grouped: bool = True
    remove_targets_on_failure: bool = True
    delay_failures: bool = False
    cache_type: CacheType = CacheType.NONE
    dont_run_if_all_targets_exist: bool = False
    run_phase: RunPhase = RunPhase.NORMAL
    is_default: bool = False
    hash_dirname: str = field(default='.maid', init=False)

    def get_stored_inputs(self) -> str:
        if not self.name:
            return ''
        return hash_file(
                os.path.join(self.hash_dirname, 'tasks', self.name),
                self.cache_type,
                )

    def get_actual_inputs(self) -> str:
        if not self.name:
            return ''
        return C(
                expand_requirements(self),
                os.path.join(self.hash_dirname, 'tasks', self.name + '.new'),
                self.cache_type,
                )

    def run_recipe(self, target: str = '') -> Iterable[Any]:
        if target:
            return dataclasses.replace(
                    self,
                    name='',
                    targets=(target,),
                    ).run_recipe()
        return self.recipe(self).run()

    def run(self) -> Optional[Exception]:
        return run(self, set())

    def dry_run(self) -> str:
        return dry_run(self, set())


def expand_requirements(task: Task) -> Iterable[str]:
    return itertools.chain(
            expand_globs(task.required_files),
            (f for t in task.required_tasks for f in expand_globs(t.targets)),
            )


def unvisited_tasks(task: Task, visited: set[str]) -> Iterable[Task]:
    yield from (t for t in task.required_tasks if t.name not in visited)


def dry_run(task: Task, visited: set[str]) -> str:
    visited.add(task.name)
    return '\n'.join(
            dry_run(t, visited) for t in unvisited_tasks(task, visited)
            ) + str(task)


def run(task: Task, visited: set[str]) -> Optional[Exception]:
    visited.add(task.name)
    err = E1(run(t, visited) for t in unvisited_tasks(task, visited))
    return err if err else E1((S(task),))


def S(task: Task) -> Optional[Exception]:
    if D(task):
        return None
    if T(task):
        return A(task)
    if is_queued(task.name):
        return X(task)
    if N(task):
        return X(task)
    return None


def N(task: Task) -> bool:
    return task.cache_type == CacheType.NONE


def D(task: Task) -> bool:
    return (
            task.dont_run_if_all_targets_exist
            and all(os.path.exists(t) for t in task.targets)
            )


def T(task: Task) -> bool:
    return task.get_actual_inputs() != task.get_stored_inputs()


def A(task: Task) -> Optional[Exception]:
    return err if (err := A1(task)) else X(task)


def A1(task: Task) -> Optional[Exception]:
    err = queue_files(itertools.chain(
        (task.name,),
        tuple() if task.grouped else task.targets,
        ))
    if err:
        return err
    return try_function(lambda: shutil.move(
        os.path.join(task.hash_dirname, 'tasks', task.name + '.new'),
        os.path.join(task.hash_dirname, 'tasks', task.name),
        ))


def try_function(f: Callable[[], Any]) -> Optional[Exception]:
    try:
        _ = f()
        return None
    except Exception as err:
        return err


def dequeue_files(filenames: Iterable[str]) -> Optional[Exception]:
    return try_function(lambda: E1(os.remove(f) for f in filenames))


def queue_files(filenames: Iterable[str]) -> Optional[Exception]:
    return try_function(lambda: E1(pathlib.Path(f).touch() for f in filenames))


def hash_file(filename: str, cache_type: CacheType) -> str:
    hash_algo: Final[str] = 'md5'
    if not os.path.isfile(filename):
        return ''
    if cache_type == CacheType.HASH:
        with open(filename, mode='rb') as fis:
            return hashlib.file_digest(fis, hash_algo).hexdigest()
    if cache_type == CacheType.TIME:
        return hashlib.file_digest(
                io.BytesIO(str(os.path.getmtime(filename)).encode('utf-8')),
                hash_algo,
                ).hexdigest()
    else:
        return ''


def hash_files(
        filenames: Iterable[str],
        cache_type: CacheType,
        ) -> Iterable[str]:
    yield from (hash_file(f, cache_type) for f in filenames)


def C(
        filenames: Iterable[str],
        outfile: str,
        cache_type: CacheType,
        ) -> str:
    if not outfile:
        return ''
    with open(outfile, mode='wt', encoding='utf-8') as fos:
        fos.writelines(hash_files(filenames, cache_type))
    return next(iter(hash_files((outfile,), cache_type)))


def X(task: Task) -> Optional[Exception]:
    if task.grouped:
        return X1(E(P(task), task), task.name)
    return X2(
            (
                X1(E(P(task, target), task), target)
                for target in task.targets
                if is_queued(target)
                ),
            task.name,
            )


def is_queued(filename: str) -> bool:
    return os.path.exists(filename)


def P(task: Task, target: str = '') -> Optional[Exception]:
    try:
        _ = task.run_recipe(target)
        return None
    except Exception as err:
        return err


def expand_globs(filenames: Iterable[str]) -> Iterable[str]:
    yield from (
            str(f)
            for glob in filenames
            for f in pathlib.Path('').glob(glob)
            )


def remove_files(filenames: Iterable[str]) -> Optional[Exception]:
    return E1(
            os.remove(f) if os.path.isfile(f) else shutil.rmtree(f)
            for f in expand_globs(filenames)
            )


def E(error: Optional[Exception], task: Task) -> Optional[Exception]:
    if not error:
        return None
    if task.remove_targets_on_failure:
        remove_files(task.targets) 
    if task.delay_failures:
        return error
    traceback.print_exception(error)
    raise error


def to_error(lhs: object, rhs: object) -> Optional[Exception]:
    if isinstance(lhs, Exception):
        return lhs
    if isinstance(rhs, Exception):
        return rhs
    return None


def E1(outputs: Iterable[object]) -> Optional[Exception]:
    return functools.reduce(to_error, outputs, None)


def X1(error: Optional[Exception], filename: str) -> Optional[Exception]:
    return error if error else dequeue_files((filename,))


def X2(
        errors: Iterable[Optional[Exception]],
        filename: str,
        ) -> Optional[Exception]:
    return err if (err := E1(errors)) else dequeue_files((filename,))
