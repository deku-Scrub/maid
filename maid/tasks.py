import traceback
import base64
import os
import io
import enum
import shutil
import pathlib
import hashlib
import functools
import itertools
import dataclasses
from dataclasses import dataclass, field
from typing import Never, Optional, Iterable, Sequence, Callable, Any, Final, assert_never

import maid.compose


class CacheType(enum.Enum):
    NONE = 0
    HASH = 1
    TIME = 2


class RunPhase(enum.Enum):
    NORMAL = 0
    START = 1
    END = 2
    FINALLY = 3


class RunReason(enum.Enum):
    DONT_RUN = 0
    MISSING_TARGETS = 1
    MODIFIED_INPUTS = 2
    NO_CACHE = 3


@dataclass(frozen=True, kw_only=True)
class Task:
    recipe: Callable[['Task'], maid.compose.Recipe] = field(kw_only=False)
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

    def __post_init__(self) -> None:
        match next(
                (
                    f
                    for f in expand_globs(self.required_files)
                    if not os.path.exists(f)
                    ),
                ''
                ):
            case '':
                return
            case _ as x:
                raise MissingRequiredFileException(self.name, x)

    def get_stored_inputs(self) -> str:
        if not self.name:
            return ''
        return hash_file(to_state_file(self), self.cache_type)

    def get_actual_inputs(self) -> str:
        if not self.name:
            return ''
        return cache_files(
                expand_requirements(self),
                to_state_file(self, suffix='.new'),
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

    def run(self) -> None:
        if (err := run(self, set())):
            traceback.print_exception(err)
            raise err

    def dry_run(self, verbose: bool = True) -> str:
        return dry_run(self, set(), verbose=verbose)[1]

    def __str__(self) -> str:
        return str(self.recipe(self))


def expand_requirements(task: Task) -> Iterable[str]:
    return itertools.chain(
            expand_globs(task.required_files),
            (f for t in task.required_tasks for f in expand_globs(t.targets)),
            )


def unvisited_tasks(task: Task, visited: set[str]) -> Iterable[Task]:
    yield from (t for t in task.required_tasks if t.name not in visited)


def _format_dry_run(
        prev_runs: Iterable[tuple[int, str]],
        task: Task,
        reason_to_run: str,
        *,
        verbose: bool = True,
        ) -> tuple[int, str]:
    match (
            sum((j for j, _ in prev_runs), 0) + 1,
            '{prev}\n{{cur}}'.format(prev='\n'.join(s for _, s in prev_runs)),
            verbose,
            ):
        case (step, template, False):
            return (
                    step,
                    template.format(
                        cur=f'{step}) {task.name} ({reason_to_run})',
                        ),
                    )
        case (step, template, True):
            return (
                    step,
                    template.format(
                        cur=(
                            '###############################################\n'
                            '# Step {step}:\n'
                            '#   Task `{task_name}` will run ({reason})\n'
                            '###############################################\n'
                            '{recipe}\n'
                            ).format(
                                step=step,
                                task_name=task.name,
                                reason=reason_to_run,
                                recipe=str(task),
                                ),
                            ),
                    )
    raise RuntimeError('Unreachable code was reached!')


def dry_run(
        task: Task,
        visited: set[str],
        *,
        verbose: bool = True,
        ) -> tuple[int, str]:
    visited.add(task.name)
    prev_runs = [
            dry_run(t, visited, verbose=verbose)
            for t in unvisited_tasks(task, visited)
            ]

    match should_run(task):
        case RunReason.MISSING_TARGETS:
            return _format_dry_run(
                    prev_runs,
                    task,
                    'targets are missing',
                    verbose=verbose,
                    )
        case RunReason.MODIFIED_INPUTS:
            return _format_dry_run(
                    prev_runs,
                    task,
                    'required files/tasks are modified',
                    verbose=verbose,
                    )
        case RunReason.NO_CACHE:
            return _format_dry_run(
                    prev_runs,
                    task,
                    'uses no cache',
                    verbose=verbose,
                    )
        case RunReason.DONT_RUN:
            pass
        case _ as unreachable:
            assert_never(unreachable)

    if is_queued(task.name):
        return _format_dry_run(
                prev_runs,
                task,
                'previous run did not finish',
                verbose=verbose,
                )
    return 0, ''


def run(task: Task, visited: set[str]) -> Optional[Exception]:
    visited.add(task.name)
    err = find_error(run(t, visited) for t in unvisited_tasks(task, visited))
    return err if err else find_error((start_state_machine(task),))


def start_state_machine(task: Task) -> Optional[Exception]:
    if should_not_run(task):
        return None
    if (reason := should_run(task)) != RunReason.DONT_RUN:
        return start_run(task, reason)
    if is_queued(task.name):
        return start_execution(task)
    return None


def should_not_run(task: Task) -> bool:
    return (
            task.dont_run_if_all_targets_exist
            and all(os.path.exists(t) for t in expand_globs(task.targets))
            )


def should_run(task: Task) -> RunReason:
    if task.get_actual_inputs() != task.get_stored_inputs():
        return RunReason.MODIFIED_INPUTS
    if any(not os.path.exists(f) for f in expand_globs(task.targets)):
        return RunReason.MISSING_TARGETS
    if task.cache_type == CacheType.NONE:
        return RunReason.NO_CACHE
    return RunReason.DONT_RUN


def start_run(task: Task, reason: RunReason) -> Optional[Exception]:
    if (err := setup_file_states(task, reason)):
        return err
    return start_execution(task)


def expand_targets(task: Task, reason: RunReason) -> Iterable[str]:
    if task.grouped:
        return tuple()
    if reason == RunReason.MISSING_TARGETS:
        return (f for f in expand_globs(task.targets) if not os.path.exists(f))
    return expand_globs(task.targets)


def setup_file_states(task: Task, reason: RunReason) -> Optional[Exception]:
    match queue_files(
            itertools.chain((task.name,), expand_targets(task, reason)),
            ):
        case Exception() as err:
            return err
        case _:
            return try_function(lambda: shutil.move(
                to_state_file(task, suffix='.new'),
                to_state_file(task),
                ))


def try_function(f: Callable[[], Any]) -> Optional[Exception]:
    try:
        _ = f()
        return None
    except Exception as err:
        return err


def to_state_file(obj: str | Task, suffix: str = '') -> str:
    if isinstance(obj, str):
        return _filename_to_state_file(obj + suffix, 'targets')
    if isinstance(obj, Task):
        return _filename_to_state_file(obj.name + suffix, 'tasks')
    raise RuntimeError('Unknown type given to `to_state_file`.  Only `str` and `Task` are accepted.')


def _filename_to_state_file(filename: str, dirname: str) -> str:
    return os.path.join(
            '.maid',
            dirname,
            base64.b64encode(filename.encode('utf-8')).decode('utf-8'),
            )


def dequeue_files(filenames: Iterable[str]) -> Optional[Exception]:
    return try_function(
            lambda: find_error(os.remove(to_state_file(f)) for f in filenames),
            )


def queue_files(filenames: Iterable[str]) -> Optional[Exception]:
    return try_function(
            lambda: find_error(
                pathlib.Path(to_state_file(f)).touch() for f in filenames
                ),
            )


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
    return ''


def hash_files(
        filenames: Iterable[str],
        cache_type: CacheType,
        ) -> Iterable[str]:
    yield from (hash_file(f, cache_type) for f in filenames)


def cache_files(
        filenames: Iterable[str],
        outfile: str,
        cache_type: CacheType,
        ) -> str:
    if not outfile:
        return ''
    with open(outfile, mode='wt', encoding='utf-8') as fos:
        fos.writelines(
                (f'{h}\n' for h in hash_files(sorted(filenames), cache_type)),
                )
    return next(iter(hash_files((outfile,), cache_type)))


def start_execution(task: Task) -> Optional[Exception]:
    if task.grouped:
        return execute(task)
    return cleanup_states(
            (
                execute(task, target)
                for target in expand_globs(task.targets)
                if is_queued(target)
                ),
            task.name,
            )


def execute(task: Task, target: str = '') -> Optional[Exception]:
    return cleanup_state(
            handle_error(
                try_function(lambda: task.run_recipe(target)),
                task,
                ),
            target if target else task.name,
            )


def is_queued(filename: str) -> bool:
    return os.path.exists(to_state_file(filename))


def take_from_nonempty(
        x: Iterable[pathlib.Path],
        y: Iterable[pathlib.Path],
        ) -> Iterable[pathlib.Path]:
    yield from (
            xj if xj else yj
            for (xj, yj)
            in itertools.zip_longest(x, y)
            )


def expand_globs(filenames: Iterable[str]) -> Iterable[str]:
    yield from (
            str(f)
            for glob in filenames
            for f in take_from_nonempty(
                pathlib.Path('').glob(glob),
                (pathlib.Path(glob),)
                )
            )


def remove_files(filenames: Iterable[str]) -> Optional[Exception]:
    return find_error(
            os.remove(f) if os.path.isfile(f) else shutil.rmtree(f)
            for f in filenames
            )


def handle_error(error: Optional[Exception], task: Task) -> Optional[Exception]:
    if not error:
        return None
    if task.remove_targets_on_failure:
        remove_files(expand_globs(task.targets))
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


def find_error(outputs: Iterable[object]) -> Optional[Exception]:
    return functools.reduce(to_error, outputs, None)


def cleanup_state(
        error: Optional[Exception],
        filename: str,
        ) -> Optional[Exception]:
    return error if error else dequeue_files((filename,))


def cleanup_states(
        errors: Iterable[Optional[Exception]],
        filename: str,
        ) -> Optional[Exception]:
    return err if (err := find_error(errors)) else dequeue_files((filename,))


class MissingRequiredFileException(Exception):
    '''
    '''

    def __init__(self, task_name: str, filename: str):
        '''
        '''
        msg = 'Task `{}` is missing required file `{}`'.format(
                task_name,
                filename,
                )
        super().__init__(msg)