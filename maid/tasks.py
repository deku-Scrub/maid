import shlex
import concurrent.futures
import multiprocessing
import re
import traceback
import mmap
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
from typing import IO, Optional, Iterable, Sequence, Callable, Any, Final, assert_never, cast

import maid.compose
import maid.utils.setops

type ExecuteMapCallable = Callable[
        [
            Callable[[tuple[Task, str]], Optional[Exception]],
            Iterable[tuple[Task, str]]
            ],
        Iterable[Optional[Exception]]
        ]


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
    parent: Optional['Task'] = None
    tied_targets: Optional[tuple[str, str, str, str, Sequence[str], str]] = None

    def __post_init__(self) -> None:
        if self.tied_targets and self.targets:
            raise InvalidTargetsError(self)
        object.__setattr__(
                self,
                'grouped',
                False if self.tied_targets else self.grouped,
                )
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

    def get_modified_files(self) -> Iterable[str]:
        if self.parent and self.parent.tied_targets:
            return pathlib.Path(to_state_file(self.parent, self.targets[0])).read_text().split('\n')[0].split('\t')
        if self.parent:
            return self.parent.get_modified_files()
        if not os.path.exists(diff := to_state_file(self, suffix='.diff')):
            return tuple()
        with open(diff) as fis:
            return fis.readlines()

    def run_recipe(self, target: str = '') -> Iterable[Any]:
        if target and self.tied_targets and (os.path.getsize(to_state_file(self, target)) > 0):
            return dataclasses.replace(
                    self,
                    name='',
                    targets=(target,),
                    tied_targets=None,
                    parent=self,
                    required_files=pathlib.Path(to_state_file(self, target)).read_text().split('\n')[1].split('\t'),
                    ).run_recipe()
        if target:
            return dataclasses.replace(
                    self,
                    name='',
                    targets=(target,),
                    parent=self,
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

    def qjoin_targets(self) -> str:
        return self.join_targets(shlex.quote)

    def join_targets(self, f: Callable[[str], str] = str) -> str:
        return ' '.join(f(x) for x in expand_globs(self.targets))

    def qjoin_modified_prereqs(self) -> str:
        return self.join_modified_prereqs(shlex.quote)

    def join_modified_prereqs(self, f: Callable[[str], str] = str) -> str:
        return ' '.join(f(x) for x in self.get_modified_files())

    def qjoin_prereqs(self) -> str:
        return self.join_prereqs(shlex.quote)

    def join_prereqs(self, f: Callable[[str], str] = str) -> str:
        return ' '.join(f(x) for x in expand_requirements(self))


def try_function(f: Callable[[], Any]) -> Optional[Exception]:
    try:
        _ = f()
        return None
    except Exception as err:
        traceback.print_exception(err)
        return err


def get_tied_targets(task: Task) -> Iterable[tuple[str, ...]]:
    if not task.tied_targets:
        return tuple()
    file_pattern = re.compile(task.tied_targets[1])
    sub_pattern = re.compile(task.tied_targets[2])
    return (
            tuple(sub_pattern.sub(r, f) for l in ((task.tied_targets[5],), task.tied_targets[4]) for r in l)
            for p in pathlib.Path('').glob(task.tied_targets[0])
            if (f := str(p)) and file_pattern.search(f)
            )


def expand_requirements(task: Task) -> Iterable[str]:
    return itertools.chain(
            expand_globs(task.required_files),
            (f for t in task.required_tasks for f in expand_globs(t.targets)),
            (f[0] for t in task.required_tasks for f in get_tied_targets(t)),
            (f for t in get_tied_targets(task) for f in t[1:]),
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
            '{prev}{{cur}}'.format(prev='\n'.join(s for _, s in prev_runs)),
            verbose,
            ):
        case (step, template, _) if not reason_to_run:
            return (step, template.format(cur=''))
        case (step, template, False):
            return (
                    step,
                    template.format(
                        cur=f'\n{step}) {task.name} ({reason_to_run})',
                        ),
                    )
        case (step, template, True):
            return (
                    step,
                    template.format(
                        cur=(
                            '\n'
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
            return _format_dry_run(prev_runs, task, '', verbose=verbose)
        case _ as unreachable:
            assert_never(unreachable)

    if is_queued(task):
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
    return err if err else start_state_machine(task)


def start_state_machine(task: Task) -> Optional[Exception]:
    if should_not_run(task):
        return None
    if should_run(task) != RunReason.DONT_RUN:
        return start_run(task)
    if is_queued(task):
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
    if any(not os.path.exists(f[0]) for f in get_tied_targets(task)):
        return RunReason.MISSING_TARGETS
    if task.cache_type == CacheType.NONE:
        return RunReason.NO_CACHE
    return RunReason.DONT_RUN


def remove_target_state(task: Task) -> Optional[Exception]:
    return remove_files((
        os.path.join('.maid', 'targets', _get_encoded_name(task.name)),
        ))


def start_run(task: Task) -> Optional[Exception]:
    if (err := setup_file_states(task)):
        remove_target_state(task)
        traceback.print_exception(err)
        return err
    return start_execution(task)


def dequeue_files(filenames: Iterable[str], task: Task) -> Optional[Exception]:
    return try_function(
            lambda: find_error(os.remove(to_state_file(task, f)) for f in filenames),
            )


def queue_files(filenames: Iterable[str], task: Task) -> Optional[Exception]:
    return try_function(
            lambda: find_error((_touch_file(f, task) for f in filenames))
            )


def _touch_file(filename: str, task: Task) -> Optional[Exception]:
    if task and os.path.exists(to_state_file(task, filename)):
        raise DuplicateFileException(filename, task)
    pathlib.Path(to_state_file(task, filename)).touch()
    return None


def queue_targets(task: Task) -> Optional[Exception]:
    if task.tied_targets:
        return None
    if task.grouped:
        return None
    return queue_files((f for f in expand_globs(task.targets)), task)


def add_tied_if_missing(tt: Sequence[str], task: Task) -> bool:
    if os.path.exists(tt[0]):
        return False
    if os.path.exists(state_file := to_state_file(task, tt[0])):
        raise DuplicateFileException(tt[0], task)
    with open(state_file, mode='wt') as fos:
        fos.writelines('\n{required}'.format(required='\t'.join(tt[1:])))
    return True


def diff_tied_target(tt: Sequence[str], diff_mmap: mmap.mmap, task: Task) -> bool:
    if os.path.exists(state_file := to_state_file(task, tt[0])):
        raise DuplicateFileException(tt[0], task)
    match '\t'.join(
            r
            for r in tt[1:]
            if maid.utils.setops.is_in(r.encode('utf-8'), diff_mmap)
            ):
        case '':
            return False
        case modified:
            with open(state_file, mode='wt') as fos:
                fos.writelines('{modified}\n{required}'.format(
                    modified=modified,
                    required='\t'.join(tt[1:]),
                    ))
            return True


def queue_tied_targets(task: Task) -> Optional[Exception]:
    if not task.tied_targets:
        return None
    if not os.path.exists(diff_file := to_state_file(task, suffix='.diff')):
        raise RuntimeError('Precondition not met for `expand_tied_targets`.')
    if os.path.getsize(diff_file) < 1:
        return try_function(
                lambda: find_error(
                    tt[0]
                    for tt in get_tied_targets(task)
                    if add_tied_if_missing(tt, task)
                    ))
    with open(diff_file) as diff_fis:
        diff_mmap = mmap.mmap(diff_fis.fileno(), 0, access=mmap.ACCESS_READ)
        return try_function(
                lambda: find_error(
                    tt[0]
                    for tt in get_tied_targets(task)
                    if add_tied_if_missing(tt, task) or diff_tied_target(tt, diff_mmap, task)
                    ))


def open_state(task: Task, suffix: str = '', mode: str = 'rt') -> IO[str]:
    match mode, to_state_file(task, suffix=suffix):
        case ('rt', str(f)) if os.path.exists(f):
            return open(f, mode=mode)
        case ('wt', str(f)):
            return open(f, mode=mode)
    return io.StringIO()


def diff_task_state(task: Task) -> Optional[Exception]:
    if not task.name:
        return Exception('Cannot diff anonymous tasks.')
    if not os.path.exists(to_state_file(task, suffix='.new')):
        return Exception('Preconditions not met for `diff_task_state`.')
    with (
            open_state(task) as old_fis,
            open_state(task, suffix='.new') as new_fis,
            open_state(task, suffix='.diff', mode='wt') as diff_fos,
            ):
        # Sorting could be costly.
        return try_function(
                lambda: diff_fos.writelines(sorted(
                    x[(x.find(' ') + 1):]
                    for x in maid.utils.setops.difference(new_fis, old_fis)
                    )))


def to_state_file(task: Task, target: str = '', suffix: str = '') -> str:
    if target:
        return _filename_to_state_file(
                target,
                os.path.join('targets', _get_encoded_name(task.name))
                )
    return _filename_to_state_file(task.name + suffix, 'tasks')


def _get_encoded_name(filename: str) -> str:
    return base64.b64encode(filename.encode('utf-8')).decode('utf-8')


def _filename_to_state_file(filename: str, dirname: str) -> str:
    return os.path.join('.maid', dirname, _get_encoded_name(filename))


def setup_target_state(task: Task) -> Optional[Exception]:
    return try_function(
            lambda: os.makedirs(
                os.path.join('.maid', 'targets', _get_encoded_name(task.name))
                ))


def setup_file_states(task: Task) -> Optional[Exception]:
    return find_error((
        remove_target_state(task),
        setup_target_state(task),
        diff_task_state(task),
        queue_targets(task),
        queue_tied_targets(task),
        try_function(lambda: shutil.copy(
            to_state_file(task, suffix='.new'),
            to_state_file(task),
            )),
        ))


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


def cache_files(
        filenames: Iterable[str],
        outfile: str,
        cache_type: CacheType,
        ) -> str:
    if not outfile:
        return ''
    with open(outfile, mode='wt', encoding='utf-8') as fos:
        # Sorting could be costly.
        fos.writelines(
                sorted(
                    '{h} {f}\n'.format(h=hash_file(f, cache_type), f=f)
                    for f in filenames
                    )
                )
    return hash_file(outfile, cache_type)


def _pexec(args: tuple[Task, str]) -> Optional[Exception]:
    task, target = args[0], args[1]
    return execute(task, target)


executor = concurrent.futures.ProcessPoolExecutor(max_workers=2)
#executor = multiprocessing.Pool(processes=2)
def start_execution(task: Task, parallel: bool = False) -> Optional[Exception]:
    if task.grouped:
        return cleanup_states((execute(task),), task)

    # The cast is needed for mypy.  It complains about `object not callable`
    # when using the ternary expression.
    apply = cast(ExecuteMapCallable, executor.map if parallel else map)
    return cleanup_states(
            apply(_pexec, ((task, t) for t in _get_targets_to_execute(task))),
            task,
            )


def _get_targets_to_execute(task: Task) -> Iterable[str]:
    return (
            f
            for t in (
                expand_globs(task.targets),
                (tt[0] for tt in get_tied_targets(task)),
                )
            for f in t
            if is_queued(task, f)
            )


def execute(task: Task, target: str = '') -> Optional[Exception]:
    return cleanup_state(
            handle_error(
                try_function(lambda: task.run_recipe(target)),
                task,
                target,
                ),
            target,
            task,
            )


def is_queued(task: Task, filename: str = '') -> bool:
    if not filename:
        return os.path.exists(os.path.join('.maid', 'targets', _get_encoded_name(task.name)))
    return os.path.exists(to_state_file(task, filename))


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
    filenames = list(filenames)
    return find_error(
            os.remove(f) if os.path.isfile(f) else shutil.rmtree(f)
            for f in filenames
            if f and (os.path.isfile(f) or os.path.isdir(f))
            )


def expand_all_targets(task: Task) -> Iterable[str]:
    return (
            f
            for x in (
                (t[0] for t in get_tied_targets(task)),
                expand_globs(task.targets),
                )
            for f in x
            )


def handle_error(
        error: Optional[Exception],
        task: Task,
        target: str = '',
        ) -> Optional[Exception]:
    if target:
        if not os.path.exists(target):
            return MissingTargetException(target, task)
    elif (f := next((f for f in expand_all_targets(task) if not os.path.exists(f)), '')):
        return MissingTargetException(f, task)
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
        task: Task,
        ) -> Optional[Exception]:
    if not filename:
        return error
    return error if error else dequeue_files((filename,), task)


def cleanup_states(
        errors: Iterable[Optional[Exception]],
        task: Task,
        ) -> Optional[Exception]:
    return find_error((
        x
        for e in (
            (find_error(errors),),
            (remove_target_state(task),),
            (remove_files((to_state_file(task, suffix='.diff'),)),),
            )
        for x in e
        ))


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


class MissingTargetException(Exception):
    '''
    '''

    def __init__(self, filename: str, task: Task):
        '''
        '''
        msg = 'Target `{}` for task `{}` was not created.'.format(
                filename,
                task.name,
                )
        super().__init__(msg)


class InvalidTargetsError(Exception):
    '''
    '''

    def __init__(self, task: Task):
        '''
        '''
        msg = 'Invalid initialization of task `{}`.  Only one of `targets` and `tied_targets` can be nonempty.'.format(task.name)
        super().__init__(msg)


class DuplicateFileException(Exception):
    '''
    '''

    def __init__(self, filename: str, task: Task):
        '''
        '''
        msg = 'Duplicate file detected: task `{}` contains multiple instances of file `{}` (likely a target).'.format(
                task.name,
                filename,
                )
        super().__init__(msg)
