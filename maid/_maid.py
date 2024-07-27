import itertools
import sys
from typing import Iterable, Final, IO, Callable

import maid.cache
import maid.composition

DEFAULT_MAID_NAME: str = 'm0'
_maids: dict = dict()


class _Maid:
    '''
    '''

    def __init__(self, name: str):
        self.name: Final[str] = name
        self._default_task: maid.composition.Task = None
        self._tasks: dict[str, maid.composition.Task] = dict()
        self._start_tasks: dict[str, maid.composition.Task] = dict()
        self._end_tasks: dict[str, maid.composition.Task] = dict()
        self._finally_tasks: dict[str, maid.composition.Task] = dict()

    def dry_run(
            self,
            task_name: str = '',
            verbose: bool = False,
            ) -> str:
        tasks = itertools.chain(
                self._start_tasks.values(),
                (self._get_task(task_name),),
                self._end_tasks.values(),
                self._finally_tasks.values(),
                )
        return '\n'.join(map(lambda t: t.dry_run(verbose), tasks))

    def _run[T](
            self,
            tasks: Iterable[maid.composition.Task],
            capture_outputs: bool = True,
            ) -> Iterable[T]:
        outputs = map(maid.composition.Task.run, tasks)
        empty_iter = filter(lambda _: False, outputs)
        return outputs if capture_outputs else list(empty_iter)

    def run[T](self, task_name: str = '') -> Iterable[T]:
        try:
            _ = self._run(self._start_tasks.values(), capture_outputs=False)
            return next(self._run([self._get_task(task_name)]))
        except Exception as err:
            raise err
        finally:
            _ = self._run(self._finally_tasks.values(), capture_outputs=False)

    def _get_task(self, task_name: str) -> maid.composition.Task:
        if task_name in self._tasks:
            return self._tasks[task_name]
        if self._default_task:
            return self._default_task
        raise UnknownTaskException(task)

    def add_task(self, task: maid.composition.Task) -> bool:
        '''
        Add a task.

        If the task name is empty, it will not be added.
        '''
        match task:
            case maid.composition.Task(name=''):
                return False
            case maid.composition.Task(name=x) if x in self._tasks:
                raise DuplicateTaskException(task)
            case maid.composition.Task(is_default=True) if self._default_task:
                raise DuplicateTaskException(task)
            case maid.composition.Task(is_default=True, run_phase=x) if x != maid.RunPhase.NORMAL:
                raise DefaultTaskRunPhaseException(task)
            case maid.composition.Task(is_default=True):
                self._default_task = task

        match task.run_phase:
            case maid.RunPhase.NORMAL:
                self._tasks[task.name] = task
            case maid.RunPhase.START:
                self._start_tasks[task.name] = task
            case maid.RunPhase.END:
                self._end_tasks[task.name] = task
            case maid.RunPhase.FINALLY:
                self._finally_tasks[task.name] = task

        return True


def task[T](
        name: str,
        maid_name: str = DEFAULT_MAID_NAME,
        inputs: Iterable[T] = None,
        required_files: Iterable = tuple(),
        required_tasks: Iterable = tuple(),
        targets: tuple = tuple(),
        cache: maid.cache.CacheType = maid.cache.CacheType.NONE,
        output_stream: IO = sys.stdout,
        script_stream: IO = sys.stderr,
        independent_targets: bool = False,
        is_default: bool = False,
        ) -> Callable[
            [Callable[[maid.composition.Task], None]],
            Callable[[], maid.composition.Task]
        ]:

    def build_task(
            define_commands: Callable[[maid.composition.Task], None]
            ) -> Callable[[], maid.composition.Task]:
        t = maid.composition.Task(
                name,
                maid_name=maid_name,
                inputs=inputs,
                required_files=required_files,
                targets=targets,
                cache=cache,
                is_default=is_default,
                build_task=define_commands if independent_targets else None,
                required_tasks=required_tasks,
                output_stream=output_stream,
                script_stream=script_stream,
                )
        # Let `define_commands` immediately create commands for the task.
        define_commands(t)
        _ = get_maid(maid_name=maid_name).add_task(t)

        return lambda: t

    return build_task


def get_maid(maid_name: str = DEFAULT_MAID_NAME) -> _Maid:
    if not maid_name:
        raise MaidNameException()
    return _maids.setdefault(maid_name, _Maid(maid_name))


class DefaultTaskRunPhaseException(Exception):

    def __init__(self, task: maid.composition.Task):
        '''
        '''
        msg = 'Only pipelines in the `NORMAL` run phase can be a default; was given `{}` for task `{}`.'.format(
            task.run_phase,
            task.name,
            )
        super().__init__(msg)


class DuplicateTaskException(Exception):

    def __init__(self, task: maid.composition.Task):
        '''
        '''
        msg = 'Maid `{}` already has task named `{}`.'.format(
                    task.maid_name,
                    task.name,
                    )
        super().__init__(msg)


class UnknownTaskException(Exception):

    def __init__(self, task: maid.composition.Task):
        '''
        '''
        msg = 'Unknown task.  Maid `{}` has no task named `{}`'.format(
                    task.maid_name,
                    task.name,
                    )
        super().__init__(msg)


class MaidNameException(Exception):

    def __init__(self):
        '''
        '''
        msg = 'Maid name must not be empty.'
        super().__init__(msg)
