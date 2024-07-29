import itertools
from typing import Iterable, Final, Iterator

import maid.compose.tasks

DEFAULT_MAID_NAME: str = 'm0'
_maids: dict = dict()


class _Maid:
    '''
    '''

    def __init__(self, name: str):
        self.name: Final[str] = name
        self._default_task: maid.compose.tasks.Task | None = None
        self._tasks: dict[str, maid.compose.tasks.Task] = dict()
        self._start_tasks: dict[str, maid.compose.tasks.Task] = dict()
        self._end_tasks: dict[str, maid.compose.tasks.Task] = dict()
        self._finally_tasks: dict[str, maid.compose.tasks.Task] = dict()

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
            tasks: Iterable[maid.compose.tasks.Task],
            capture_outputs: bool = True,
            ) -> Iterator[Iterable[T]]:
        outputs: Iterator[Iterable[T]] = map(maid.compose.tasks.Task.run, tasks)
        empty_iter: Iterator[Iterable[T]] = filter(lambda _: False, outputs)
        return outputs if capture_outputs else iter(list(empty_iter))

    def run[T](self, task_name: str = '') -> Iterable[T]:
        try:
            _ = self._run(self._start_tasks.values(), capture_outputs=False)
            return next(self._run([self._get_task(task_name)]))
        except Exception as err:
            raise err
        finally:
            _ = self._run(self._finally_tasks.values(), capture_outputs=False)

    def _get_task(self, task_name: str) -> maid.compose.tasks.Task:
        if task_name in self._tasks:
            return self._tasks[task_name]
        if self._default_task:
            return self._default_task
        raise UnknownTaskException(task_name, self.name)

    def add_task(self, task: maid.compose.tasks.Task) -> bool:
        '''
        Add a task.

        If the task name is empty, it will not be added.
        '''
        match task:
            case maid.compose.tasks.Task(name=''):
                return False
            case maid.compose.tasks.Task(name=x) if x in self._tasks:
                raise DuplicateTaskException(task)
            case maid.compose.tasks.Task(is_default=True) if self._default_task:
                raise DefaultTaskException(task, self._default_task.name)
            case maid.compose.tasks.Task(is_default=True, run_phase=x) if x != maid.RunPhase.NORMAL:
                raise DefaultTaskRunPhaseException(task)
            case maid.compose.tasks.Task(is_default=True):
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


def get_maid(maid_name: str = DEFAULT_MAID_NAME) -> _Maid:
    if not maid_name:
        raise MaidNameException()
    return _maids.setdefault(maid_name, _Maid(maid_name))


class DefaultTaskRunPhaseException(Exception):

    def __init__(self, task: maid.compose.tasks.Task):
        '''
        '''
        msg = 'Only pipelines in the `NORMAL` run phase can be a default; was given `{}` for task `{}`.'.format(
            task.run_phase,
            task.name,
            )
        super().__init__(msg)


class DuplicateTaskException(Exception):

    def __init__(self, task: maid.compose.tasks.Task):
        '''
        '''
        msg = 'Maid `{}` already has task named `{}`.'.format(
                    task.maid_name,
                    task.name,
                    )
        super().__init__(msg)


class DefaultTaskException(Exception):

    def __init__(self, task: maid.compose.tasks.Task, default_name: str):
        '''
        '''
        msg = 'Error setting task `{}` as the default.  Maid `{}` already has default task `{}`.'.format(
                    default_name,
                    task.maid_name,
                    task.name,
                    )
        super().__init__(msg)


class UnknownTaskException(Exception):

    def __init__(self, task_name: str, maid_name: str):
        '''
        '''
        msg = 'Unknown task.  Maid `{}` has no task named `{}`'.format(
                    maid_name,
                    task_name,
                    )
        super().__init__(msg)


class MaidNameException(Exception):

    def __init__(self):
        '''
        '''
        msg = 'Maid name must not be empty.'
        super().__init__(msg)
