import itertools
from typing import Optional, Iterable, Final

import maid2.tasks

DEFAULT_MAID_NAME: Final[str] = 'm03'
_maids: Final[dict[str, '_Maid']] = dict()


class _Maid:
    '''
    '''

    def __init__(self, name: str):
        self.name: Final[str] = name
        self._default_task: Optional[maid2.tasks.Task] = None
        self._task_graph: Final[dict[
                maid2.tasks.RunPhase,
                dict[str, maid2.tasks.Task]
                ]] = {
                        maid2.tasks.RunPhase.NORMAL: dict(),
                        maid2.tasks.RunPhase.START: dict(),
                        maid2.tasks.RunPhase.END: dict(),
                        maid2.tasks.RunPhase.FINALLY: dict(),
                        }

    def dry_run(self, task_name: str = '', verbose: bool = False) -> str:
        return '\n'.join(
                t.dry_run() for t in itertools.chain(
                    self._task_graph[maid2.tasks.RunPhase.START].values(),
                    (self._get_task(task_name),),
                    self._task_graph[maid2.tasks.RunPhase.END].values(),
                    self._task_graph[maid2.tasks.RunPhase.FINALLY].values(),
                    )
                )

    def _try_run(
            self,
            tasks: Iterable[maid2.tasks.Task],
            ) -> Optional[Exception]:
        return next(
                (
                    out
                    for t in tasks
                    if (out := t.run()) and isinstance(out, Exception)
                    ),
                None,
                )

    def run(self, task_name: str = '') -> Optional[Exception]:
        try:
            return self._try_run(
                    itertools.chain(
                        self._task_graph[maid2.tasks.RunPhase.START].values(),
                        (self._get_task(task_name),),
                        self._task_graph[maid2.tasks.RunPhase.END].values(),
                        )
                    )
        except Exception as err:
            return err
        finally:
            if (e := self._try_run(self._task_graph[maid2.tasks.RunPhase.FINALLY].values())):
                raise e

    def _get_task(self, task_name: str) -> maid2.tasks.Task:
        if task_name in self._task_graph[maid2.tasks.RunPhase.NORMAL]:
            return self._task_graph[maid2.tasks.RunPhase.NORMAL][task_name]
        if task_name:
            raise UnknownTaskException(task_name, self.name)
        if self._default_task:
            return self._default_task
        raise UnknownTaskException(task_name, self.name)

    def add_task(self, task: maid2.tasks.Task) -> bool:
        '''
        Add a task.

        If the task name is empty, it will not be added.
        '''
        if not task.name:
            raise InvalidTaskNameException()
        if any(task.name in tasks for tasks in self._task_graph.values()):
            raise DuplicateTaskException(task, self.name)
        if task.run_phase not in self._task_graph:
            raise UnknownRunPhaseException(task)
        if task.is_default:
            if task.run_phase != maid2.tasks.RunPhase.NORMAL:
                raise DefaultTaskException(task, self.name)
            if self._default_task:
                raise DefaultTaskException(
                        task,
                        self.name,
                        self._default_task.name,
                        )
            self._default_task = task

        self._task_graph[task.run_phase][task.name] = task

        return True


def get_maid(maid_name: str = DEFAULT_MAID_NAME) -> _Maid:
    if not maid_name:
        raise MaidNameException()
    return _maids.setdefault(maid_name, _Maid(maid_name))


class UnknownRunPhaseException(Exception):

    def __init__(self, task: maid2.tasks.Task):
        '''
        '''
        msg = 'RunPhase `{}` is not supported.'.format(task.run_phase)
        super().__init__(msg)


class DuplicateTaskException(Exception):

    def __init__(self, task: maid2.tasks.Task, maid_name: str):
        '''
        '''
        msg = 'Maid `{}` already has task named `{}`.'.format(
                    maid_name,
                    task.name,
                    )
        super().__init__(msg)


class InvalidTaskNameException(Exception):

    def __init__(self) -> None:
        '''
        '''
        msg = 'Task name must be non-empty.'
        super().__init__(msg)


class DefaultTaskException(Exception):

    def __init__(
            self,
            task: maid2.tasks.Task,
            maid_name: str,
            default_name: str = '',
            ):
        '''
        '''
        msg = ''
        if default_name:
            msg = 'Error setting task `{}` as the default.  Maid `{}` already has default task `{}`.'.format(
                    task.name,
                    maid_name,
                    default_name,
                    )
        else:
            msg = 'Error setting task `{}` as the default.  Only RunPhase.NORMAL tasks can be set as the default.'.format(task.name)
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

    def __init__(self) -> None:
        '''
        '''
        msg = 'Maid name must not be empty.'
        super().__init__(msg)
