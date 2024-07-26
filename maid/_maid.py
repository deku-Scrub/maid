import itertools

import maid

DEFAULT_MAID_NAME = 'm0'
_maids = dict()


def get_maid(maid_name=DEFAULT_MAID_NAME):
    if not maid_name:
        raise maid.exceptions.MaidNameException()
    return _maids.setdefault(maid_name, _Maid(maid_name))


class _Maid:
    '''
    '''

    def __init__(self, name):
        self.name = name
        self._default_task = None
        self._tasks = dict()
        self._start_tasks = dict()
        self._end_tasks = dict()
        self._finally_tasks = dict()

    def dry_run(self, task_name='', verbose=False):
        tasks = itertools.chain(
                self._start_tasks.values(),
                (self._get_task(task_name),),
                self._end_tasks.values(),
                self._finally_tasks.values(),
                )
        return '\n'.join(map(lambda t: t.dry_run(verbose), tasks))

    def _run(self, tasks, capture_outputs=True):
        outputs = map(maid.Task.run, tasks)
        empty_iter = filter(lambda _: False, outputs)
        return outputs if capture_outputs else list(empty_iter)

    def run(self, task_name=''):
        try:
            _ = self._run(self._start_tasks.values(), capture_outputs=False)
            return next(self._run([self._get_task(task_name)]))
        except Exception as err:
            raise err
        finally:
            _ = self._run(self._finally_tasks.values(), capture_outputs=False)

    def _get_task(self, task_name):
        if task_name in self._tasks:
            return self._tasks[task_name]
        if self._default_task:
            return self._default_task
        raise maid.exceptions.UnknownTaskException(task)

    def add_task(self, task):
        '''
        Add a task.

        If the task name is empty, it will not be added.
        '''
        match task:
            case maid.Task(name=''):
                return False
            case maid.Task(name=x) if x in self._tasks:
                raise maid.exceptions.DuplicateTaskException(task)
            case maid.Task(is_default=True) if self._default_task:
                raise maid.exceptions.DuplicateTaskException(task)
            case maid.Task(is_default=True, run_phase=x) if x != maid.RunPhase.NORMAL:
                raise maid.exceptions.DefaultTaskRunPhaseException(task)
            case maid.Task(is_default=True):
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

