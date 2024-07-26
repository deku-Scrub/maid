import itertools
import enum
import os
import sys

import maid.exceptions
import maid.tasks
import maid.monitor.hash
import maid.monitor.time
import maid.composition

DEFAULT_MAID_NAME = 'm0'
maids = dict()


def get_maid(maid_name=DEFAULT_MAID_NAME):
    if not maid_name:
        raise maid.exceptions.MaidNameException()
    return maids.setdefault(maid_name, _Maid(maid_name))


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
        outputs = map(Task.run, tasks)
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
            case Task(name=''):
                return False
            case Task(name=x) if x in self._tasks:
                raise maid.exceptions.DuplicateTaskException(task)
            case Task(is_default=True) if self._default_task:
                raise maid.exceptions.DuplicateTaskException(task)
            case Task(is_default=True, run_phase=x) if x != RunPhase.NORMAL:
                raise maid.exceptions.DefaultTaskRunPhaseException(task)
            case Task(is_default=True):
                self._default_task = task

        match task.run_phase:
            case RunPhase.NORMAL:
                self._tasks[task.name] = task
            case RunPhase.START:
                self._start_tasks[task.name] = task
            case RunPhase.END:
                self._end_tasks[task.name] = task
            case RunPhase.FINALLY:
                self._finally_tasks[task.name] = task

        return True


def update_files(cache, filenames):
    if cache == CacheType.TIME:
        maid.monitor.time.touch_files(maid.tasks.get_filenames(filenames))
    elif cache == CacheType.HASH:
        maid.monitor.hash.make_hashes(maid.tasks.get_filenames(filenames))


def _any_files_missing(filenames, must_exist=True):
    filenames = maid.tasks.get_filenames(filenames, must_exist=must_exist)
    return next((f for f in filenames if not os.path.exists(f)), '')


class RunPhase(enum.Enum):
    NORMAL = 0
    START = 1
    END = 2
    FINALLY = 3


class CacheType(enum.Enum):
    NONE = 0
    HASH = 1
    TIME = 2


class Task:

    _visited = set()

    def __init__(
            self,
            name='', # o
            *,
            # Maid exclusive.
            maid_name=DEFAULT_MAID_NAME, # o
            run_phase=RunPhase.NORMAL, # o
            is_default=False, # o
            # Task exclusive.
            inputs=None, # o
            required_tasks=None, # o
            required_files=None, # o
            targets=None, # o
            cache=CacheType.NONE, # o
            independent_targets_creator=None,
            script_stream=None, # o
            output_stream=None, # o
            delete_targets_on_error=True, #o
            dont_run_if_all_targets_exist=False, # o
            description='',
            finish_depth_on_failure=False, # o
            update_requested=False, # o
            ):
        self.name = name

        rp = required_tasks if required_tasks else dict()
        self.required_tasks = {t().name: t() for t in rp}

        self._task_cacher = TaskCacher(self)
        self._simple_task = maid.composition.SimpleTask(
                inputs=inputs,
                script_stream=script_stream,
                output_stream=output_stream,
                )
        self.required_files = tuple(required_files) if required_files else tuple()
        self.targets = tuple(targets) if targets else tuple()
        self._outfile = ''
        self._mode = ''
        self._finish_depth_on_failure = finish_depth_on_failure
        self.dont_run_if_all_targets_exist = dont_run_if_all_targets_exist
        self.cache = cache
        self.update_requested = update_requested
        self._delete_targets_on_error = delete_targets_on_error
        self.maid_name = maid_name
        self.is_default = is_default
        self.run_phase = run_phase

        self._get_independent_task = None
        if independent_targets_creator:
            def f(target):
                # Makes several assumptions:
                #   * the empty `name` prevents querying maid.
                #   * the lack of `required_tasks` skips running
                #     of dependencies.
                a = Task(
                    name='',
                    inputs=inputs,
                    required_files=required_files,
                    targets=[target],
                    cache=cache,
                    independent_targets_creator=None,
                    script_stream=script_stream,
                    output_stream=output_stream,
                    delete_targets_on_error=delete_targets_on_error,
                    dont_run_if_all_targets_exist=dont_run_if_all_targets_exist,
                    finish_depth_on_failure=finish_depth_on_failure,
                    update_requested=update_requested,
                )
                independent_targets_creator(a)
                return a
            self._get_independent_task = f

        _ = get_maid(maid_name=maid_name).add_task(self)

    def __gt__(self, rhs):
        '''
        Write to file given by `rhs`.

        The file is truncated first.
        '''
        self._simple_task.write_to_file(rhs)
        return self

    def __rshift__(self, rhs):
        '''
        Append to file given by `rhs`.

        Note that due to Python's precedence rules, this takes
        precedence over `|` and results in an error unless everything
        before the `>>` is wrapped in parentheses.
        '''
        self._simple_task.append_to_file(rhs)
        return self

    def __or__(self, rhs):
        '''
        Add `rhs`'s command to this object's command list.
        '''
        self._simple_task.append(rhs)
        return self

    def __str__(self):
        '''
        Return a string representation of this object's commands.
        '''
        return str(self._simple_task)

    def _wrap_visited(f, task_name):
        '''
        Reset visited after running f.
        '''
        is_root = not Task._visited
        Task._visited.add(task_name)
        try:
            return f()
        except Exception as err:
            raise err
        finally:
            # Clear visited list once the task has finished
            # so that other pipelines can run correctly.
            if is_root:
                Task._visited.clear()

    def _get_required_dry_runs(tasks, verbose):
        return lambda: '\n'.join(
            t.dry_run(verbose)
            for t in tasks
            if t.name not in Task._visited
            )

    def dry_run(self, verbose=False):
        '''
        Return a string containing all steps that a call to `run`
        would execute.
        '''
        # This goes before anything below it because `_should_run`
        # depends on the traversal's output.
        output = Task._wrap_visited(
                Task._get_required_dry_runs(
                    self.required_tasks.values(),
                    verbose,
                    ),
                self.name,
                )

        # Won't run so nothing to show.
        if (isit := self._task_cacher.is_up_to_date()) and isit[0]:
            return ''

        if not verbose:
            return '{}\n{} ({})'.format(output, self.name, isit[1])

        return '''
        \r{previous_tasks}
        \r########################################
        \r# Task `{task_name}` will run due to {run_reason}
        \r{recipe}
        \r########################################
        '''.format(
                previous_tasks=output,
                task_name=self.name,
                run_reason=isit[1],
                recipe=str(self),
                )
        return output

    def run(self):
        '''
        Run task.
        '''
        return Task._wrap_visited(self._run, self.name)

    def _throw_if_any_fail(f, iterable, *, delay_throw=False):
        error = None
        for val in iterable:
            try:
                f(val)
            except Exception as err:
                # Save first error and continue if so specified.
                error = error if error else err
                if not delay_throw:
                    raise error

        # Raise error, if any, once the depth is complete.
        if error:
            raise error

    def _prerun(self):
        '''
        Run functions that the task requires to have finished.
        '''
        Task._run_dependencies(
                self.required_tasks.values(),
                delay_throw=self._finish_depth_on_failure,
                )
        if self._task_cacher.is_up_to_date()[0]:
            return True, tuple()
        if self.update_requested:
            self._task_cacher.cache()
            return True, tuple()
        return False, ''

    def _main_run(self):
        '''
        Logic for running the task.
        '''
        # TODO: When independent targets is enabled, the task
        # probably should end in `>`.  Doesn't make sense to have
        # each produce non-empty output.  Which task is the one
        # to use as the aggregate output?  Should they be zipped?
        # Can't guarantee that they're all the same length.  Too
        # many problems.  Either end in `>` or ignore output.
        if self._get_independent_task:
            Task._throw_if_any_fail(
                    lambda f: self._get_independent_task(f).run(),
                    maid.tasks.get_filenames(self.targets),
                    delay_throw=self._finish_depth_on_failure,
                    )
            return tuple()
        else:
            return self._simple_task.run()

    def _run(self):
        '''
        Execute the pre-, main-, and post-run stages.
        '''
        try:
            if (stop_early := self._prerun()) and stop_early[0]:
                return stop_early[1]
            outputs = self._main_run()
            # The post-run step has already been done by the
            # independent tasks.  Doing it again would cause
            # errors, particularly overwriting files.
            if not self._get_independent_task:
                self._postrun(outputs)
            return outputs
        except Exception as err:
            msg = 'Error running task `{}`: {}'.format(self.name, err)
            maid.error_utils.remove_files_and_throw(
                    maid.tasks.get_filenames(self.targets) if self._delete_targets_on_error else [],
                    Exception(msg),
                    )

    def _postrun(self, outputs):
        '''
        Run functions that require the task to have finished.
        '''
        if (f := _any_files_missing(self.targets)):
            raise maid.exceptions.MissingTargetException(task, f)

        self._task_cacher.cache()

    def _run_dependencies(tasks, *, delay_throw=False):
        # Checking that `p.name not in Task._visited` prevents reruning
        # pipelines that have already run.
        Task._throw_if_any_fail(
                Task.run,
                (t for t in tasks if t.name not in Task._visited),
                delay_throw=delay_throw,
                )


class TaskCacher:

    def __init__(self, task):
        self._task = task

    def cache(self):
        '''
        '''
        update_files(self._task.cache, self._task.targets)
        update_files(self._task.cache, self._task.required_files)

    def is_up_to_date(self):
        '''
        '''
        # Checks based on file existance.
        if (f := _any_files_missing(self._task.targets, must_exist=False)):
            return False, f'missing target `{f}`'
        if self._task.dont_run_if_all_targets_exist:
            return True, ''

        # Checks based on cache type.
        if self._task.cache == CacheType.NONE:
            return False, 'uncached task'
        return self._is_cached()

    def _is_cached(self):
        # Get appropriate decision function.
        should_task_run = maid.monitor.hash.should_task_run
        if self._task.cache == CacheType.TIME:
            should_task_run = maid.monitor.time.should_task_run

        if should_task_run(self._get_graph())(self._task.name):
            return False, 'targets out of date'
        return True, ''

    def _get_graph(self):
        '''
        '''
        task = self._task

        # This is the graph required for the time and hash cache
        # decision functions.
        graph = {
            p.name: maid.tasks.Task(
                    p.name,
                    lambda a: a, # This doesn't matter; never runs.
                    targets=p.targets,
                    )
            for p in task.required_tasks.values()
        }
        graph[task.name] = maid.tasks.Task(
                    task.name,
                    lambda a: a, # This doesn't matter; never runs.
                    targets=task.targets,
                    required_files=task.required_files,
                    required_tasks=tuple(
                        [p.name for p in task.required_tasks.values()]
                        ),
                    )
        return graph


def task(
        name,
        inputs=None,
        required_files=tuple(),
        required_tasks=tuple(),
        targets=tuple(),
        cache=CacheType.NONE,
        output_stream=sys.stdout,
        script_stream=sys.stderr,
        independent_targets=False,
        is_default=False,
        ):

    def build_task(define_commands):
        t = Task(
                name,
                inputs=inputs,
                required_files=required_files,
                targets=targets,
                cache=cache,
                is_default=is_default,
                independent_targets_creator=define_commands if independent_targets else None,
                required_tasks=required_tasks,
                output_stream=output_stream,
                script_stream=script_stream,
                )
        # Let `define_commands` immediately create commands for the task.
        define_commands(t)
        return lambda: t

    return build_task

@task(
    'p1',
    inputs=['lol\n', '.lol\n'],
    required_files=['requirements.txt'],
    targets=['a.txt', 'b.txt'],
    cache=CacheType.HASH,
    script_stream=sys.stdout,
    independent_targets=True,
)
def h(a):
    a \
    | "sed 's/lol/md/'" \
    | "grep .md" \
    | (lambda x: x.strip()+'?') \
    | (lambda x: x.strip()+'m') \
    | "tr 'm' '!'" \
    > a.targets[0]

@task(
    'p2',
    required_tasks=[h],
    output_stream=sys.stdout,
    script_stream=sys.stderr,
    is_default=True,
)
def h2(a):
    a | f"cat {a.required_tasks['p1'].targets[0]}"

print(get_maid().dry_run(verbose=True), file=sys.stderr)
sys.stdout.writelines(get_maid().run())

a = Task(inputs=(j for j in range(100))) \
    | (filter, lambda x: x % 3 == 0) \
    | 'parallel {args} "echo paraLOL; echo {{}}"'.format(args='--bar') \
    | 'grep -i lol' \
    | len \
    | 'wc -l'
# can probably use joblib as a step to parallelize python code in
# the same way gnu parallel can be a step to paralellize shell code:
# ```
#  | (lambda x: joblib.Parallel()(joblib.delayed(f)(xj) for xj in x),)
# ```
print(a.dry_run(True))
print('task output: {}'.format(list(a.run())))

# example from https://github.com/pytoolz/toolz
import collections
import itertools
stem = lambda x: [w.lower().rstrip(",.!:;'-\"").lstrip("'\"") for w in x]
flatten = lambda x: (col for row in x for col in row)
counter = collections.Counter()
a = Task(inputs=['this cat jumped over this other cat!']) \
    | str.split \
    | stem \
    | counter.update
_ = list(a.run())
print(counter)
