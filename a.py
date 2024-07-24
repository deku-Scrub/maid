import functools
import itertools
import enum
import os
import sys
import subprocess

import maid.exceptions
import maid.tasks
import maid.task_runner
import maid.monitor.hash
import maid.monitor.time

DEFAULT_MAID_NAME = 'm0'
maids = dict()


def get_maid(maid_name=DEFAULT_MAID_NAME):
    if not maid_name:
        raise maid.exceptions.MaidNameException()
    return maids.setdefault(maid_name, M(maid_name))


def _add_task(task):
    _maid = get_maid(task.maid_name)

    match task:
        case A(name=x) if not x:
            return False
        case A(is_default=True):
            _maid.default_task = task.name

    match task.run_phase:
        case RunPhase.NORMAL:
            _maid.tasks[task.name] = task
        case RunPhase.START:
            _maid.start_tasks[task.name] = task
        case RunPhase.END:
            _maid.end_tasks[task.name] = task
        case RunPhase.FINALLY:
            _maid.finally_tasks[task.name] = task

    return True


class Pipeline:
    '''
    '''

    def __init__(self, commands=None):
        self._commands = commands if commands else []

    def append(self, cmd):
        self._commands.append(cmd)

    def _make_process(self, cmd, stdin):
        '''
        Make process with the necessary common parameters.
        '''
        return subprocess.Popen(
                cmd,
                stdin=stdin,
                stdout=subprocess.PIPE,
                shell=True,
                text=True,
                )

    def __str__(self):
        return '\n'.join(self._commands)

    def _pipe_process_to_command(self, process, cmd):
        return process + [self._make_process(cmd, process[-1].stdout)]

    def __call__(self, inputs=None):
        inputs = inputs if inputs else tuple()

        # Hook up command outputs to inputs.
        processes = functools.reduce(
                self._pipe_process_to_command,
                self._commands[1:],
                [self._make_process(self._commands[0], subprocess.PIPE)],
                )

        # Write to first command.
        processes[0].stdin.writelines(str(i) + '\n' for i in inputs)
        processes[0].stdin.flush()
        processes[0].stdin.close()

        # Yield output of last command.
        yield from processes[-1].stdout


class M:
    '''
    '''

    def __init__(self, name):
        self.name = name
        self.default_task = ''
        self.tasks = dict()
        self.start_tasks = dict()
        self.end_tasks = dict()
        self.finally_tasks = dict()

    def get_task(self, name):
        return self.tasks[name]

    def dry_run(self, task_name='', verbose=False):
        tasks = itertools.chain(
                self.start_tasks.values(),
                (self._get_task(task_name),),
                self.end_tasks.values(),
                self.finally_tasks.values(),
                )
        return '\n'.join(map(lambda t: t.dry_run(verbose), tasks))

    def _run(self, tasks, capture_outputs=True):
        outputs = map(A.run, tasks)
        empty_iter = filter(lambda _: False, outputs)
        return outputs if capture_outputs else list(empty_iter)

    def run(self, task_name=''):
        try:
            _ = self._run(self.start_tasks.values(), capture_outputs=False)
            return next(self._run([self._get_task(task_name)]))
        except Exception as err:
            raise err
        finally:
            _ = self._run(self.finally_tasks.values(), capture_outputs=False)

    def _get_task(self, task_name):
        if task_name in self.tasks:
            return self.tasks[task_name]
        if self.default_task:
            return self.tasks[self.default_task]
        raise Exception('Unknown pipeline.  Maid `{}` has no pipeline named `{}`'.format(self.name, task_name))


def _make_hashes(cache, *files):
    if cache != CacheType.HASH:
        return
    for filenames in files:
        maid.monitor.hash.make_hashes(maid.tasks.get_filenames(filenames))


def _write_to_file(lines, filename, mode):
    if not filename:
        return False

    with open(filename, mode=mode) as fos:
        fos.writelines(lines)
    return True


def update_files(filenames):
    maid.monitor.time.touch_files(maid.tasks.get_filenames(filenames))
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


class A:

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
        self.inputs = tuple(inputs) if inputs else tuple()

        rp = required_tasks if required_tasks else dict()
        self.required_tasks = {p: get_maid(maid_name).get_task(p) for p in rp}

        self.required_files = tuple(required_files) if required_files else tuple()
        self.targets = tuple(targets) if targets else tuple()
        self._commands = []
        self._outfile = ''
        self._mode = ''
        self._finish_depth_on_failure = finish_depth_on_failure
        self._dont_run_if_all_targets_exist = dont_run_if_all_targets_exist
        self._cache = cache
        self._update_requested = update_requested
        self._delete_targets_on_error = delete_targets_on_error
        self._output_stream = output_stream
        self._script_stream = script_stream
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
                a = A(
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

        self._validate()
        _ = _add_task(self)

    def _validate(self):
        match self:
            case A(is_default=True, name=x) if not x:
                raise maid.exceptions.InvalidTaskNameException()
            case A(is_default=True, run_phase=x) if x != RunPhase.NORMAL:
                raise maid.exceptions.DefaultTaskRunPhaseException(self)
            case A(is_default=True) if get_maid(self.maid_name).default_task:
                raise maid.exceptions.DuplicateTaskException(self)
            case A(name=x) if x and (x in get_maid(self.maid_name).tasks):
                raise maid.exceptions.DuplicateTaskException(self)

    def __gt__(self, rhs):
        '''
        Write to file given by `rhs`.

        The file is truncated first.
        '''
        self._outfile = rhs
        self._mode = 'wt'
        return self

    def __rshift__(self, rhs):
        '''
        Append to file given by `rhs`.

        Note that due to Python's precedence rules, this takes
        precedence over `|` and results in an error unless everything
        before the `>>` is wrapped in parentheses.
        '''
        self._outfile = rhs
        self._mode = 'at'
        return self

    def __or__(self, rhs):
        '''
        Add `rhs`'s command to this object's command list.
        '''
        if isinstance(rhs, str):
            if (not self._commands) or (not isinstance(self._commands[-1], Pipeline)):
                self._commands.append(Pipeline())
            self._commands[-1].append(rhs)
        elif callable(rhs):
            self._commands.append(rhs)
        elif isinstance(rhs, tuple):
            self._commands.append(rhs)
        else:
            raise Exception('Unknown command type used with `|`: {}.  Only `str`, `callable`, and `tuple` instances are supported.'.format(type(rhs)))
        return self

    def __str__(self):
        '''
        Return a string representation of this object's pipeline.
        '''
        s = [str(c) for c in self._commands]
        s = '\n'.join(s).replace('\n', '\n    | ')
        if self._mode.startswith('w'):
            s += '\n    > ' + self._outfile
        elif self._mode.startswith('a'):
            s += '\n    >> ' + self._outfile
        return s

    def _wrap_visited(self, f):
        '''
        Reset visited after running f.
        '''
        is_root = not A._visited
        A._visited.add(self.name)
        try:
            return f()
        except Exception as err:
            raise err
        finally:
            # Clear visited list once the pipeline has finished
            # so that other pipelines can run correctly.
            if is_root:
                A._visited.clear()

    def dry_run(self, verbose=False):
        '''
        Return a string containing all steps that a call to `run`
        would execute.
        '''
        f = lambda : '\n'.join(p.dry_run(verbose) for p in self.required_tasks.values() if p.name not in A._visited)

        # This goes before anything below it because `_should_run`
        # depends on the traversal's output.
        output = self._wrap_visited(f)

        if (should := self._should_run()) and not should[0]:
            return ''

        if not verbose:
            return '{}\n{} ({})'.format(output, self.name, should[1])

        output += '''
        \r########################################
        \r# Pipeline `{}` will run due to {}
        \r{}
        \r########################################
        '''.format(
            self.name,
                 should[1],
                 str(self),
                )
        return output

    def _print_scripts(self, command):
        if not self._script_stream:
            return

        self._script_stream.write(str(command) + '\n')

    def run(self):
        '''
        Run pipeline.
        '''
        return self._wrap_visited(self._run)

    def _throw_if_any_fail(self, f, iterable):
        error = None
        for val in iterable:
            try:
                f(val)
            except Exception as err:
                # Save first error and continue if so specified.
                error = error if error else err
                if not self._finish_depth_on_failure:
                    raise error

        # Raise error, if any, once the depth is complete.
        if error:
            raise error

    def _run_dependencies(self):
        # Checking that `p.name not in A._visited` prevents reruning
        # pipelines that have already run.
        self._throw_if_any_fail(
                lambda p: p.run(),
                (p for p in self.required_tasks.values()
                 if p.name not in A._visited
                 ))

    def _stop_early(self):
        '''
        Pre-run checks to determine if the pipeline should run.
        '''
        # Check files and caches.
        if not self._should_run()[0]:
            return True, tuple()
        # Just update any file that's out of date.
        if self._update_requested:
            update_files(self.targets)
            update_files(self.required_files)
            return True, tuple()

        return False, tuple()

    def _prerun(self):
        '''
        Run functions that the pipeline requires to have finished.
        '''
        self._run_dependencies()
        return self._stop_early()

    def _main_run(self):
        '''
        Logic for running the pipeline.
        '''
        # TODO: When independent targets is enabled, the task
        # probably should end in `>`.  Doesn't make sense to have
        # each produce non-empty output.  Which task is the one
        # to use as the aggregate output?  Should they be zipped?
        # Can't guarantee that they're all the same length.  Too
        # many problems.  Either end in `>` or ignore output.
        if self._get_independent_task:
            self._throw_if_any_fail(
                    lambda f: self._get_independent_task(f).run(),
                    maid.tasks.get_filenames(self.targets),
                    )
            return tuple()
        else:
            inputs = self.inputs
            for command in self._commands:
                self._print_scripts(command)
                if isinstance(command, Pipeline):
                    inputs = command(inputs)
                elif callable(command):
                    inputs = map(command, inputs)
                elif isinstance(command, tuple):
                    inputs = command[0](*command[1:], inputs)
            return inputs # ie, outputs.

    def _run(self):
        '''
        Execute the pre-, main-, and post-run stages.
        '''
        try:
            if (r := self._prerun()) and r[0]:
                return r[1]
            outputs = self._main_run()
            # The post-run step has already been done by the
            # independent tasks.  Doing it again would cause
            # errors, particularly overwriting files.
            if not self._get_independent_task:
                self._postrun(outputs)
            return outputs
        except Exception as err:
            msg = 'Error running pipeline `{}`: {}'.format(self.name, err)
            maid.error_utils.remove_files_and_throw(
                    maid.tasks.get_filenames(self.targets) if self._delete_targets_on_error else [],
                    Exception(msg),
                    )

    def _postrun(self, outputs):
        '''
        Run functions that require the pipeline to have finished.
        '''
        if self._outfile:
            self._print_scripts('{} {}\n'.format(
                        '>' if self._mode.startswith('w') else '>>',
                        self._outfile,
                        ))
        if not _write_to_file(outputs, self._outfile, self._mode):
            if self._output_stream:
                self._output_stream.writelines(outputs)

        if (f := maid.task_runner.is_any_target_not_found(self.name, self.targets)):
            msg = 'Task `{task}` ran without error but did not create expected files: `{filename}` not found.'.format(task=self.name, filename=f)
            raise maid.task_runner.MissingExpectedTargetsException(msg)

        _make_hashes(self._cache, self.targets, self.required_files)

    def _should_run(self):
        '''
        '''
        # Checks based on file existance.
        if (f := _any_files_missing(self.targets, must_exist=False)):
            return True, f'missing target `{f}`'
        if self._dont_run_if_all_targets_exist:
            return False, ''

        # Checks based on cache type.
        if self._cache == CacheType.NONE:
            return True, 'uncached pipeline'
        return self._should_run_cache()

    def _should_run_cache(self):
        # Get appropriate decision function.
        should_task_run = maid.monitor.hash.should_task_run
        if self._cache == CacheType.TIME:
            should_task_run = maid.monitor.time.should_task_run

        if should_task_run(self._get_graph())(self.name):
            return True, 'targets out of date'
        return False, ''

    def _get_graph(self):
        '''
        '''
        # This is the graph required for the time and hash cache
        # decision functions.
        graph = {
            p.name: maid.tasks.Task(
                    p.name,
                    lambda a: a, # This doesn't matter; never runs.
                    targets=p.targets,
                    )
            for p in self.required_tasks.values()
        }
        graph[self.name] = maid.tasks.Task(
                    self.name,
                    lambda a: a, # This doesn't matter; never runs.
                    targets=self.targets,
                    required_files=self.required_files,
                    required_tasks=tuple(
                        [p.name for p in self.required_tasks.values()]
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
    def _f(g):
        p = A(
                name,
                inputs=inputs,
                required_files=required_files,
                targets=targets,
                cache=cache,
                is_default=is_default,
                independent_targets_creator=g if independent_targets else None,
                required_tasks=required_tasks,
                output_stream=output_stream,
                script_stream=script_stream,
                )
        # Let `g` immediately create commands for `p`.
        g(p)
    return _f

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
    required_tasks=['p1'],
    output_stream=sys.stdout,
    script_stream=sys.stderr,
    is_default=True,
)
def h2(a):
    a | f"cat {a.required_tasks['p1'].targets[0]}"

print(get_maid().dry_run(verbose=True), file=sys.stderr)
sys.stdout.writelines(get_maid().run())

a = A(inputs=(j for j in range(100))) \
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
print('pipeline output: {}'.format(list(a.run())))

# example from https://github.com/pytoolz/toolz
import collections
import itertools
stem = lambda x: [w.lower().rstrip(",.!:;'-\"").lstrip("'\"") for w in x]
flatten = lambda x: (col for row in x for col in row)
counter = collections.Counter()
a = A(inputs=['this cat jumped over this other cat!']) \
    | str.split \
    | stem \
    | counter.update
_ = list(a.run())
print(counter)
