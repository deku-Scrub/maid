import enum
import os
import sys
import subprocess

import maid.tasks
import maid.task_runner
import maid.monitor.hash
import maid.monitor.time

DEFAULT_MAID_NAME = 'm0'
maids = dict()


def get_maid(maid_name=DEFAULT_MAID_NAME):
    return maids.setdefault(maid_name, M(maid_name))


def _add_task(maid_name, pipeline, is_default, run_phase):
    if not pipeline.name:
        raise Exception('Pipeline names must not be empty.')

    _maid = get_maid(maid_name)

    if is_default:
        if run_phase != RunPhase.NORMAL:
            raise Exception(
                    'Only pipelines in the `NORMAL` run phase can be a default; was given `{}` for pipeline `{}`.'.format(
                        run_phase,
                        pipeline.name,
                        )
                    )
        if _maid.default_pipeline:
            raise Exception(
                    'Maid `{}` already has default pipeline `{}`.'.format(
                        maid_name,
                        _maid.default_pipeline,
                        )
                    )
        _maid.default_pipeline = pipeline.name

    if pipeline.name in _maid.pipelines:
        raise Exception(
                'Maid `{}` already has pipeline named `{}`.'.format(
                    maid_name,
                    pipeline.name,
                    )
                )

    if run_phase == RunPhase.NORMAL:
        _maid.pipelines[pipeline.name] = pipeline
    elif run_phase == RunPhase.START:
        _maid.start_pipelines[pipeline.name] = pipeline
    elif run_phase == RunPhase.END:
        _maid.end_pipelines[pipeline.name] = pipeline
    elif run_phase == RunPhase.FINALLY:
        _maid.finally_pipelines[pipeline.name] = pipeline


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

    def __call__(self, inputs=None):
        inputs = inputs if inputs else tuple()

        # Hook up command outputs to inputs.
        processes = [self._make_process(self._commands[0], subprocess.PIPE)]
        for cmd in self._commands[1:]:
            processes.append(self._make_process(cmd, processes[-1].stdout))

        # Write to first command.
        for cur_input in inputs:
            processes[0].stdin.write(str(cur_input) + '\n')
        processes[0].stdin.flush()
        processes[0].stdin.close()

        # Yield output of last command.
        yield from processes[-1].stdout


class M:
    '''
    '''

    def __init__(self, name):
        self.name = name
        self.default_pipeline = ''
        self.pipelines = dict()
        self.start_pipelines = dict()
        self.end_pipelines = dict()
        self.finally_pipelines = dict()

    def get_pipeline(self, name):
        return self.pipelines[name]

    def dry_run(self, pipeline_name='', verbose=False):
        r = '\n'.join(p.dry_run(verbose) for p in self.start_pipelines.values())
        r += '\n' + self._get_pipeline(pipeline_name).dry_run(verbose)

        re = '\n'.join(p.dry_run(verbose) for p in self.end_pipelines.values())
        if re:
            r += '\n#### These run only if the previous run without error.'
            r += '\n' + re

        rf = '\n'.join(p.dry_run(verbose) for p in self.finally_pipelines.values())
        if rf:
            r += '\n#### These run regardless of any error.'
            r += '\n' + rf

        return r

    def run(self, pipeline_name=''):
        outputs = tuple()
        main_pipeline = self._get_pipeline(pipeline_name)
        try:
            for _, pipeline in self.start_pipelines:
                pipeline.run()
            outputs = main_pipeline.run()
            for _, pipeline in self.end_pipelines:
                pipeline.run()
        except Exception as err:
            raise err
        finally:
            for _, pipeline in self.finally_pipelines:
                pipeline.run()
        return outputs

    def _get_pipeline(self, pipeline_name):
        if pipeline_name in self.pipelines:
            return self.pipelines[pipeline_name]
        if self.default_pipeline:
            return self.pipelines[self.default_pipeline]
        raise Exception('Unknown pipeline.  Maid `{}` has no pipeline named `{}`'.format(self.name, pipeline_name))


def _make_hashes(cache, *files):
    if cache != CacheType.HASH:
        return
    for filenames in files:
        maid.monitor.hash.make_hashes(maid.tasks.get_filenames(filenames))


def _write_to_file(lines, filename, mode):
    if not filename:
        return False

    with open(filename, mode=mode) as fos:
        for line in lines:
            fos.write(line)
    return True


def update_files(filenames):
    maid.monitor.time.touch_files(maid.tasks.get_filenames(filenames))
    maid.monitor.hash.make_hashes(maid.tasks.get_filenames(filenames))


def _any_files_missing(filenames, must_exist=True):
    for f in maid.tasks.get_filenames(filenames, must_exist=must_exist):
        if not os.path.exists(f):
            return f
    return ''


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
            maid_name=DEFAULT_MAID_NAME, # o
            inputs=None, # o
            required_pipelines=None, # o
            required_files=None, # o
            targets=None, # o
            cache=CacheType.NONE, # o
            run_phase=RunPhase.NORMAL, # o
            is_default=False, # o
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

        rp = required_pipelines if required_pipelines else dict()
        self.required_pipelines = {p: get_maid(maid_name).get_pipeline(p) for p in rp}

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
        self._maid_name = maid_name
        self._is_default = is_default

        if self.name:
            _add_task(self._maid_name, self, self._is_default, run_phase)

        self._get_independent_task = None
        if independent_targets_creator:
            def f(target):
                # Makes several assumptions:
                #   * the empty `name` prevents querying maid.
                #   * the lack of `required_pipelines` skips running
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
        f = lambda : '\n'.join(p.dry_run(verbose) for p in self.required_pipelines.values() if p.name not in A._visited)

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
                (p for p in self.required_pipelines.values()
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
            for p in self.required_pipelines.values()
        }
        graph[self.name] = maid.tasks.Task(
                    self.name,
                    lambda a: a, # This doesn't matter; never runs.
                    targets=self.targets,
                    required_files=self.required_files,
                    required_tasks=tuple(
                        [p.name for p in self.required_pipelines.values()]
                        ),
                    )
        return graph


def task(
        name,
        inputs=None,
        required_files=tuple(),
        required_pipelines=tuple(),
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
                required_pipelines=required_pipelines,
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
    required_pipelines=['p1'],
    output_stream=sys.stdout,
    script_stream=sys.stderr,
    is_default=True,
)
def h2(a):
    a | f"cat {a.required_pipelines['p1'].targets[0]}"

print(get_maid().dry_run(verbose=True), file=sys.stderr)
sys.stdout.writelines(get_maid().run())

#a = A(inputs=(j for j in range(100))) \
    #| (lambda x: (xj % 3 == 0 for xj in x)) \
    #| (lambda x: filter(None, x)) \
    #| 'parallel {args} "echo paraLOL; echo {{}}"'.format(args='--bar') \
    #| 'grep -i lol' \
    #| (lambda x: map(len, x)) \
    #| 'wc -l'
## can probably use joblib as a step to parallelize python code in
## the same way gnu parallel can be a step to paralellize shell code:
## ```
##  | (lambda x: joblib.Parallel()(joblib.delayed(f)(xj) for xj in x))
## ```
#print(a.dry_run(True))
#print('pipeline output: {}'.format(list(a.run())))
#
## example from https://github.com/pytoolz/toolz
import collections
stem = lambda x: [w.lower().rstrip(",.!:;'-\"").lstrip("'\"") for w in x]
counter = collections.Counter()
a = A(inputs=['this cat jumped over this other cat!']) \
    | str.split \
    | stem \
    | counter.update
_ = list(a.run())
print(counter)
