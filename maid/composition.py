import enum
import itertools
import functools
import sys
import subprocess

import maid.exceptions
import maid.cache
import maid.files


def _print_scripts(outstream, command):
    if outstream:
        outstream.write(str(command) + '\n')


def _write_to_file(lines, filename, mode):
    if not filename:
        return False

    with open(filename, mode=mode) as fos:
        fos.writelines(lines)
    return True


class RunPhase(enum.Enum):
    NORMAL = 0
    START = 1
    END = 2
    FINALLY = 3


class ShellPipeline:
    '''
    '''

    def __init__(self):
        self._commands = []

    def append(self, cmd):
        self._commands.append(cmd)

    def _make_process(cmd, stdin):
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
        # Hook up command outputs to inputs.
        processes = list(itertools.accumulate(
                self._commands[1:],
                lambda proc, cmd: ShellPipeline._make_process(cmd, proc.stdout),
                initial=ShellPipeline._make_process(self._commands[0], subprocess.PIPE),
                ))

        # Write to first command.
        inputs = inputs if inputs else tuple()
        processes[0].stdin.writelines(str(i) + '\n' for i in inputs)
        processes[0].stdin.flush()
        processes[0].stdin.close()

        # Yield output of last command.
        yield from processes[-1].stdout


class SimpleTask:

    def __init__(
            self,
            *,
            inputs=tuple(),
            script_stream=sys.stdout,
            output_stream=sys.stdout,
            ):
        '''
        '''
        self._inputs = inputs
        self._commands = []
        self._outfile = ''
        self._mode = ''
        self._script_stream = script_stream
        self._output_stream = output_stream

    def append(self, command):
        '''
        '''
        match command:
            case str():
                match self._commands:
                    case []:
                        self._commands.append(ShellPipeline())
                    case list(x) if not isinstance(x[-1], ShellPipeline):
                        self._commands.append(ShellPipeline())
                self._commands[-1].append(command)
            case tuple():
                self._commands.append(command)
            case x if callable(x):
                self._commands.append(command)
            case _:
                raise maid.exceptions.UnknownCommandTypeException(command)

    def write_to_file(self, filename):
        SimpleTask._validate_filename(filename)
        self._outfile = filename
        self._mode = 'wt'

    def append_to_file(self, filename):
        SimpleTask._validate_filename(filename)
        self._outfile = filename
        self._mode = 'at'

    def _validate_filename(filename):
        #if self._outfile:
            #raise maid.exceptions.EndOfTaskError(filename)
        if not isinstance(filename, str):
            raise maid.exceptions.InvalidFileTypeException(filename)
        if not filename:
            raise maid.exceptions.EmptyOutputFileException()

    def __str__(self):
        '''
        Return a string representation of this object's commands.
        '''
        commands = '\n'.join(map(str, self._commands)).replace('\n', '\n    | ')
        append = f'\n    >> {self._outfile}' if self._mode == 'at' else ''
        truncate = f'\n    > {self._outfile}' if self._mode == 'wt' else ''
        return '{commands}{truncate}{append}'.format(
                commands=commands,
                append=append,
                truncate=truncate,
                )

    def run(self):
        outputs = functools.reduce(
                self._run_command,
                self._commands,
                self._inputs,
                )
        self._postrun(outputs)
        return outputs

    def _run_command(self, inputs, command):
        _print_scripts(self._script_stream, command)
        match command:
            case ShellPipeline():
                return command(inputs)
            case tuple():
                return command[0](*command[1:], inputs)
            case _ if callable(command):
                return map(command, inputs)
            case _:
                raise maid.exceptions.UnknownCommandTypeException(command)

    def _postrun(self, outputs):
        '''
        Run functions that require the task to have finished.
        '''
        if self._outfile:
            _print_scripts(
                    self._script_stream,
                    '{mode} {file}\n'.format(
                        mode='>' if self._mode.startswith('w') else '>>',
                        file=self._outfile,
                        ))
        if not _write_to_file(outputs, self._outfile, self._mode):
            if self._output_stream:
                self._output_stream.writelines(outputs)


class Task:

    _visited = set()

    def __init__(
            self,
            name='', # o
            *,
            # Maid exclusive.
            maid_name=maid.DEFAULT_MAID_NAME, # o
            run_phase=RunPhase.NORMAL, # o
            is_default=False, # o
            # Task exclusive.
            inputs=None, # o
            required_tasks=None, # o
            required_files=None, # o
            targets=None, # o
            cache=maid.cache.CacheType.NONE, # o
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

        self._task_cacher = maid.cache.TaskCacher(self)
        self._simple_task = SimpleTask(
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

        _ = maid.get_maid(maid_name=maid_name).add_task(self)

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
            self._task_cacher.cache_all()
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
                    maid.files.get_filenames(self.targets),
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
                    maid.files.get_filenames(self.targets) if self._delete_targets_on_error else [],
                    Exception(msg),
                    )

    def _postrun(self, outputs):
        '''
        Run functions that require the task to have finished.
        '''
        if (f := maid.cache.any_files_missing(self.targets)):
            raise maid.exceptions.MissingTargetException(task, f)

        self._task_cacher.cache_targets()

    def _run_dependencies(tasks, *, delay_throw=False):
        # Checking that `p.name not in Task._visited` prevents reruning
        # pipelines that have already run.
        Task._throw_if_any_fail(
                Task.run,
                (t for t in tasks if t.name not in Task._visited),
                delay_throw=delay_throw,
                )
