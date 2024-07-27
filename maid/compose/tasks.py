import enum
import itertools
import functools
import sys
import subprocess
from typing import Optional, Any, Iterable, IO, Callable, Final, Self

import maid.cache.cacher
import maid.files
import maid.compose.base


class RunPhase(enum.Enum):
    NORMAL = 0
    START = 1
    END = 2
    FINALLY = 3


class ShellPipeline:
    '''
    '''

    def __init__(self):
        self._commands: list = []

    def append(self, cmd: str) -> None:
        self._commands.append(cmd)

    @staticmethod
    def _make_process(cmd: str, stdin: IO) -> subprocess.Popen:
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

    def __str__(self) -> str:
        return '\n'.join(self._commands)

    def __call__[I, O](self, inputs: Optional[Iterable[I]] = None) -> Iterable[O]:
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

    def __init__[T](
            self,
            *,
            inputs: tuple[T] = tuple(),
            script_stream: IO = sys.stdout,
            output_stream: IO = sys.stdout,
            ):
        '''
        '''
        self._inputs: Final[tuple[T]] = inputs
        self._commands: Final[list[ShellPipeline | Callable[[Any], Any]]] = []
        self._outfile: str = ''
        self._mode: str = ''
        self._script_stream: Final[IO] = script_stream
        self._output_stream: Final[IO] = output_stream

    def append(self, command: str | tuple | Callable[[Any], Any]) -> None:
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
                raise UnknownCommandTypeException(command)

    def write_to_file(self, filename: str) -> None:
        SimpleTask._validate_filename(filename)
        self._outfile = filename
        self._mode = 'wt'

    def append_to_file(self, filename: str) -> None:
        SimpleTask._validate_filename(filename)
        self._outfile = filename
        self._mode = 'at'

    @staticmethod
    def _validate_filename(filename: str) -> None:
        #if self._outfile:
            #raise EndOfTaskError(filename)
        if not isinstance(filename, str):
            raise InvalidFileTypeException(filename)
        if not filename:
            raise EmptyOutputFileException()

    def __str__(self) -> str:
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

    def run[T](self) -> Iterable[T]:
        outputs = functools.reduce(
                self._run_command,
                self._commands,
                self._inputs,
                )
        self._postrun(outputs)
        return outputs

    def _run_command[I, O](
            self,
            inputs: Iterable[I],
            command: ShellPipeline | tuple | Callable[[I], O]
            ) -> Iterable[O]:
        _print_scripts(self._script_stream, command)
        match command:
            case ShellPipeline():
                return command(inputs)
            case tuple():
                return command[0](*command[1:], inputs)
            case _ if callable(command):
                return map(command, inputs)
            case _:
                raise UnknownCommandTypeException(command)

    def _postrun[T](self, outputs: Iterable[T]) -> None:
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


class Task(maid.compose.base.DependecyGraphTask):

    _visited = set()

    def __init__[T](
            self,
            name: str = '', # o
            *,
            # Maid exclusive.
            maid_name: str =  '', # o
            run_phase: RunPhase = RunPhase.NORMAL, # o
            is_default: bool = False, # o
            # Task exclusive.
            inputs: Optional[Iterable[T]] = None, # o
            required_tasks: Optional[Iterable[str]] = None, # o
            required_files: Optional[Iterable[str]] = None, # o
            targets: Optional[Iterable[str]] = None, # o
            cache: maid.cache.cacher.CacheType = maid.cache.cacher.CacheType.NONE, # o
            build_task: Optional[Callable[[Self], None]] = None,
            script_stream: IO = None, # o
            output_stream: IO = None, # o
            delete_targets_on_error: bool = True, #o
            dont_run_if_all_targets_exist: bool = False, # o
            description: str = '',
            finish_depth_on_failure: bool = False, # o
            update_requested: bool = False, # o
            ):
        super().__init__(name)

        self.name = name

        rp = required_tasks if required_tasks else dict()
        self.required_tasks: Final[dict[str, Self]] = {t().name: t() for t in rp}

        self._task_cacher: Final[maid.cache.cacher.TaskCacher] = maid.cache.cacher.TaskCacher(self)
        self._simple_task: Final[SimpleTask] = SimpleTask(
                inputs=inputs,
                script_stream=script_stream,
                output_stream=output_stream,
                )
        self.required_files: Final[tuple[str]] = tuple(required_files) if required_files else tuple()
        self.targets: Final[tuple[str]] = tuple(targets) if targets else tuple()
        self._outfile: str = ''
        self._mode: str = ''
        self._finish_depth_on_failure: Final[bool] = finish_depth_on_failure
        self.dont_run_if_all_targets_exist: Final[bool] = dont_run_if_all_targets_exist
        self.cache: Final[maid.cache.cacher.CacheType] = cache
        self.update_requested: Final[bool] = update_requested
        self._delete_targets_on_error: Final[bool] = delete_targets_on_error
        self.maid_name: Final[str] = maid_name
        self.is_default: Final[bool] = is_default
        self.run_phase: Final[RunPhase] = run_phase

        self._get_independent_task: Optional[Callable[[str], Self]] = None
        if build_task:
            def f(target: str) -> Self:
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
                    build_task=None,
                    script_stream=script_stream,
                    output_stream=output_stream,
                    delete_targets_on_error=delete_targets_on_error,
                    dont_run_if_all_targets_exist=dont_run_if_all_targets_exist,
                    finish_depth_on_failure=finish_depth_on_failure,
                    update_requested=update_requested,
                )
                build_task(a)
                return a
            self._get_independent_task = f

    def __gt__(self, rhs: Any) -> Self:
        '''
        Write to file given by `rhs`.

        The file is truncated first.
        '''
        self._simple_task.write_to_file(rhs)
        return self

    def __rshift__(self, rhs: Any) -> Self:
        '''
        Append to file given by `rhs`.

        Note that due to Python's precedence rules, this takes
        precedence over `|` and results in an error unless everything
        before the `>>` is wrapped in parentheses.
        '''
        self._simple_task.append_to_file(rhs)
        return self

    def __or__[I, O](self, rhs: str | tuple | Callable[[I], O]) -> Self:
        '''
        Add `rhs`'s command to this object's command list.
        '''
        self._simple_task.append(rhs)
        return self

    def __str__(self) -> str:
        '''
        Return a string representation of this object's commands.
        '''
        return str(self._simple_task)

    @staticmethod
    def _wrap_visited[T](f: Callable[[], T], task_name: str) -> T:
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

    @staticmethod
    def _get_required_dry_runs(tasks: Iterable[Self], verbose: bool) -> str:
        return lambda: '\n'.join(
            t.dry_run(verbose)
            for t in tasks
            if t.name not in Task._visited
            )

    def dry_run(self, verbose: bool = False) -> str:
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

    def run[T](self) -> Iterable[T]:
        '''
        Run task.
        '''
        return Task._wrap_visited(self._run, self.name)

    @staticmethod
    def _throw_if_any_fail[T](
            f: Callable[[T], None],
            iterable: Iterable[T],
            *,
            delay_throw: bool = False,
            ) -> None:
        error: Exception = None
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

    def _prerun(self) -> tuple[bool, str]:
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

    def _main_run[T](self) -> Iterable[T]:
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
        return self._simple_task.run()

    def _run[T](self) -> Iterable[T]:
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
                self._postrun()
            return outputs
        except Exception as err:
            msg = 'Error running task `{}`: {}'.format(self.name, err)
            maid.error_utils.remove_files_and_throw(
                    maid.files.get_filenames(self.targets) if self._delete_targets_on_error else [],
                    Exception(msg),
                    )

    def _postrun(self) -> None:
        '''
        Run functions that require the task to have finished.
        '''
        if (f := maid.cache.cacher.any_files_missing(self.targets)):
            raise MissingTargetException(self.name, f)

        self._task_cacher.cache_targets()

    @staticmethod
    def _run_dependencies(
            tasks: Iterable[Self],
            *,
            delay_throw: bool = False,
            ) -> None:
        # Checking that `p.name not in Task._visited` prevents reruning
        # pipelines that have already run.
        Task._throw_if_any_fail(
                Task.run,
                (t for t in tasks if t.name not in Task._visited),
                delay_throw=delay_throw,
                )


class EmptyOutputFileException(Exception):
    '''
    '''

    def __init__(self):
        '''
        '''
        msg = 'The right operand of `>` and `>>` must not be empty'
        super().__init__(msg)


class InvalidFileTypeException(Exception):
    '''
    '''

    def __init__(self, filename: str):
        '''
        '''
        msg = 'The right operand of `>` and `>>` must be a string, instead got {}'.format(filename)
        super().__init__(msg)


class MissingTargetException(Exception):
    '''
    '''

    def __init__(self, task_name: str, filename: str):
        '''
        '''
        msg = 'Task `{task}` ran without error but did not create expected files: `{filename}` not found.'.format(task=task_name, filename=filename)
        super().__init__(msg)


class UnknownCommandTypeException(Exception):

    def __init__(self, command: Any):
        '''
        '''
        msg = 'Unknown command type used with `|`: {}.  Only `str`, `callable`, and `tuple` instances are supported.'.format(command)
        super().__init__(msg)


def _print_scripts[I, O](
        outstream: IO,
        command: ShellPipeline | tuple | Callable[[I], O],
        ) -> None:
    if outstream:
        outstream.write(str(command) + '\n')


def _write_to_file(lines: Iterable[str], filename: str, mode: str) -> None:
    if not filename:
        return False

    with open(filename, mode=mode) as fos:
        fos.writelines(lines)
    return True
