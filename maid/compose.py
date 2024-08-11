import enum
import itertools
import functools
import sys
import subprocess
from dataclasses import dataclass
from typing import Optional, Any, IO, Callable, Final, Self, Iterable, assert_never, cast


type Function = (
        Callable[[Any], Any]
        | tuple[Callable[..., Iterable[Any]], *tuple[Any, ...]]
        )
type CommandType = list[str] | Function
type ValidCommandType = str | Function


@dataclass(frozen=True)
class ShellPipeline:
    '''
    '''

    commands: list[str]

    @staticmethod
    def _pipe_process(
            prev_proc: Optional[subprocess.Popen[str]],
            cmd: str,
            ) -> subprocess.Popen[str]:
        '''
        Make process with the necessary common parameters.
        '''
        return subprocess.Popen(
                cmd,
                stdin=prev_proc.stdout if prev_proc else subprocess.PIPE,
                stdout=subprocess.PIPE,
                shell=True,
                text=True,
                )

    def __str__(self) -> str:
        return '\n'.join(self.commands)

    def __call__(self, inputs: Iterable[Any] = tuple()) -> Iterable[Any]:
        if not self.commands:
            return

        # Pipe command outputs to inputs.
        processes: Final[list[subprocess.Popen[str]]] = list(
                itertools.accumulate(
                    self.commands[1:],
                    ShellPipeline._pipe_process,
                    initial=ShellPipeline._pipe_process(None, self.commands[0]),
                    ))

        # Write to first command.
        assert processes[0].stdin is not None # Needed for mypy.
        assert processes[-1].stdout is not None # Needed for mypy.
        processes[0].stdin.writelines(str(i) + '\n' for i in inputs)
        processes[0].stdin.flush()
        processes[0].stdin.close()

        # Yield output of last command.
        yield from processes[-1].stdout


class Recipe:

    def __init__(
            self,
            *,
            inputs: Iterable[Any] = tuple(),
            script_stream: Optional[IO[str]] = None,
            output_stream: Optional[IO[str]] = None,
            ):
        '''
        '''
        self._inputs: Final[Iterable[Any]] = inputs
        self._script_stream: Final[Optional[IO[str]]] = script_stream
        self._output_stream: Final[Optional[IO[str]]] = output_stream
        self._commands: Final[list[CommandType]] = []
        self._outfile: str = ''
        self._mode: str = ''

    def __gt__(self, filename: str) -> Self:
        '''
        Write to file given by `rhs`.

        The file is truncated first.
        '''
        Recipe._validate_filename(filename)
        self._outfile = filename
        self._mode = 'wt'
        return self

    def __rshift__(self, filename: str) -> Self:
        '''
        Append to file given by `rhs`.

        Note that due to Python's precedence rules, this takes
        precedence over `|` and results in an error unless everything
        before the `>>` is wrapped in parentheses.
        '''
        Recipe._validate_filename(filename)
        self._outfile = filename
        self._mode = 'at'
        return self

    def __or__(self, cmd: ValidCommandType) -> Self:
        '''
        Add `rhs`'s command to this object's command list.
        '''
        match cmd:
            case str():
                if not self._commands:
                    self._commands.append([])
                if not isinstance(self._commands[-1], list):
                    self._commands.append([])
                cast(list[str], self._commands[-1]).append(cmd)
            case tuple():
                self._commands.append(cmd)
            case x if callable(x):
                self._commands.append(cmd)
            case _ as unreachable:
                assert_never(unreachable)
        return self

    @staticmethod
    def _validate_filename(filename: str) -> None:
        # TODO: uncomment
        #if self._outfile:
            #raise EndOfRecipeException(filename)
        if not isinstance(filename, str):
            raise InvalidFileTypeException(filename)
        if not filename:
            raise EmptyOutputFileException()

    def __str__(self) -> str:
        '''
        Return a string representation of this object's commands.
        '''
        commands = '\n'.join(
                str(ShellPipeline(c)) if isinstance(c, list) else str(c)
                for c in self._commands
                ).replace('\n', '\n    | ')
        append = f'\n    >> {self._outfile}' if self._mode == 'at' else ''
        truncate = f'\n    > {self._outfile}' if self._mode == 'wt' else ''
        return '{commands}{truncate}{append}'.format(
                commands=commands,
                append=append,
                truncate=truncate,
                )

    def run(self) -> Iterable[Any]:
        return self._postrun(
                functools.reduce(
                    self._run_command,
                    self._commands,
                    self._inputs,
                    ))

    def _run_command(
            self,
            inputs: Iterable[Any],
            command: CommandType,
            ) -> Iterable[Any]:
        _print_scripts(self._script_stream, command)
        match command:
            case list():
                return ShellPipeline(command)(inputs)
            case tuple():
                return command[0](*command[1:], inputs)
            case _ if callable(command):
                return map(command, inputs)
            case _ as unreachable:
                assert_never(unreachable)

    def _postrun(self, outputs: Iterable[Any]) -> Iterable[Any]:
        '''
        Run functions that require the recipe to have finished.
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
                if isinstance(outputs, Iterable):
                    self._output_stream.writelines(str(o) for o in outputs)
                else:
                    self._output_stream.writelines((str(outputs),))
        return outputs


def _print_scripts(
        outstream: Optional[IO[str]],
        command: ValidCommandType | CommandType,
        ) -> None:
    if outstream:
        match command:
            case list():
                outstream.write(str(ShellPipeline(command)) + '\n')
            case _:
                outstream.write(str(command) + '\n')


def _write_to_file(lines: Iterable[object], filename: str, mode: str) -> bool:
    if not filename:
        return False

    with open(filename, mode=mode) as fos:
        fos.writelines(lines)
    return True


class EmptyOutputFileException(Exception):
    '''
    '''

    def __init__(self) -> None:
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


#r = (
        #Recipe(script_stream=sys.stdout, output_stream=sys.stdout)
        #| 'ls'
        #| (filter, lambda x: '.py' in x)
        #| str.upper
        #| (lambda x: x.replace('\n', ''))
        #| (''.join,)
        #)
#print((r.run()))
