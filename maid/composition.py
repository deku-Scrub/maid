import itertools
import functools
import sys
import subprocess

import maid.exceptions


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


def _print_scripts(outstream, command):
    if outstream:
        outstream.write(str(command) + '\n')


def _write_to_file(lines, filename, mode):
    if not filename:
        return False

    with open(filename, mode=mode) as fos:
        fos.writelines(lines)
    return True
