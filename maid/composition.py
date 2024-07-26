import itertools
import subprocess


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
