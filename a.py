import enum
import os
import sys
import subprocess
import multiprocessing

import maid.tasks
import maid.monitor.hash
import maid.monitor.time


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

    def __init__(
            self,
            name,
            inputs=None,
            required_pipelines=None,
            required_files=None,
            targets=None,
            cache=CacheType.NONE, # o
            run_phase=RunPhase.NORMAL,
            is_default=False,
            print_script=sys.stdout,
            print_script_output=sys.stdout,
            delete_targets_on_error=True,
            dont_run_if_all_targets_exist=False, # o
            description='',
            finish_depth_on_failure=False,
            ):
        self.name = name
        self.inputs = tuple(inputs) if inputs else tuple()
        self.required_pipelines = tuple(required_pipelines) if required_pipelines else tuple()
        self.required_files = tuple(required_files) if required_files else tuple()
        self.targets = tuple(targets) if targets else tuple()
        self._commands = [[]]
        self._iterables = []
        self._outfile = ''
        self._mode = ''
        self._finish_depth_on_failure = finish_depth_on_failure
        self._dont_run_if_all_targets_exist = dont_run_if_all_targets_exist
        self._cache = cache

        self._graph = {
            p.name: maid.tasks.Task(
                    p.name,
                    lambda a: a, # This doesn't matter; never runs.
                    targets=p.targets,
                    )
            for p in self.required_pipelines
        }
        self._graph[name] = maid.tasks.Task(
                    name,
                    lambda a: a, # This doesn't matter; never runs.
                    targets=self.targets,
                    required_files=self.required_files,
                    required_tasks=tuple([p.name for p in self.required_pipelines]),
                    )

    def __gt__(self, rhs):
        self._outfile = rhs
        self._mode = 'wb'
        return self

    def __rshift__(self, rhs):
        self._outfile = rhs
        self._mode = 'ab'
        return self

    def __or__(self, rhs):
        if isinstance(rhs, str):
            self._commands[-1].append(rhs)
        elif isinstance(rhs, A):
            self._commands[-1].append(rhs.cmd)
        #elif callable(rhs):
            #self._callables.append(rhs)
        else:
            self._commands.append([])
            self._iterables.append(rhs)
        return self

    def __str__(self):
        s = [str(p) for p in self.required_pipelines]
        iterables = ['<inputs>'] + self._iterables
        #iterables = [self.inputs] + self._iterables
        for j, (inputs, commands) in enumerate(zip(iterables, self._commands)):
            if j == 0:
                s.append('' + str(inputs))
            else:
                s.append('    | ' + str(inputs))
            for cmd in commands:
                s.append('    | ' + cmd)
        if self._mode.startswith('w'):
            s.append('    > ' + self._outfile)
        elif self._mode.startswith('a'):
            s.append('    >> ' + self._outfile)
        return '\n'.join(s)

    def run(self):
        error = None
        for pipeline in self.required_pipelines:
            try:
                pipeline.run()
            except Exception as err:
                error = error if error else err
                if not self._finish_depth_on_failure:
                    raise error
        if error:
            raise error

        if not self._should_run():
            return

        try:
            iterables = [self.inputs] + self._iterables
            for inputs, commands in zip(iterables, self._commands):
                if callable(inputs):
                    outputs = self._run(inputs(outputs), commands)
                else:
                    outputs = self._run(inputs, commands)
        except Exception as err:
            msg = 'Error running pipeline `{}`: {}'.format(self.name, err)
            maid.error_utils.remove_files_and_throw(
                    maid.tasks.get_filenames(self.targets) if self.delete_targets_on_error else [],
                    Exception(msg),
                    )

        if self._outfile:
            with open(self._outfile, mode=self._mode) as fos:
                for line in outputs:
                    fos.write(line)

        return outputs

    def _run(self, inputs, commands):
        processes = [
            subprocess.Popen(
                    commands[0],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    shell=True,
                    ),
                ]
        for cmd in commands[1:]:
            process = subprocess.Popen(
                    cmd,
                    stdin=processes[-1].stdout,
                    stdout=subprocess.PIPE,
                    shell=True,
                    )
            processes.append(process)

        for cur_input in inputs:
           processes[0].stdin.write(cur_input)
        processes[0].stdin.flush()
        processes[0].stdin.close()

        for line in processes[-1].stdout:
           yield line

    def _should_run(self):
        '''
        '''
        filenames = maid.tasks.get_filenames(self.targets, must_exist=False)
        all_targets_exist = all(os.path.exists(f) for f in filenames)
        if not all_targets_exist:
            return True, f'missing target `{f}`'
        if self._dont_run_if_all_targets_exist:
            return False, ''
        if self._cache == CacheType.NONE:
            return True, 'uncached pipeline'
        if self._cache == CacheType.HASH:
            if maid.monitor.hash.should_task_run(self._graph)(self.name):
                return True, 'targets out of date'
            return False, ''
        if self._cache == CacheType.TIME:
            if maid.monitor.time.should_task_run(self._graph)(self.name):
                return True, 'targets out of date'
            return False, ''
        return False, ''


pipeline = A(
        'p1',
        inputs=[b'lol\n', b'.lol\n'],
        required_files=['requirements.txt'],
        targets=['a.txt'],
        cache=CacheType.HASH,
        )
pipeline = pipeline \
        | "sed 's/lol/md/'" \
        | "grep .md" \
        | (lambda o: (oj.strip()+b'?' for oj in o)) \
        | "tr '.' '!'" \
        > pipeline.targets[0]
pipeline2 = A('p2', required_pipelines=[pipeline]) | "cat a.txt"
output = pipeline2.run()
print(pipeline2)
print(list(output))
