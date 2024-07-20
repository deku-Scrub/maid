import enum
import os
import sys
import subprocess

import maid.tasks
import maid.monitor.hash
import maid.monitor.time


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
            name, # o
            inputs=None, # o
            required_pipelines=None, # o
            required_files=None, # o
            targets=None, # o
            cache=CacheType.NONE, # o
            run_phase=RunPhase.NORMAL,
            is_default=False,
            independent_targets=False,
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
        self.required_pipelines = tuple(required_pipelines) if required_pipelines else tuple()
        self.required_files = tuple(required_files) if required_files else tuple()
        self.targets = tuple(targets) if targets else tuple()
        self._pipelines = [[]]
        self._iterables = []
        self._outfile = ''
        self._mode = ''
        self._finish_depth_on_failure = finish_depth_on_failure
        self._dont_run_if_all_targets_exist = dont_run_if_all_targets_exist
        self._cache = cache
        self._update_requested = update_requested
        self._delete_targets_on_error = delete_targets_on_error
        self._output_stream = output_stream
        self._script_stream = script_stream

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
            self._pipelines[-1].append(rhs)
        elif isinstance(rhs, A):
            self._pipelines[-1].append(rhs.cmd)
        #elif callable(rhs):
            #self._callables.append(rhs)
        else:
            self._pipelines.append([])
            self._iterables.append(rhs)
        return self

    def __str__(self):
        '''
        Return a string representation of this object's pipeline.
        '''
        s = []
        iterables = ['<inputs>' if self.inputs else '<no inputs>' ] + self._iterables
        #iterables = [self.inputs] + self._iterables
        for j, (inputs, pipeline) in enumerate(zip(iterables, self._pipelines)):
            if j == 0:
                s.append('' + str(inputs))
            else:
                s.append('    | ' + str(inputs))
            for k, cmd in enumerate(pipeline):
                if (k == 0) and (not self.inputs):
                    s.append('    ' + cmd)
                else:
                    s.append('    | ' + cmd)
        if self._mode.startswith('w'):
            s.append('    > ' + self._outfile)
        elif self._mode.startswith('a'):
            s.append('    >> ' + self._outfile)
        return '\n'.join(s)

    def dry_run(self):
        '''
        Return a string containing all steps that a call to `run`
        would execute.
        '''
        output = '\n'.join(p.dry_run() for p in self.required_pipelines if pipeline.name not in A._visited)
        if (should := self._should_run()) and should[0]:
            output += '''
            \r########################################
            \r# Pipeline `{}` will run due to {}
            \r{}
            \r########################################
            '''.format(
                self.name,
                     should[1],
                     self.__str__(),
                    )
        return output

    def _print_scripts(self, inputs, pipeline):
        if not self._script_stream:
            return

        if callable(inputs):
            self._script_stream.write(str(inputs) + '\n')
        self._script_stream.writelines('\n'.join(pipeline) + '\n')

    def run(self):
        '''
        Run pipeline.
        '''
        is_root = False if A._visited else True
        A._visited.add(self.name)

        try:
            return self._run()
        except Exception as err:
            raise err
        finally:
            # Clear visited list once the pipeline has finished
            # so that other pipelines can run correctly.
            if is_root:
                A._visited.clear()

    def _run_dependencies(self):
        error = None
        for pipeline in self.required_pipelines:
            # Don't rerun pipelines that have already run.
            if pipeline.name in A._visited:
                continue

            try:
                pipeline.run()
            except Exception as err:
                # Save first error and continue if so specified.
                error = error if error else err
                if not self._finish_depth_on_failure:
                    raise error

        # Raise error, if any, once the depth is complete.
        if error:
            raise error

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
        outputs = tuple()
        iterables = [self.inputs] + self._iterables
        for inputs, pipeline in zip(iterables, self._pipelines):
            self._print_scripts(inputs, pipeline)
            # If given a function, its output is `pipeline`'s input.
            new_inputs = inputs(outputs) if callable(inputs) else inputs
            outputs = self._run_pipeline(new_inputs, pipeline)
        return outputs

    def _run(self):
        '''
        Execute the pre-, main-, and post-run stages.
        '''
        try:
            if (r := self._prerun()) and r[0]:
                return r[1]
            outputs = self._main_run()
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
        _make_hashes(self._cache, self.targets, self.required_files)

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

    def _run_pipeline(self, inputs, pipeline):
        # Hook up command outputs to inputs.
        processes = [self._make_process(pipeline[0], subprocess.PIPE)]
        for cmd in pipeline[1:]:
            processes.append(self._make_process(cmd, processes[-1].stdout))

        # Write to first command.
        for cur_input in inputs:
           processes[0].stdin.write(cur_input)
        processes[0].stdin.flush()
        processes[0].stdin.close()

        # Yield output of last command.
        for line in processes[-1].stdout:
           yield line

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
            for p in self.required_pipelines
        }
        graph[self.name] = maid.tasks.Task(
                    self.name,
                    lambda a: a, # This doesn't matter; never runs.
                    targets=self.targets,
                    required_files=self.required_files,
                    required_tasks=tuple(
                        [p.name for p in self.required_pipelines]
                        ),
                    )
        return graph


pipeline = A(
        'p1',
        inputs=['lol\n', '.lol\n'],
        required_files=['requirements.txt'],
        targets=['a.txt'],
        cache=CacheType.HASH,
        script_stream=sys.stdout,
        )
pipeline = pipeline \
        | "sed 's/lol/md/'" \
        | "grep .md" \
        | (lambda o: (oj.strip()+'?' for oj in o)) \
        | "tr 'm' '!'" \
        > pipeline.targets[0]
pipeline2 = A(
        'p2',
        required_pipelines=[pipeline],
        output_stream=sys.stdout,
        script_stream=sys.stdout,
        ) \
    | "cat a.txt"
print(pipeline2.dry_run())
output = pipeline2.run()
print(list(output))
