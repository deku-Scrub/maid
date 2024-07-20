import sys
import subprocess
import inspect
import types
import pathlib
import os
import traceback

import maid.error_utils


def _throw_script_error(task, err_info):
    msg = ''
    if task.delete_targets_on_error:
        msg = 'Error running script for task `{task}` (return code {return_code}): {err_msg}.  Any target files of the failed task will now be removed.'.format(task=task.name, return_code=err_info[0], err_msg=err_info[1])
    else:
        msg = 'Error running script for task `{task}` (return code {return_code}): {err_msg}.  Deleting target for this task has been disabled, so they will not be removed.'.format(task=task.name, return_code=err_info[0], err_msg=err_info[1])
    maid.error_utils.remove_files_and_throw(
            task.get_targets() if task.delete_targets_on_error else [],
            ScriptException(msg),
            )


def _handle_function_error(task, err):
    msg = ''
    if task.delete_targets_on_error:
        msg = 'Error running Python function for task `{task}`: {err_msg}.  Any target files of the failed task will now be removed.'.format(task=task.name, err_msg=err)
    else:
        msg = 'Error running Python function for task `{task}`: {err_msg}.  Deleting target for this task has been disabled, so they will not be removed.'.format(task=task.name, err_msg=err)
    traceback.print_exc()
    maid.error_utils.remove_files_and_throw(
            task.get_targets() if task.delete_targets_on_error else [],
            PythonFunctionException(msg),
            )


def get_filenames(filenames, must_exist=True):
    return _get_filenames(filenames, must_exist=must_exist)


def _get_filenames(filenames, must_exist=True):
    for f in filenames:
        dirname, basename = os.path.split(f)

        # Whichever isn't empty is the basename.
        if not basename:
            yield dirname
        elif not dirname:
            yield basename
        else:
            has_files = False
            for p in pathlib.Path(dirname).glob(basename.replace('\\', '\\\\')):
                yield str(p)
                has_files = True
            if (not has_files) and must_exist:
                raise FileNotFoundError('Required file not found: {}'.format(f))
            if (not has_files) and (not must_exist):
                yield f


class Task:
    '''
    '''

    def __init__(
            self,
            name,
            recipe,
            *,
            targets=None,
            required_tasks=None,
            required_files=None,
            targets_regex_mappings=None, # TODO: implement and test.
            description='Please add a task description.',
            delete_targets_on_error=True,
            print_script=sys.stdout,
            print_script_output=sys.stdout,
            always_run_if_in_graph=False,
            dont_run_if_all_targets_exist=False,
            #run_at_start=False, # TODO: test. Seems only used in worker.
            #run_at_end=False, # TODO: test. Seems only used in worker.
            shell=None,
            ):
        '''
        '''
        if not callable(recipe):
            raise Exception()
        if not isinstance(name, str):
            raise Exception()

        shell = shell if shell else ('bash', '-c')
        if type(shell) not in (list, tuple, types.NoneType):
            raise Exception()
        if type(targets) not in (list, tuple, types.NoneType):
            raise Exception()
        if type(required_files) not in (list, tuple, types.NoneType):
            raise Exception()
        if type(required_tasks) not in (list, tuple, types.NoneType):
            raise Exception()
        if type(targets_regex_mappings) not in (list, tuple, types.NoneType):
            raise Exception()
        if not isinstance(description, str):
            raise Exception()

        required_files = required_files if required_files else tuple()
        for f in _get_filenames(required_files):
            if not os.path.exists(f):
                raise Exception()

        required_tasks = required_tasks if required_tasks else tuple()
        if name in required_tasks:
            raise Exception()

        self.name = name
        self.recipe = recipe
        self.targets = tuple(targets if targets else [])
        self.required_tasks = tuple(required_tasks if required_tasks else [])
        self.required_files = tuple(required_files if required_files else [])
        self.description = description
        self.delete_targets_on_error = delete_targets_on_error
        self.print_script = print_script
        self.print_script_output = print_script_output
        self.always_run_if_in_graph = always_run_if_in_graph
        self.dont_run_if_all_targets_exist = dont_run_if_all_targets_exist
        self.shell = shell

    def get_targets(self):
        '''
        '''
        return _get_filenames(self.targets, must_exist=False)

    def get_required_files(self):
        '''
        '''
        return _get_filenames(self.required_files)

    def run(self, *, modified_tasks=None):
        '''
        '''
        if (script := self._run_recipe(modified_tasks=modified_tasks)):
            if self.print_script:
                self.print_script.writelines(script + '\n')
            return self._run_script(script)

        if self.print_script:
            try:
                src = inspect.getsource(self.recipe)
                self.print_script.write(src)
            except OSError as err:
                print('Recipe for task `{}` could not be printed: {}'.format(self.name, repr(err)), file=sys.stderr)

        return 0, ''

    def _run_script(self, script):
        stdout, stderr = sys.stdout, sys.stderr
        if not self.print_script_output:
            stdout = subprocess.DEVNULL
            stderr = subprocess.DEVNULL
        result = subprocess.run(
                (*self.shell, script),
                stdout=stdout,
                stderr=stderr,
                text=True,
                )
        if (code := result.returncode) != 0:
            err = f'Script failed to run successfully: return code {code}'
            _throw_script_error(self, err)
        return 0, result.stderr if result.stderr else ''

    def _run_recipe(self, modified_tasks=None):
        script = ''

        prev_stdout, prev_stderr = sys.stdout, sys.stderr
        if not self.print_script_output:
            sys.stdout = open(os.devnull, mode='w')
            sys.stderr = open(os.devnull, mode='w')
        try:
            script = self.recipe(
                    self.required_tasks,
                    self.required_files,
                    self.targets,
                    modified_tasks,
                    )
        except Exception as err:
            # Revert before `_handle_function_error` because it needs to
            # print a message.
            sys.stdout = prev_stdout
            sys.stderr = prev_stderr
            _handle_function_error(self, err)
        finally:
            if not self.print_script_output:
                sys.stdout.close()
                sys.stderr.close()
            sys.stdout = prev_stdout
            sys.stderr = prev_stderr

        return script


class WorkRequest:
    '''
    '''

    def __init__(
            self,
            maid_name,
            task,
            startup_tasks,
            main_tasks,
            teardown_tasks,
            dry_run=False, # TODO: test
            use_hash=False,
            update_requested=False,
            finish_depth_on_failure=False,
            ):
        '''
        '''
        self.maid_name = maid_name
        self.task = task
        self.startup_tasks = startup_tasks
        self.main_tasks = main_tasks
        self.teardown_tasks = teardown_tasks
        self.dry_run = dry_run
        self.use_hash = use_hash
        self.update_requested = update_requested
        self.finish_depth_on_failure = finish_depth_on_failure


class PythonFunctionException(Exception):
    '''
    '''

    def __init__(self, msg):
        '''
        '''
        super().__init__(msg)


class ScriptException(Exception):
    '''
    '''

    def __init__(self, msg):
        '''
        '''
        super().__init__(msg)


#class InvalidPathException(Exception):
    #'''
    #'''
#
    #def __init__(self, msg):
        #'''
        #'''
        #super().__init__(msg)
