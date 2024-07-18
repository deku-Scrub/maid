import io
import pathlib
import unittest
import shutil
import os

import maid.tasks


class PublicApiTestCase(unittest.TestCase):

    def _make_task(self, targets, only_targets=False):
        return maid.tasks.Task(
                'a',
                lambda a: a,
                targets=targets,
                required_files=[] if only_targets else targets,
                )

    def _setup_env(self):
        tmp_dir = 'tmp'
        if os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)
        filename_a = os.path.join(tmp_dir, 'a')
        filename_b = os.path.join(tmp_dir, 'b')
        pathlib.Path(filename_a).touch()
        pathlib.Path(filename_b).touch()
        return filename_a, filename_b

    def test_run_delete_targets_on_error(self):
        filename_a, _ = self._setup_env()
        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: f'leisldkeifjsslsleirjsls {filename_a}',
            targets=[filename_a],
            )
        with self.assertRaises(maid.tasks.ScriptException):
            return_code, _ = task.run()
            self.assertTrue(return_code == 0)
            self.assertTrue(not os.path.exists(filename_a))

        filename_a, _ = self._setup_env()
        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: f'leisldkeifjsslsleirjsls {filename_a}',
            delete_targets_on_error=False,
            targets=[filename_a],
            )
        with self.assertRaises(maid.tasks.ScriptException):
            return_code, _ = task.run()
            self.assertTrue(return_code == 0)
            self.assertTrue(os.path.exists(filename_a))

    def test_run_shell(self):
        filename_a, _ = self._setup_env()
        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: f'rm {filename_a}',
            shell=None,
            )
        return_code, _ = task.run()
        self.assertTrue(return_code == 0)
        self.assertTrue(not os.path.exists(filename_a))

        filename_a, _ = self._setup_env()
        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: f'import os; os.remove("{filename_a}")',
            shell=('python3', '-c'),
            )
        return_code, _ = task.run()
        self.assertTrue(return_code == 0)
        self.assertTrue(not os.path.exists(filename_a))

        filename_a, _ = self._setup_env()
        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: f'rm {filename_a}',
            shell=('sh', '-c'),
            )
        return_code, _ = task.run()
        self.assertTrue(return_code == 0)
        self.assertTrue(not os.path.exists(filename_a))

    def test_run_script(self):
        filename_a, _ = self._setup_env()
        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: f'rm {filename_a}',
            )
        return_code, _ = task.run()
        self.assertTrue(return_code == 0)
        self.assertTrue(not os.path.exists(filename_a))

        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: 'echo IF YOU SEE THIS WITHOUT "echo" AT THE FRONT THEN THE TEST HAS FAILED',
            print_script_output=False,
            )
        return_code, _ = task.run()
        self.assertTrue(return_code == 0)

        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: 'echo IF YOU SEE THIS THEN THE TEST HAS FAILED',
            print_script_output=False,
            print_script=None,
            )
        return_code, _ = task.run()
        self.assertTrue(return_code == 0)


    def test_run_recipe_print(self):
        arr = []
        iostream = io.StringIO()
        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: arr.append(1),
            print_script=iostream,
            )
        recipe_str = 'lambda x1, x2, x3, x4: arr.append(1),'
        return_code, _ = task.run()
        self.assertTrue(return_code == 0)
        self.assertTrue(iostream.getvalue().strip() == recipe_str)
        iostream.close()

        iostream = io.StringIO()
        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: 'echo 1',
            print_script=iostream,
            )
        recipe_str = 'echo 1'
        return_code, _ = task.run()
        self.assertTrue(return_code == 0)
        self.assertTrue(iostream.getvalue().strip() == recipe_str)
        iostream.close()

    def test_run_recipe(self):
        arr = []
        task = maid.tasks.Task(
            'a',
            lambda x1, x2, x3, x4: arr.append(1),
            )
        return_code, _ = task.run()
        self.assertTrue(return_code == 0)
        self.assertTrue(len(arr) == 1)
        self.assertTrue(arr[0] == 1)

        task = maid.tasks.Task(
            'a',
            # `apped` is intentional so to trigger the error.
            lambda x1, x2, x3, x4: arr.apped(1),
            )
        with self.assertRaises(maid.tasks.PythonFunctionException):
            task.run()

    def test_run_recipe_inputs(self):
        arr = []
        task = maid.tasks.Task(
            'a',
            lambda rt, rf, t, mt: arr.extend([rt, rf, t, mt]),
            required_tasks=['b'],
            required_files=[__file__],
            targets=['c'],
            )
        modified_tasks = ['d']
        return_code, _ = task.run(modified_tasks=modified_tasks)
        self.assertTrue(return_code == 0)
        self.assertTrue(len(arr) == 4)
        self.assertTrue(arr[0] == task.required_tasks)
        self.assertTrue(arr[1] == tuple(task.get_required_files()))
        self.assertTrue(arr[2] == tuple(task.get_targets()))
        self.assertTrue(arr[3] == modified_tasks)

    def test_get_targets(self):
        self._test_get_filenames(maid.tasks.Task.get_targets)

        filename_a, _ = self._setup_env()
        targets = [os.path.join(os.path.dirname(filename_a), 'lei3827lei')]
        # Doesn't matter that file doesn't exist; shouldn't raise exception.
        task = self._make_task(targets, only_targets=True)
        self.assertTrue(len(list(task.get_targets())) == len(targets))
        self.assertTrue(list(task.get_targets())[0] == targets[0])

    def test_get_required_files(self):
        self._test_get_filenames(maid.tasks.Task.get_required_files)

        filename_a, _ = self._setup_env()
        targets = [os.path.join(os.path.dirname(filename_a), 'lei3827lei')]
        # Required file doesn't exist.
        with self.assertRaises(FileNotFoundError):
            _ = self._make_task(targets)

    def _test_get_filenames(self, func):
        filename_a, filename_b = self._setup_env()

        targets = [filename_a, filename_b]
        task = self._make_task(targets)
        task_targets = list(func(task))
        # Contents should be the same.
        self.assertTrue(len(task_targets) == len(targets))
        self.assertTrue(all(a == b for a, b in zip(targets, task_targets)))

        dirname = os.path.dirname(__file__)
        targets = [os.path.join(dirname, '*')]
        task = self._make_task(targets)
        task_targets = list(func(task))
        # Contents should be the same even when target is directory.
        self.assertTrue(len(os.listdir(dirname)) == len(task_targets))

        dirname = os.path.dirname(__file__) + '/'
        targets = [dirname]
        task = self._make_task(targets)
        task_targets = list(func(task))
        # Returned name should be `dirname` without training slash.
        self.assertTrue(len(task_targets) == len(targets))
        self.assertTrue(all(a[:-1] == b for a, b in zip(targets, task_targets)))

        dirname = os.path.dirname(__file__)
        targets = [filename_a, filename_b, os.path.join(dirname, '*')]
        task = self._make_task(targets)
        task_targets = list(func(task))
        # Contents should be the same when targets has mix of
        # directory and regular files.
        self.assertTrue((len(os.listdir(dirname)) + 2) == len(task_targets))

        if func == maid.tasks.Task.get_targets:
            dirname = os.path.dirname(__file__)
            targets = ['*', os.path.join(dirname, '*')]
            task = self._make_task(targets, only_targets=True)
            task_targets = list(func(task))
            # Wildcards are regular files unless a directory preceeds them.
            self.assertTrue((len(os.listdir(dirname)) + 1) == len(task_targets))

        dirname = os.path.dirname(__file__)
        targets = ['./*', os.path.join(dirname, '*')]
        task = self._make_task(targets)
        task_targets = list(func(task))
        # Contents should include `dirname/*` plus all of `./*`.
        self.assertTrue((len(os.listdir(dirname)) + 1) < len(task_targets))


if __name__ == '__main__':
    unittest.main()
