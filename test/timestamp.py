import time
import unittest
import shutil
import os

import maid.monitor.time


class PublicApiTestCase(unittest.TestCase):
    def _setup_env(self):
        '''
        '''
        tmp_dir = 'tmp'
        if os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)

        graph = {
            'a': maid.tasks.Task(
                    'a',
                    lambda i, o, t, mt: f'touch {t[0]}',
                    targets=[os.path.join(tmp_dir, 'a')],
                    required_files=[__file__],
                    ),
            'b': maid.tasks.Task(
                    'b',
                    lambda i, o, t, mt: f'touch {t[0]}',
                    targets=[os.path.join(tmp_dir, 'b')],
                    required_tasks=['a'],
                    required_files=[__file__],
                    ),
            'c': maid.tasks.Task(
                    'c',
                    lambda i, o, t, mt: f'touch {t[0]}',
                    targets=[os.path.join(tmp_dir, 'c')],
                    required_tasks=['b'],
                    ),
        }
        return graph

    def test_should_task_run(self):
        # Three cases to test:
        #   * targets don't exist
        #   * required files newer than targets
        #   * required task targets newer than targets

        self._test_targets_dont_exist()
        self._test_required_files_newer_than_targets()
        self._test_required_tasks_newer_than_targets()
        return

    def test_touch_files(self):
        tmp_dir = 'tmp'
        if os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)
        filenames = [os.path.join(tmp_dir, 'a'), os.path.join(tmp_dir, 'b')]

        maid.monitor.time.touch_files(filenames)
        timestamps = []
        for f in filenames:
            self.assertTrue(os.path.exists(f))
            timestamps.append(os.path.getmtime(f))

        time.sleep(1)
        maid.monitor.time.touch_files(filenames)
        for f, t in zip(filenames, timestamps):
            self.assertTrue(os.path.getmtime(f) > t)

    def _test_required_tasks_newer_than_targets(self):
        '''
        '''
        graph = self._setup_env()
        should_task_run = maid.monitor.time.should_task_run(graph)

        task = graph['b']
        maid.monitor.time.touch_files(task.get_targets())
        # Shouldn't run since required task's targets don't exist.
        self.assertTrue(len(should_task_run(task.name)) == 0)

        time.sleep(1)
        maid.monitor.time.touch_files(graph['a'].get_targets())
        # Should run since required tasks are newer than targets.
        self.assertTrue(should_task_run(task.name))

        maid.monitor.time.touch_files(task.get_targets())
        # Shouldn't run since targets are newer than required tasks.
        self.assertTrue(len(should_task_run(task.name)) == 0)

    def _test_required_files_newer_than_targets(self):
        '''
        '''
        graph = self._setup_env()
        should_task_run = maid.monitor.time.should_task_run(graph)

        task = graph['a']
        maid.monitor.time.touch_files(task.get_targets())
        time.sleep(1)
        maid.monitor.time.touch_files(task.get_required_files())
        # Should run since required files are newer than targets.
        self.assertTrue(should_task_run(task.name))

        time.sleep(1)
        maid.monitor.time.touch_files(task.get_targets())
        # Shouldn't run since targets are newer than required files.
        self.assertTrue(len(should_task_run(task.name)) == 0)

        task = graph['b']
        maid.monitor.time.touch_files(task.get_targets())
        time.sleep(1)
        maid.monitor.time.touch_files(task.get_required_files())
        # Should run even though required task is updated since
        # required files are newer than targets.
        self.assertTrue(should_task_run(task.name))

        time.sleep(1)
        maid.monitor.time.touch_files(task.get_targets())
        # Shouldn't run since targets are newer than required files.
        self.assertTrue(len(should_task_run(task.name)) == 0)

    def _test_targets_dont_exist(self):
        '''
        '''
        graph = self._setup_env()
        should_task_run = maid.monitor.time.should_task_run(graph)

        self.assertTrue(should_task_run('a')) # Even if required files exist.
        self.assertTrue(should_task_run('b'))

        # Should run even though required tasks have updated.
        for task_name in graph['b'].required_tasks:
            task = graph[task_name]
            maid.monitor.time.touch_files(task.get_required_files())
            maid.monitor.time.touch_files(task.get_targets())
            # Shouldn't run since required files and targets
            # are up to date.
            self.assertTrue(len(should_task_run(task_name)) == 0)
        self.assertTrue(should_task_run('b'))


if __name__ == '__main__':
    unittest.main()
