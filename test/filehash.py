import pathlib
import time
import unittest
import shutil
import os

import maid.monitor.hash
import maid.monitor.time


class PublicApiTestCase(unittest.TestCase):

    def _setup_env(self):
        '''
        '''
        tmp_dir = 'tmp'
        if os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)
        task_b_required_file = os.path.join(tmp_dir, 'rfb')
        pathlib.Path(task_b_required_file).touch()
        task_a_required_file = os.path.join(tmp_dir, 'rfa')
        pathlib.Path(task_a_required_file).touch()
        index_dir = os.path.join('.maid', 'index')
        for f in pathlib.Path(index_dir).glob('*'):
            os.remove(f)

        graph = {
            'a': maid.tasks.Task(
                    'a',
                    lambda i, o, t, mt: f'touch {t[0]}',
                    targets=[os.path.join(tmp_dir, 'a')],
                    required_files=[task_a_required_file],
                    ),
            'b': maid.tasks.Task(
                    'b',
                    lambda i, o, t, mt: f'touch {t[0]}',
                    targets=[os.path.join(tmp_dir, 'b')],
                    required_tasks=['a'],
                    required_files=[task_b_required_file],
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
        # Cases to test:
        #   * targets have no hashes
        #   * targets have new hashes
        #   * required files have no hashes
        #   * required files have new hashes
        #   * required task targets have no hashes
        #   * required task targets have new hashes

        self._test_targets_have_no_hashes()
        self._test_targets_have_new_hashes()
        self._test_required_files_have_no_hashes()
        self._test_required_files_have_new_hashes()
        self._test_required_tasks_have_no_hashes()
        self._test_required_tasks_have_new_hashes()

    def test_make_hashes(self):
        _ = self._setup_env()

        index_dir = os.path.join('.maid', 'index')
        # Index directory should be empty.
        with self.assertRaises(StopIteration):
            next(pathlib.Path(index_dir).glob('*'))

        filename = os.path.join('tmp', 'a')
        pathlib.Path(filename).touch()
        maid.monitor.hash.make_hashes([filename])
        files = list(pathlib.Path(index_dir).glob('*'))
        # Index directory should not be empty.
        self.assertTrue(len(files) == 1)

        contents = pathlib.Path(files[0]).read_text()
        old_hash, old_time = contents.split()
        # Should have two lines.
        self.assertTrue(len(contents.split()) == 2)
        # Second line should be an integer.
        self.assertTrue(int(old_time))

        maid.monitor.hash.make_hashes([filename])
        files = list(pathlib.Path(index_dir).glob('*'))
        # Index should still only have one file.
        self.assertTrue(len(files) == 1)

        new_hash, new_time = pathlib.Path(files[0]).read_text().split()
        # Contents should be identical since file was not modified.
        self.assertTrue(new_hash == old_hash)
        self.assertTrue(new_time == old_time)

        pathlib.Path(filename).write_text('.')
        maid.monitor.hash.make_hashes([filename])

        files = list(pathlib.Path(index_dir).glob('*'))
        # Index should still only have one file.
        self.assertTrue(len(files) == 1)

        new_hash, new_time = pathlib.Path(files[0]).read_text().split()
        # Contents should differ since file was modified.
        self.assertTrue(new_hash != old_hash)
        self.assertTrue(new_time != old_time)

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

    def _test_targets_have_new_hashes(self):
        '''
        '''
        graph = self._setup_env()
        should_task_run = maid.monitor.hash.should_task_run(graph)

        task = graph['a']
        maid.monitor.time.touch_files(task.get_required_files())
        maid.monitor.time.touch_files(task.get_targets())
        maid.monitor.hash.make_hashes(task.get_required_files())
        maid.monitor.hash.make_hashes(task.get_targets())
        # Shouldn't run since no modifications have not been made
        # after the above hash creations.
        self.assertTrue(len(should_task_run(task.name)) == 0)

        # Should run because the files have been modified.
        for f in task.get_targets():
            pathlib.Path(f).write_text('.')
        self.assertTrue(should_task_run(task.name))

        # Shouldn't run since new hashes have been stored.
        maid.monitor.hash.make_hashes(task.get_targets())
        self.assertTrue(len(should_task_run(task.name)) == 0)

    def _test_targets_have_no_hashes(self):
        '''
        '''
        graph = self._setup_env()
        should_task_run = maid.monitor.hash.should_task_run(graph)

        self.assertTrue(should_task_run('a'))
        maid.monitor.hash.make_hashes(graph['a'].get_required_files())
        self.assertTrue(should_task_run('a')) # Even if required files exist.
        self.assertTrue(should_task_run('b'))

        # Should run even though required tasks have updated.
        for task_name in graph['b'].required_tasks:
            task = graph[task_name]
            maid.monitor.hash.make_hashes(task.get_required_files())
            maid.monitor.time.touch_files(task.get_targets())
            maid.monitor.hash.make_hashes(task.get_targets())
            # Shouldn't run since required files and targets
            # are up to date.
            self.assertTrue(len(should_task_run(task_name)) == 0)
        self.assertTrue(should_task_run('b'))

    def _test_required_files_have_no_hashes(self):
        '''
        '''
        graph = self._setup_env()
        should_task_run = maid.monitor.hash.should_task_run(graph)

        task = graph['a']
        maid.monitor.time.touch_files(task.get_targets())
        maid.monitor.hash.make_hashes(task.get_targets())
        # Should run even though targets have been hashed because
        # required files have yet to be.
        self.assertTrue(should_task_run(task.name))

        # Shouldn't run since required files and targets have
        # been hashed.
        maid.monitor.hash.make_hashes(task.get_required_files())
        self.assertTrue(len(should_task_run(task.name)) == 0)


    def _test_required_files_have_new_hashes(self):
        '''
        '''
        graph = self._setup_env()
        should_task_run = maid.monitor.hash.should_task_run(graph)

        task = graph['a']
        maid.monitor.time.touch_files(task.get_targets())
        maid.monitor.time.touch_files(task.get_required_files())
        maid.monitor.hash.make_hashes(task.get_targets())
        maid.monitor.hash.make_hashes(task.get_required_files())
        # Shouldn't run because everything has been hashed.
        self.assertTrue(len(should_task_run(task.name)) == 0)

        # Should run because the required files have been modified.
        for f in task.get_required_files():
            pathlib.Path(f).write_text('.')
        self.assertTrue(should_task_run(task.name))


    def _test_required_tasks_have_no_hashes(self):
        '''
        '''
        graph = self._setup_env()
        should_task_run = maid.monitor.hash.should_task_run(graph)

        task = graph['b']
        maid.monitor.time.touch_files(task.get_targets())
        maid.monitor.time.touch_files(task.get_required_files())
        maid.monitor.hash.make_hashes(task.get_targets())
        maid.monitor.hash.make_hashes(task.get_required_files())
        # Shouldn't run because required tasks are missing hashes.
        self.assertTrue(len(should_task_run(task.name)) == 0)

    def _test_required_tasks_have_new_hashes(self):
        '''
        '''
        graph = self._setup_env()
        should_task_run = maid.monitor.hash.should_task_run(graph)

        task = graph['b']
        maid.monitor.time.touch_files(task.get_targets())
        maid.monitor.time.touch_files(task.get_required_files())
        maid.monitor.hash.make_hashes(task.get_targets())
        maid.monitor.hash.make_hashes(task.get_required_files())

        task = graph['a']
        maid.monitor.time.touch_files(task.get_targets())
        # Shouldn't run because the required tasks don't have hashes.
        self.assertTrue(len(should_task_run('b')) == 0)

        maid.monitor.hash.make_hashes(task.get_targets())
        # Should run because the required tasks have new hashes.
        self.assertTrue(should_task_run('b'))

        task = graph['b']
        for f in task.get_targets():
            pathlib.Path(f).write_text('..')
        maid.monitor.hash.make_hashes(task.get_targets())
        # Shouldn't run because the hashes are up to date.
        self.assertTrue(len(should_task_run(task.name)) == 0)

        task = graph['a']
        for f in task.get_targets():
            pathlib.Path(f).write_text('.')
        maid.monitor.hash.make_hashes(task.get_targets())
        # Should run because the required tasks have new hashes.
        self.assertTrue(should_task_run('b'))

        task = graph['b']
        for f in task.get_targets():
            pathlib.Path(f).write_text('.')
        maid.monitor.hash.make_hashes(task.get_targets())
        # Shouldn't run because the hashes are up to date.
        self.assertTrue(len(should_task_run(task.name)) == 0)

        maid.monitor.hash.make_hashes(graph['a'].get_targets())
        # Shouldn't run because the hashes are up to date; `a`'s
        # targets weren't modified.
        self.assertTrue(len(should_task_run(task.name)) == 0)

if __name__ == '__main__':
    unittest.main()
