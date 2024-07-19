import pathlib
import time
import unittest
import shutil
import os

import maid.task_runner
import maid.tasks


class PublicApiTestCase(unittest.TestCase):

    def _setup_env(self):
        '''
        '''
        tmp_dir = 'tmp'
        if os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)
        index_dir = os.path.join('.maid', 'index')
        for f in pathlib.Path(index_dir).glob('*'):
            os.remove(f)
        a_tmp = os.path.join(tmp_dir, 'a_tmp')
        b_tmp = os.path.join(tmp_dir, 'b_tmp')
        c_tmp = os.path.join(tmp_dir, 'c_tmp')

        graph = {
            'a': maid.tasks.Task(
                    'a',
                    lambda i, o, t, mt: f'touch {t[0]}; echo 1 >> {a_tmp}',
                    targets=[os.path.join(tmp_dir, 'a')],
                    dont_run_if_all_targets_exist=True,
                    ),
            'b': maid.tasks.Task(
                    'b',
                    lambda i, o, t, mt: f'touch {t[0]}; echo 1 >> {b_tmp}',
                    targets=[os.path.join(tmp_dir, 'b')],
                    required_tasks=['a'],
                    always_run_if_in_graph=True,
                    ),
            'c': maid.tasks.Task(
                    'c',
                    lambda i, o, t, mt: f'touch {t[0]}; echo 1 >> {c_tmp}',
                    targets=[os.path.join(tmp_dir, 'c')],
                    required_tasks=['b'],
                    always_run_if_in_graph=True,
                    dont_run_if_all_targets_exist=True,
                    ),
        }
        return graph, a_tmp, b_tmp, c_tmp

    def test_run_dont_and_always_timestamp(self):
        self._test_run_dont_and_always(use_hash=False)

    def test_run_dont_and_always_filehash(self):
        self._test_run_dont_and_always(use_hash=True)
        self.assertTrue(len(os.listdir(os.path.join('.maid', 'index'))) == 3)

    def _test_run_dont_and_always(self, use_hash=False):
        graph, a_tmp, b_tmp, c_tmp = self._setup_env()

        workreq = maid.tasks.WorkRequest(
                'm',
                'c',
                dict(),
                graph,
                dict(),
                use_hash=use_hash,
                )
        maid.task_runner.run(workreq)
        self.assertTrue(
                all(
                    os.path.exists(f)
                    for t in graph.values()
                    for f in t.get_targets()
                    ))

        maid.task_runner.run(workreq)
        # Second run; shouldn't execute.
        self.assertTrue(len(pathlib.Path(a_tmp).read_text().split()) == 1)
        # Second run; should execute.
        self.assertTrue(len(pathlib.Path(b_tmp).read_text().split()) == 2)
        # Second run; shouldn't execute.
        self.assertTrue(len(pathlib.Path(c_tmp).read_text().split()) == 1)

    def test_run_startup_tasks(self):
        for use_hash in (True, False):
            graph, _, _, _ = self._setup_env()

            startup_graph = {
                'sa': maid.tasks.Task('sa', lambda a,b,c,d: startup.append(1)),
            }
            teardown_graph = {
                'ta': maid.tasks.Task('ta', lambda a,b,c,d: teardown.append(1)),
            }
            graph |= startup_graph
            graph |= teardown_graph
            startup = []
            teardown = []
            workreq = maid.tasks.WorkRequest(
                    'm',
                    'c',
                    startup_graph,
                    graph,
                    teardown_graph,
                    use_hash=use_hash,
                    )
            maid.task_runner.run(workreq)
            self.assertTrue(startup == [1])
            self.assertTrue(teardown == [1])

    def test_run_update_requested_few(self):
        graph, _, _, _ = self._setup_env()
        workreq = maid.tasks.WorkRequest(
                'm',
                'b',
                [],
                graph,
                [],
                update_requested=True,
                )
        maid.task_runner.run(workreq)
        self.assertTrue(len(os.listdir(os.path.join('.maid', 'index'))) == 2)
        self.assertTrue(
                all(
                    os.path.exists(f)
                    for t in (graph['a'], graph['b'])
                    for f in t.get_targets()
                    ))
        self.assertTrue(
                all(
                    not os.path.exists(f)
                    for t in (graph['c'],)
                    for f in t.get_targets()
                    ))

    def test_run_update_requested_all(self):
        graph, _, _, _ = self._setup_env()
        workreq = maid.tasks.WorkRequest(
                'm',
                'c',
                [],
                graph,
                [],
                update_requested=True,
                )
        maid.task_runner.run(workreq)
        self.assertTrue(len(os.listdir(os.path.join('.maid', 'index'))) == 3)
        self.assertTrue(
                all(
                    os.path.exists(f)
                    for t in graph.values()
                    for f in t.get_targets()
                    ))

    def test_run_modified_tasks(self):
        graph, _, _, _ = self._setup_env()
        modified_tasks = []
        graph |= {
            'newnode': maid.tasks.Task(
                    'newnode',
                    lambda i, o, t, mt: modified_tasks.extend(mt),
                    required_tasks=['a', 'c'],
                    )
        }
        workreq = maid.tasks.WorkRequest(
                'm',
                'newnode',
                [],
                graph,
                [],
                )
        maid.task_runner.run(workreq)
        self.assertTrue(len(modified_tasks) == len(graph['newnode'].required_tasks))
        self.assertTrue(
                all(
                    r in modified_tasks
                    for r in graph['newnode'].required_tasks))

    def test_run_finish_depth_on_failure(self):
        _, _, _, _ = self._setup_env()
        arr_a = []
        arr_b = []
        arr_c = []
        arr_d = []
        # The `x`s are intentional to produce an error.
        graph = {
            'a': maid.tasks.Task(
                    'a',
                    lambda i, o, t, mt: (arr_a.append(1), x),
                    always_run_if_in_graph=True,
                    ),
            'b': maid.tasks.Task(
                    'b',
                    lambda i, o, t, mt: (arr_b.append(1), x),
                    always_run_if_in_graph=True,
                    ),
            'c': maid.tasks.Task(
                    'c',
                    lambda i, o, t, mt: (arr_c.append(1), x),
                    always_run_if_in_graph=True,
                    ),
            'd': maid.tasks.Task(
                    'd',
                    lambda i, o, t, mt: arr_d.append(1),
                    required_tasks=['a', 'b', 'c'],
                    ),
            'e': maid.tasks.Task(
                    'e',
                    lambda i, o, t, mt: 'lsielkdj92879ei7elksix',
                    always_run_if_in_graph=True,
                    ),
        }

        workreq = maid.tasks.WorkRequest(
                'm',
                'e',
                [],
                graph,
                [],
                )
        with self.assertRaises(maid.tasks.ScriptException):
            maid.task_runner.run(workreq)
        self.assertTrue(arr_a == [])
        self.assertTrue(arr_b == [])
        self.assertTrue(arr_c == [])
        self.assertTrue(arr_d == [])

        workreq = maid.tasks.WorkRequest(
                'm',
                'd',
                [],
                graph,
                [],
                finish_depth_on_failure=True,
                )
        with self.assertRaises(maid.tasks.PythonFunctionException):
            maid.task_runner.run(workreq)
        self.assertTrue(arr_a == [1])
        self.assertTrue(arr_b == [1])
        self.assertTrue(arr_c == [1])
        self.assertTrue(arr_d == [])

        workreq = maid.tasks.WorkRequest(
                'm',
                'd',
                [],
                graph,
                [],
                )
        with self.assertRaises(maid.tasks.PythonFunctionException):
            maid.task_runner.run(workreq)
        self.assertTrue(arr_a == [1, 1])
        self.assertTrue(arr_b == [1])
        self.assertTrue(arr_c == [1])
        self.assertTrue(arr_d == [])


    def test_run_missing_expected_targets(self):
        _, a_tmp, _, _ = self._setup_env()
        a_arr = []
        b_arr = []
        graph = {
            'a': maid.tasks.Task(
                    'a',
                    lambda i, o, t, mt: a_arr.append(1),
                    targets=[a_tmp],
                    ),
            'b': maid.tasks.Task(
                    'b',
                    lambda i, o, t, mt: b_arr.append(1),
                    always_run_if_in_graph=True,
                    ),
            'c': maid.tasks.Task(
                    'c',
                    lambda i, o, t, mt: '',
                    required_tasks=['a', 'b'],
                    always_run_if_in_graph=True,
                    ),
        }

        workreq = maid.tasks.WorkRequest(
                'm',
                'a',
                [],
                graph,
                [],
                )
        with self.assertRaises(maid.task_runner.MissingExpectedTargetsException):
            maid.task_runner.run(workreq)
        self.assertTrue(a_arr == [1])

        workreq = maid.tasks.WorkRequest(
                'm',
                'c',
                [],
                graph,
                [],
                finish_depth_on_failure=True,
                )
        with self.assertRaises(maid.task_runner.MissingExpectedTargetsException):
            maid.task_runner.run(workreq)
        self.assertTrue(a_arr == [1, 1])
        self.assertTrue(b_arr == [1])

    def test_run_tasknotfoundexception(self):
        _, _, _, _ = self._setup_env()
        graph = {
            'a': maid.tasks.Task(
                    'a',
                    lambda i, o, t, mt: '',
                    always_run_if_in_graph=True,
                    ),
            'b': maid.tasks.Task(
                    'b',
                    lambda i, o, t, mt: '',
                    required_tasks=['c'],
                    always_run_if_in_graph=True,
                    ),
        }
        workreq = maid.tasks.WorkRequest(
                'm',
                'newnode',
                [],
                graph,
                [],
                )
        with self.assertRaises(maid.task_runner.TaskNotFoundException):
            maid.task_runner.run(workreq)

    def test_run_no_changes(self):
        _, a_filename, b_filename, _ = self._setup_env()
        a_arr = []
        b_arr = []
        graph = {
            'a': maid.tasks.Task(
                    'a',
                    lambda i, o, t, mt: (a_arr.append(1), f'touch {t[0]}')[1],
                    targets=[a_filename],
                    ),
            'b': maid.tasks.Task(
                    'b',
                    lambda i, o, t, mt: (b_arr.append(1), f'touch {t[0]}')[1],
                    targets=[b_filename],
                    required_tasks=['a'],
                    ),
        }
        workreq = maid.tasks.WorkRequest(
                'm',
                'b',
                [],
                graph,
                [],
                )
        maid.task_runner.run(workreq)
        self.assertTrue(a_arr == [1])
        self.assertTrue(b_arr == [1])
        self.assertTrue(
                all(
                    os.path.exists(f)
                    for t in graph.values()
                    for f in t.get_targets()
                    ))

        maid.task_runner.run(workreq)
        self.assertTrue(a_arr == [1])
        self.assertTrue(b_arr == [1])
        self.assertTrue(
                all(
                    os.path.exists(f)
                    for t in graph.values()
                    for f in t.get_targets()
                    ))

    def test_run_subgraphs_time(self):
        def _setup():
            _, _, _, _ = self._setup_env()
            filenames = [os.path.join('tmp', str(j)) for j in range(8)]
            arr = [[] for j in range(8)]
            return filenames, arr

        filenames, arr = _setup()
        graph = {
            '0': maid.tasks.Task(
                    '0',
                    lambda i, o, t, _: (arr[0].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[0]],
                    ),
            '1': maid.tasks.Task(
                    '1',
                    lambda i, o, t, _: (arr[1].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[1]],
                    ),
            '2': maid.tasks.Task(
                    '2',
                    lambda i, o, t, _: (arr[2].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[2]],
                    required_tasks=['1'],
                    ),
            '3': maid.tasks.Task(
                    '3',
                    lambda i, o, t, _: (arr[3].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[3]],
                    required_tasks=['2'],
                    ),
            '4': maid.tasks.Task(
                    '4',
                    lambda i, o, t, _: (arr[4].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[4]],
                    required_tasks=['2'],
                    ),
            '5': maid.tasks.Task(
                    '5',
                    lambda i, o, t, _: (arr[5].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[5]],
                    required_tasks=['3', '4'],
                    ),
            '6': maid.tasks.Task(
                    '6',
                    lambda i, o, t, _: (arr[6].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[6]],
                    required_tasks=['1', '5', '7'],
                    ),
            '7': maid.tasks.Task(
                    '7',
                    lambda i, o, t, _: (arr[7].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[7]],
                    required_tasks=['0'],
                    ),
        }

        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [])
        maid.task_runner.run(workreq)
        # All tasks should run since `6` depends on all of them.
        self.assertTrue(all(a == [1] for a in arr))

        filenames, arr = _setup()
        # All tasks should run once since they're unmodified after
        # the first time.
        for j in range(len(filenames)):
            workreq = maid.tasks.WorkRequest('m', str(j), [], graph, [])
            maid.task_runner.run(workreq)
            self.assertTrue(all(a == [1] for a in arr[:(j + 1)]))
            if j != 6:
                self.assertTrue(all(a == [] for a in arr[(j + 1):]))
            else:
                self.assertTrue(all(a == [1] for a in arr[7:]))

        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [])
        maid.task_runner.run(workreq)
        time.sleep(1)
        pathlib.Path(filenames[4]).touch()
        # All tasks that 4 depends on (0,1,2,3), including itself,
        # should run once, 5 and 6 should run twice.  Task 7 isn't
        # run.
        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [])
        maid.task_runner.run(workreq)
        self.assertTrue(all(a == [1] for a in arr[:5]))
        self.assertTrue(all(a == [1] for a in arr[7:]))
        self.assertTrue(all(a == [1, 1] for a in arr[5:7]))

        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [])
        maid.task_runner.run(workreq)
        time.sleep(1)
        pathlib.Path(filenames[0]).touch()
        # Task 7 should run since it depends on task 0 which was
        # modified.
        workreq = maid.tasks.WorkRequest('m', '7', [], graph, [])
        maid.task_runner.run(workreq)
        self.assertTrue(all(a == [1] for a in arr[:5]))
        self.assertTrue(all(a == [1, 1] for a in arr[7:]))
        self.assertTrue(all(a == [1, 1] for a in arr[5:7]))

        filenames, arr = _setup()
        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [])
        maid.task_runner.run(workreq)
        os.remove(filenames[4])
        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [])
        maid.task_runner.run(workreq)
        # All tasks that depend on 4, including 4, should run twice.
        # Everything just once.
        self.assertTrue(all(a == [1] for a in arr[:4]))
        self.assertTrue(all(a == [1] for a in arr[7:]))
        self.assertTrue(all(a == [1, 1] for a in arr[4:7]))

    def test_run_subgraphs_hash(self):
        def _setup():
            _, _, _, _ = self._setup_env()
            filenames = [os.path.join('tmp', str(j)) for j in range(8)]
            arr = [[] for j in range(8)]
            return filenames, arr

        filenames, arr = _setup()
        graph = {
            '0': maid.tasks.Task(
                    '0',
                    lambda i, o, t, _: (arr[0].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[0]],
                    ),
            '1': maid.tasks.Task(
                    '1',
                    lambda i, o, t, _: (arr[1].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[1]],
                    ),
            '2': maid.tasks.Task(
                    '2',
                    lambda i, o, t, _: (arr[2].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[2]],
                    required_tasks=['1'],
                    ),
            '3': maid.tasks.Task(
                    '3',
                    lambda i, o, t, _: (arr[3].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[3]],
                    required_tasks=['2'],
                    ),
            '4': maid.tasks.Task(
                    '4',
                    lambda i, o, t, _: (arr[4].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[4]],
                    required_tasks=['2'],
                    ),
            '5': maid.tasks.Task(
                    '5',
                    lambda i, o, t, _: (arr[5].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[5]],
                    required_tasks=['3', '4'],
                    ),
            '6': maid.tasks.Task(
                    '6',
                    lambda i, o, t, _: (arr[6].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[6]],
                    required_tasks=['1', '5', '7'],
                    ),
            '7': maid.tasks.Task(
                    '7',
                    lambda i, o, t, _: (arr[7].append(1), f'touch {t[0]}')[1],
                    targets=[filenames[7]],
                    required_tasks=['0'],
                    ),
        }

        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [], use_hash=True)
        maid.task_runner.run(workreq)
        # All tasks should run since `6` depends on all of them.
        self.assertTrue(all(a == [1] for a in arr))

        filenames, arr = _setup()
        # All tasks should run once since they're unmodified after
        # the first time.
        for j in range(len(filenames)):
            workreq = maid.tasks.WorkRequest('m', str(j), [], graph, [], use_hash=True)
            maid.task_runner.run(workreq)
            self.assertTrue(all(a == [1] for a in arr[:(j + 1)]))
            if j != 6:
                self.assertTrue(all(a == [] for a in arr[(j + 1):]))
            else:
                self.assertTrue(all(a == [1] for a in arr[7:]))

        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [], use_hash=True)
        maid.task_runner.run(workreq)
        pathlib.Path(filenames[4]).write_text('.')
        # All tasks that 4 depends on (0,1,2,3), not including itself,
        # should run once.  4, 5 and 6 should run twice.  Task 7 isn't
        # run.
        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [], use_hash=True)
        maid.task_runner.run(workreq)
        self.assertTrue(all(a == [1] for a in arr[:4]))
        self.assertTrue(all(a == [1] for a in arr[7:]))
        self.assertTrue(all(a == [1, 1] for a in arr[4:7]))

        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [], use_hash=True)
        maid.task_runner.run(workreq)
        pathlib.Path(filenames[0]).write_text('.')
        # Tasks 0, 5, 6, and 7 should run since they depends on task
        # 0 which was modified.
        workreq = maid.tasks.WorkRequest('m', '7', [], graph, [], use_hash=True)
        maid.task_runner.run(workreq)
        self.assertTrue(arr[0] == [1, 1])
        self.assertTrue(arr[5] == [1, 1, 1])
        self.assertTrue(arr[6] == [1, 1, 1])
        self.assertTrue(arr[7] == [1, 1])
        self.assertTrue(all(a == [1] for a in arr[1:4]))

        filenames, arr = _setup()
        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [], use_hash=True)
        maid.task_runner.run(workreq)
        os.remove(filenames[4])
        workreq = maid.tasks.WorkRequest('m', '6', [], graph, [], use_hash=True)
        maid.task_runner.run(workreq)
        # All tasks that depend on 4, including 4, should run twice.
        # Everything just once.
        self.assertTrue(all(a == [1] for a in arr[:4]))
        self.assertTrue(all(a == [1] for a in arr[7:]))
        self.assertTrue(all(a == [1, 1] for a in arr[4:7]))


if __name__ == '__main__':
    unittest.main()
