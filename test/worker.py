import pathlib
import time
import unittest
import shutil
import os

import maid.worker
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

    def test_get_maid(self):
        self.assertTrue(maid.worker.get_maid().name == 'Default maid')
        self.assertTrue(maid.worker.get_maid('m').name == 'm')

    def test_exists(self):
        self.assertTrue(not maid.worker.exists('Default maid'))
        self.assertTrue(not maid.worker.exists('m'))
        maid_1 = maid.worker.get_maid()
        maid_2 = maid.worker.get_maid('m')
        self.assertTrue(maid.worker.exists(maid_1.name))
        self.assertTrue(maid.worker.exists(maid_2.name))
        self.assertTrue(not maid.worker.exists('maid'))
        self.assertTrue(not maid.worker.exists(''))

    def test_describe_maids(self):
        maid.worker.describe_maids()
        maid.worker.describe_maids(['m1', 'm2'])
        _ = maid.worker.get_maid('m1')
        _ = maid.worker.get_maid('m2')
        maid.worker.describe_maids()
        maid.worker.describe_maids(['m1', 'm2'])

        made_it_without_error = True
        self.assertTrue(made_it_without_error)


    def test_describe_tasks(self):
        maid.worker.describe_tasks()
        maid.worker.describe_tasks(['m1', 'm2'])
        _ = maid.worker.get_maid('m1')
        _ = maid.worker.get_maid('m2')
        maid.worker.describe_tasks()
        maid.worker.describe_tasks(['m1', 'm2'])

        made_it_without_error = True
        self.assertTrue(made_it_without_error)

    def test_add_task(self):
        arr = []
        maid_1 = maid.worker.get_maid('m1')
        maid_1.add_task(
                't1',
                lambda a, b, c, d: arr.append(1),
                always_run_if_in_graph=True,
                )
        maid_1.add_task(
                't2',
                lambda a, b, c, d: arr.append(2),
                required_tasks=['t1'],
                always_run_if_in_graph=True,
                print_script=None,
                print_script_output=None,
                dont_run_if_all_targets_exist=True,
                )
        maid_1.add_task(
                't2.1',
                lambda a, b, c, d: arr.append(21),
                required_tasks=['t2'],
                always_run_if_in_graph=True,
                print_script=None,
                print_script_output=None,
                is_default=True,
                )
        maid_1.add_task(
                't3',
                lambda a, b, c, d: arr.append(3),
                run_at_start=True,
                )
        maid_1.add_task(
                't4',
                lambda a, b, c, d: arr.append(4),
                run_at_end=True,
                )
        maid_1.work()

        # `2` is missing because task `t2` doesn't run -- the
        # `dont_run_if_all_targets_exist` causes the task to
        # not run when the task has no targets.  That is, it's
        # a vacuous truth.
        self.assertTrue(arr == [3, 1, 21, 4])

    def test_work(self):
        _, filename, _, _ = self._setup_env()
        tmp_dir = os.path.dirname(filename)

        arr = []
        maid_1 = maid.worker.get_maid('ma1')
        maid_1.add_task(
                't1',
                lambda a, b, c, d: (arr.append(1), f'touch {c[0]}')[1],
                targets=[os.path.join(tmp_dir, 't1')],
                )
        maid_1.add_task(
                't2',
                lambda a, b, c, d: (arr.append(2), f'touch {c[0]}')[1],
                targets=[os.path.join(tmp_dir, 't2')],
                required_tasks=['t1'],
                )
        maid_1.add_task(
                't3',
                lambda a, b, c, d: (arr.append(3), f'touch {c[0]}')[1],
                targets=[os.path.join(tmp_dir, 't3')],
                run_at_start=True,
                )
        maid_1.add_task(
                't4',
                lambda a, b, c, d: (arr.append(4), f'touch {c[0]}')[1],
                targets=[os.path.join(tmp_dir, 't4')],
                run_at_end=True,
                )
        maid_1.work('t2', dry_run=True)
        self.assertTrue(not arr)

        maid_1.work('t2', use_hash=True)
        self.assertTrue(len(os.listdir(tmp_dir)) == 4)
        self.assertTrue(len(os.listdir(os.path.join('.maid', 'index'))) == 4)

        _ = self._setup_env()
        maid_1.work('t2', update_requested=True)
        self.assertTrue(len(os.listdir(tmp_dir)) == 4)
        self.assertTrue(len(os.listdir(os.path.join('.maid', 'index'))) == 4)

        _ = self._setup_env()
        maid_1.work('t2')
        #maid_1.work('t2', finish_depth_on_failure=True)
        self.assertTrue(len(os.listdir(tmp_dir)) == 4)
        self.assertTrue(len(os.listdir(os.path.join('.maid', 'index'))) == 0)

        _ = self._setup_env()
        arr = []
        maid_1 = maid.get_maid('maid182')
        maid_1.add_task(
                'task5',
                # Intentional error in the seconds element.
                lambda a, b, c, d: (arr.append(5), '', st.slwowajdj)[1],
                always_run_if_in_graph=True,
                )
        maid_1.add_task(
                'task6',
                # Intentional error in the seconds element.
                lambda a, b, c, d: (arr.append(6), '', st.slwowajdj)[1],
                always_run_if_in_graph=True,
                )
        maid_1.add_task(
                'task7',
                lambda a, b, c, d: arr.append(7),
                required_tasks=['task6', 'task5'],
                always_run_if_in_graph=True,
                )
        with self.assertRaises(maid.tasks.PythonFunctionException):
            maid_1.work('task7', finish_depth_on_failure=True)
        self.assertTrue(len(arr) == 2)
        self.assertTrue((5 in arr) and (6 in arr))


if __name__ == '__main__':
    unittest.main()
