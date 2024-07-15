import pathlib
import unittest
import shutil
import os

import maid.error_utils


class PublicApiTestCase(unittest.TestCase):

    def test_remove_files_and_throw(self):
        tmp_dir = 'tmp'
        if os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)

        filenames = [os.path.join(tmp_dir, 'a'), tmp_dir]
        pathlib.Path(filenames[0]).touch()
        with self.assertRaises(KeyError):
            maid.error_utils.remove_files_and_throw(
                    [filenames[0] + 'l'],
                    KeyError,
                    )
        # All files not given should still exist.
        self.assertTrue(all(os.path.exists(f) for f in filenames))

        with self.assertRaises(KeyError):
            maid.error_utils.remove_files_and_throw(
                    [filenames[0]],
                    KeyError,
                    )
        # Regular file should be gone.
        self.assertTrue(not os.path.exists(filenames[0]))
        pathlib.Path(filenames[0]).touch()

        with self.assertRaises(KeyError):
            maid.error_utils.remove_files_and_throw(
                    [filenames[1]],
                    KeyError,
                    )
        # Directory should be gone.
        self.assertTrue(not os.path.exists(filenames[1]))

        os.makedirs(filenames[1])
        pathlib.Path(filenames[0]).touch()
        with self.assertRaises(KeyError):
            maid.error_utils.remove_files_and_throw(
                    filenames,
                    KeyError,
                    )
        # All should be gone.
        self.assertTrue(all(not os.path.exists(f) for f in filenames))


if __name__ == '__main__':
    unittest.main()
