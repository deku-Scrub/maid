import pathlib
import hashlib
import os
import base64
import time

import maid.files


# TODO: make this a config value.
index_dir = os.path.join('.maid', 'index')
os.makedirs(index_dir, exist_ok=True)


def make_hashes(filenames):
    for filename in filenames:
        if not _is_hash_changed(filename):
            continue

        filename_b64 = _get_b64_filename(filename)
        file_hash = _get_file_hash(filename)
        creation_time = time.time_ns()
        text = f'{file_hash}\n{creation_time}'
        pathlib.Path(filename_b64).write_text(text)


def _get_file_hash(filename):
    file_hash = ''
    with open(filename, mode='rb') as fis:
        hash_algo = hashlib.md5()
        while (bytes_read := fis.read(1024)):
            hash_algo.update(bytes_read)
        file_hash = hash_algo.hexdigest()
    return file_hash


# TODO: set a global config for `encoding`.
def _get_b64_filename(filename, encoding='utf-8'):
    return os.path.join(
            index_dir,
            base64.b64encode(filename.encode(encoding)).decode(encoding),
            )


def _hash_exists(filename):
    filename_b64 = _get_b64_filename(filename)
    if os.path.exists(filename) and os.path.exists(filename_b64):
        return filename_b64
    return ''


def _is_hash_changed(filename, must_exist=True):
    '''
    Checks if a file's hash has changed from its stored hash.

    Returns:
        When the file is not found or its hash has not been stored,
        then True if `must_exist` is True, otherwise False.

        When both the file exists and its hash has been stored,
        then regardless of the value of `must_exist`, True if
        the newly generated hash is different from the stored hash,
        False otherwise.
    '''
    if not (filename_b64 := _hash_exists(filename)):
        return must_exist

    file_hash = _get_file_hash(filename)
    stored_hash = pathlib.Path(filename_b64).read_text().split()[0]
    return file_hash != stored_hash


def _get_hash_creation_time(filename):
    if (filename_b64 := _hash_exists(filename)):
        return int(pathlib.Path(filename_b64).read_text().split()[1])
    return 0


def _is_newer(filename, timestamp):
    '''
    '''
    return _get_hash_creation_time(filename) > timestamp


def should_task_run(task):
    oldest_time = float('inf')
    # If any outputs or their hashes don't exist, the task should run.
    for f in maid.files.get_filenames(task.targets):
        if _is_hash_changed(f):
            return (task.name, '')
        if (hash_time := _get_hash_creation_time(f)) < oldest_time:
            oldest_time = hash_time

    # If any inputs have a new hash, the task should run.
    if any(_is_hash_changed(f) for f in maid.files.get_filenames(task.required_files)):
        return (task.name, '')

    # If any outputs of required tasks have a new hash, the
    # task should run.
    for req in task.required_tasks:
        for f in maid.files.get_filenames(req.targets):
            if _is_hash_changed(f, must_exist=False):
                return (req, '')
            if _is_newer(f, oldest_time):
                return (req, '')

    return tuple()
