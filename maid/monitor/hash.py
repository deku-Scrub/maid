import pathlib
import hashlib
import os
import base64


# TODO: make this a config value.
index_dir = os.path.join('.maid', 'index')
os.makedirs(index_dir, exist_ok=True)


def make_hashes(filenames):
    for filename in filenames:
        file_hash = _get_file_hash(filename)
        filename_b64 = _get_b64_filename(filename)
        pathlib.Path(filename_b64).write_text(file_hash)


def _get_file_hash(filename):
    file_hash = ''
    with open(filename, mode='rb') as fis:
        hash_algo = hashlib.md5()
        while (bytes_read := fis.read(1024)):
            hash_algo.update(bytes_read)
        file_hash = hash_algo.hexdigest()
    return file_hash


def _get_b64_filename(filename, encoding='utf-8'):
    return os.path.join(
            index_dir,
            base64.b64encode(filename.encode(encoding)).decode(encoding),
            )


def _is_hash_changed(filename, encoding='utf-8', must_exist=True):
    filename_b64 = _get_b64_filename(filename, encoding)

    files_exist = os.path.exists(filename) and os.path.exists(filename_b64)
    if not files_exist:
        if must_exist:
            return True
        return False

    file_hash = _get_file_hash(filename)
    stored_hash = pathlib.Path(filename_b64).read_text()
    return file_hash != stored_hash


def should_task_run(graph):
    def _should_it(task_name):
        task = graph[task_name]

        # If any outputs or their hashes don't exist, the task should run.
        if any(_is_hash_changed(f) for f in task.get_targets()):
            return (task_name, '')

        # If any inputs have a new hash, the task should run.
        if any(_is_hash_changed(f) for f in task.get_required_files()):
            return (task_name, '')

        # If any outputs of required tasks have a new hash, the
        # task should run.
        for req in task.required_tasks:
            if any(_is_hash_changed(f, must_exist=False) for f in graph[req].get_targets()):
                return (req, '')

        return tuple()

    return _should_it
