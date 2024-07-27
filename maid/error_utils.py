import shutil
import os


def remove_files_and_throw(filenames: str, err: Exception) -> None:
    for f in filenames:
        if os.path.isdir(f):
            shutil.rmtree(f)
        elif os.path.exists(f):
            os.remove(f)
    raise err
