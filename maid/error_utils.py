import shutil
import os
from typing import Iterable


def remove_files_and_throw(filenames: Iterable[str], err: Exception) -> None:
    for f in filenames:
        if os.path.isdir(f):
            shutil.rmtree(f)
        elif os.path.exists(f):
            os.remove(f)
    raise err
