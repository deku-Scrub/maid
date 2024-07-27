import os
import pathlib
from typing import Iterable


def get_filenames(
        filenames: Iterable[str],
        must_exist: bool = True,
        ) -> Iterable[str]:
    for f in filenames:
        dirname, basename = os.path.split(f)

        # Whichever isn't empty is the basename.
        if not basename:
            yield dirname
        elif not dirname:
            yield basename
        else:
            has_files = False
            for p in pathlib.Path(dirname).glob(basename.replace('\\', '\\\\')):
                yield str(p)
                has_files = True
            if (not has_files) and must_exist:
                raise FileNotFoundError('Required file not found: {}'.format(f))
            if (not has_files) and (not must_exist):
                yield f
