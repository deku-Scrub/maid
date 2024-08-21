import os
import io
import sys
import enum
import shutil
import pathlib
import hashlib
import functools
import itertools
import dataclasses
from dataclasses import dataclass, field
from typing import Optional, Iterable, Sequence, Callable, Any, Final

import maid.maids
import maid.compose
import maid.tasks
import maid.decorators


@maid.decorators.task(
        't1',
        )
def g(task: maid.tasks.Task) -> maid.compose.Recipe:
    return maid.compose.Recipe(
            script_stream=sys.stdout,
            output_stream=sys.stdout,
            ) \
                    | 'ls .' \
                    | (filter, lambda x: '.py' in x) \
                    | str.upper \
                    > 't1.txt'


#t = g
#print(t.recipe(t).run())
#print((maid.compose.Recipe(inputs=range(10)) | (sum,)).run())
print(maid.maids.get_maid().dry_run('t1', verbose=True))
print(maid.maids.get_maid().run('t1'))



@maid.decorators.task(
    'p1',
    required_files=['requirements.txt'],
    targets=['a.txt', 'b.txt'],
    cache_type=maid.tasks.CacheType.HASH,
    grouped=False,
)
def h(task: maid.tasks.Task) -> maid.compose.Recipe:
    print('from task p1', list(task.get_modified_files()))
    return maid.compose.Recipe(
            inputs=['lol\n', '.lol\n'],
            script_stream=sys.stdout,
            ) \
                    | "sed 's/lol/md/'" \
                    | "grep .md" \
                    | (lambda x: x.strip()+'?') \
                    | (lambda x: str(list(task.get_modified_files())) + x.strip()+'m') \
                    | "tr 'm' '!'" \
                    > task.targets[0]


@maid.decorators.task(
    'p2',
    required_files=['requirements.txt'],
    required_tasks=['p1'],
    cache_type=maid.tasks.CacheType.HASH,
)
def h2(task: maid.tasks.Task) -> maid.compose.Recipe:
    return maid.compose.Recipe(
            script_stream=sys.stdout,
            output_stream=sys.stdout,
            ) \
                    | f"echo looool {list(task.get_modified_files())}" \
                    | f"cat {task.required_tasks[0].targets[0]}"


@maid.decorators.task(
    'p3',
    required_tasks=['p2'],
    cache_type=maid.tasks.CacheType.HASH,
    tied_targets=('maid/**/*.py', '[^_]+.py', '(.+/)([^/]+)$', '', (r'\1\2',), r'logs/\2'),
)
def h3(task: maid.tasks.Task) -> maid.compose.Recipe:
    print(f'rf h3: {task.required_files}')
    print(f't h3: {task.targets}')
    print(f'mf h3: {task.get_modified_files()}')
    return maid.compose.Recipe(
            script_stream=sys.stdout,
            output_stream=sys.stdout,
            ) \
                    | 'mkdir -p logs' \
                    | f'touch {task.qjoin_targets()}'


@maid.decorators.task(
    'p4',
    required_tasks=['p3'],
    cache_type=maid.tasks.CacheType.HASH,
    is_default=True,
)
def h4(task: maid.tasks.Task) -> maid.compose.Recipe:
    print(f'h4')
    return maid.compose.Recipe(
            script_stream=sys.stdout,
            output_stream=sys.stdout,
            )




print(maid.maids.get_maid().dry_run(verbose=True), file=sys.stderr)
maid.maids.get_maid().run()
maid.maids.get_maid().save_graph('graph.png')

a = maid.compose.Recipe(inputs=(j for j in range(100))) \
    | (filter, lambda x: x % 3 == 0) \
    | 'parallel {args} "echo paraLOL; echo {{}}"'.format(args='--bar') \
    | 'grep -i lol' \
    | len \
    | 'wc -l'
# can probably use joblib as a step to parallelize python code in
# the same way gnu parallel can be a step to paralellize shell code:
# ```
#  | (lambda x: joblib.Parallel()(joblib.delayed(f)(xj) for xj in x),)
# ```
print(a)
print('task output: {}'.format(list(a.run())))

# example from https://github.com/pytoolz/toolz
import collections
import itertools
stem = lambda x: [w.lower().rstrip(",.!:;'-\"").lstrip("'\"") for w in x]
flatten = lambda x: (col for row in x for col in row)
counter: collections.Counter[str] = collections.Counter()
a = maid.compose.Recipe(inputs=['this cat jumped over this other cat!']) \
    | str.split \
    | stem \
    | counter.update
_ = list(a.run())
print(counter)
