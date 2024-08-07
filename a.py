import itertools
import enum
import sys

import maid.cache


@maid.task(
    'p1',
    inputs=['lol\n', '.lol\n'],
    required_files=['requirements.txt'],
    targets=['a.txt', 'b.txt'],
    cache=maid.cache.CacheType.HASH,
    script_stream=sys.stdout,
    independent_targets=True,
)
def h(a):
    a \
    | "sed 's/lol/md/'" \
    | "grep .md" \
    | (lambda x: x.strip()+'?') \
    | (lambda x: x.strip()+'m') \
    | "tr 'm' '!'" \
    > a.targets[0]

@maid.task(
    'p2',
    required_tasks=[h],
    output_stream=sys.stdout,
    script_stream=sys.stderr,
    is_default=True,
)
def h2(a):
    a | f"cat {a.required_tasks['p1'].targets[0]}"

print(maid.get_maid().dry_run(verbose=True), file=sys.stderr)
sys.stdout.writelines(maid.get_maid().run())

a = maid.Task(inputs=(j for j in range(100))) \
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
print(a.dry_run(True))
print('task output: {}'.format(list(a.run())))

# example from https://github.com/pytoolz/toolz
import collections
import itertools
stem = lambda x: [w.lower().rstrip(",.!:;'-\"").lstrip("'\"") for w in x]
flatten = lambda x: (col for row in x for col in row)
counter = collections.Counter()
a = maid.Task(inputs=['this cat jumped over this other cat!']) \
    | str.split \
    | stem \
    | counter.update
_ = list(a.run())
print(counter)
