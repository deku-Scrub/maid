import subprocess
import itertools
from typing import Mapping, Final, Iterable

import maid.tasks
import maid.compose

type TaskGraph = dict[maid.tasks.RunPhase, dict[str, maid.tasks.Task]]

def _try_running_dot() -> None:
    # Raise exception if `dot` not found (or behaves unexpectedly).
    proc = subprocess.run(['dot', '-V'], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError('Command `dot` for saving graphs found but behaved unexpectedly: {}'.format(proc.stderr))

def _get_run_phase_colors() -> Mapping[maid.tasks.RunPhase, str]:
    return {
            maid.tasks.RunPhase.NORMAL: 'black',
            maid.tasks.RunPhase.START: 'blue',
            maid.tasks.RunPhase.END: 'green',
            maid.tasks.RunPhase.FINALLY: 'red',
            }

def _define_nodes(task_graph: TaskGraph) -> Iterable[str]:
    run_phase_to_color: Final[Mapping[maid.tasks.RunPhase, str]] = _get_run_phase_colors()
    return (
            '{task_name} [color={color}];\n'.format(
                task_name=task_name,
                color=run_phase_to_color.get(run_phase, 'white'),
                )
            for run_phase, graph, in task_graph.items()
            for task_name in graph
            )

def _define_edges(task_graph: TaskGraph) -> Iterable[str]:
    return (
            f'{required_task.name} -> {task.name};\n'
            for graph in task_graph.values()
            for task in graph.values()
            for required_task in task.required_tasks
            )


def save_graph(task_graph: TaskGraph, outfile: str) -> None:
    _try_running_dot()

    recipe = maid.compose.Recipe(inputs=itertools.chain(
        ('digraph maid_graph {\n',),
        _define_nodes(task_graph),
        _define_edges(task_graph),
        ('}',),
        ))

    (recipe | f'dot -Tpng > {outfile}').exhaust()
