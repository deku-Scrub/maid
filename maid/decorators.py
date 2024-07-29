from typing import IO, Callable, Sequence, Any
import sys

import maid.maid_utils
import maid.cache
import maid.compose.tasks

type TaskBuilder = Callable[[], maid.compose.tasks.Task]
type CommandDefiner = Callable[[maid.compose.tasks.Task], None]


def task(
        name: str,
        maid_name: str = maid.maid_utils.DEFAULT_MAID_NAME,
        inputs: Sequence[Any] = tuple(),
        required_files: Sequence[str] = tuple(),
        required_tasks: Sequence[TaskBuilder] = tuple(),
        targets: Sequence[str] = tuple(),
        cache: maid.cache.CacheType = maid.cache.CacheType.NONE,
        output_stream: IO = sys.stdout,
        script_stream: IO = sys.stderr,
        independent_targets: bool = False,
        is_default: bool = False,
        ) -> Callable[[CommandDefiner], TaskBuilder]:

    def build_task(define_commands: CommandDefiner) -> TaskBuilder:
        t = maid.compose.tasks.Task(
                name,
                maid_name=maid_name,
                inputs=inputs,
                required_files=required_files,
                targets=targets,
                cache=cache,
                is_default=is_default,
                build_task=define_commands if independent_targets else None,
                required_tasks=required_tasks,
                output_stream=output_stream,
                script_stream=script_stream,
                )
        # Let `define_commands` immediately create commands for the task.
        define_commands(t)
        _ = maid.maid_utils.get_maid(maid_name=maid_name).add_task(t)

        return lambda: t

    return build_task
