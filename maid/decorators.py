from typing import Iterable, IO, Callable
import sys

import maid
import maid.cache
import maid.compose.tasks


def task[T](
        name: str,
        maid_name: str = maid.DEFAULT_MAID_NAME,
        inputs: Iterable[T] = None,
        required_files: Iterable = tuple(),
        required_tasks: Iterable = tuple(),
        targets: tuple = tuple(),
        cache: maid.cache.CacheType = maid.cache.CacheType.NONE,
        output_stream: IO = sys.stdout,
        script_stream: IO = sys.stderr,
        independent_targets: bool = False,
        is_default: bool = False,
        ) -> Callable[
            [Callable[[maid.compose.tasks.Task], None]],
            Callable[[], maid.compose.tasks.Task]
        ]:

    def build_task(
            define_commands: Callable[[maid.compose.tasks.Task], None]
            ) -> Callable[[], maid.compose.tasks.Task]:
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
        _ = maid.get_maid(maid_name=maid_name).add_task(t)

        return lambda: t

    return build_task


