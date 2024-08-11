from typing import Any, Callable

import maid.tasks
import maid.compose
import maid.maids

type RecipeBuilder = Callable[[maid.tasks.Task], maid.compose.Recipe]


def task(
        **kwargs: Any,
        ) -> Callable[[RecipeBuilder], maid.tasks.Task]:
    def f(g: RecipeBuilder) -> maid.tasks.Task:
        t = maid.tasks.Task(g, **kwargs)
        maid.maids.get_maid().add_task(t)
        return t
    return f
