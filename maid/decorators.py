from typing import Any, Callable

import maid2.tasks
import maid2.compose
import maid2.maids

type RecipeBuilder = Callable[[maid2.tasks.Task], maid2.compose.Recipe]


def task(
        **kwargs: Any,
        ) -> Callable[[RecipeBuilder], maid2.tasks.Task]:
    def f(g: RecipeBuilder) -> maid2.tasks.Task:
        t = maid2.tasks.Task(g, **kwargs)
        maid2.maids.get_maid().add_task(t)
        return t
    return f
