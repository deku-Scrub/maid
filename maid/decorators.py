from typing import Any, Callable, Iterable

import maid.tasks
import maid.compose
import maid.maids

type RecipeBuilder = Callable[[maid.tasks.Task], maid.compose.Recipe]


def task(
        name: str,
        /,
        required_tasks: Iterable[str] = tuple(),
        **kwargs: Any,
        ) -> Callable[[RecipeBuilder], RecipeBuilder]:
    def f(g: RecipeBuilder) -> RecipeBuilder:
        kwargs['required_tasks'] = tuple(maid.maids.get_maid().get_task(t) for t in required_tasks)
        t = maid.tasks.Task(g, name=name, **kwargs)
        maid.maids.get_maid().add_task(t)
        return g
    return f
