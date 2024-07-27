from typing import Self, Optional, Iterable


class DependecyGraphTask:

    def __init__(
            self,
            name: str = '', # o
            *,
            required_tasks: Optional[Iterable[str]] = None, # o
            required_files: Optional[Iterable[str]] = None, # o
            targets: Optional[Iterable[str]] = None, # o
            ):
        self.name: str = ''
        self.required_tasks: dict[str, Self] = dict()
        self.required_files: tuple[str] = []
        self.targets: tuple[str] = []
