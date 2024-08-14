from typing import Protocol, Iterable, Callable, Optional, Any


class Comparable(Protocol):

    def __eq__(self, a: Any, /) -> bool: ...
    def __lt__(self, a: Any, /) -> bool: ...
    def __gt__(self, a: Any, /) -> bool: ...



def difference[T: Comparable](
        a: Iterable[T],
        b: Iterable[T],
        ) -> Iterable[T]:
    '''
    Does not remove duplicates.
    '''
    return _setop_template(
            a,
            b,
            a_only=lambda aj, a: (xj for x in ((aj,), a) for xj in x),
            b_only=lambda bj, b: tuple(),
            eq=lambda aj, bj: tuple(),
            lt=lambda aj, bj: (aj,),
            gt=lambda aj, bj: tuple(),
            )


def _setop_template[T: Comparable](
        a: Iterable[T],
        b: Iterable[T],
        /,
        a_only: Callable[[T, Iterable[T]], Iterable[T]],
        b_only: Callable[[T, Iterable[T]], Iterable[T]],
        eq: Callable[[T, T], Iterable[T]],
        lt: Callable[[T, T], Iterable[T]],
        gt: Callable[[T, T], Iterable[T]],
        ) -> Iterable[T]:
    '''
    Does not remove duplicates.
    Does not support None.
    '''
    # Preferrably -- and for cleanliness -- this function would be
    # recursive, but Python does not handle recursion efficiently,
    # especially when involving generators.
    aj: Optional[T] = next(iter(a), None)
    bj: Optional[T] = next(iter(b), None)
    while True:
        match (aj, bj):
            case (None, None):
                return
            case (None, bj) if bj is not None: # Guard needed for mypy.
                yield from b_only(bj, b)
                return
            case (aj, None) if aj is not None: # Guard needed for mypy.
                yield from a_only(aj, a)
                return
            case _ if (aj is not None) and (bj is not None): # Again... mypy.
                if aj < bj:
                    yield from lt(aj, bj)
                    aj = next(iter(a), None)
                elif aj > bj:
                    yield from gt(aj, bj)
                    bj = next(iter(b), None)
                elif aj == bj:
                    yield from eq(aj, bj)
                    aj = next(iter(a), None)
                    bj = next(iter(b), None)


#for j in difference((2*j+1 for j in range(1000000)),(2*j for j in range(100000))):
    #pass
#for j in difference((2*j+1 for j in range(10)),(2*j for j in range(10))):
    #print(j)
#exit()
