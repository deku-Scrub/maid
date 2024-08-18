import mmap

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


def _find_midpoint(x: mmap.mmap, a: int, b: int) -> tuple[int, bytes]:
    if (b - a) < 1:
        return -1, b''
    #if (b - a) == 1:
        #return -1, x[a : (a + 1)]
    match (
            m := (a + b)//2,
            ma := max(x.rfind(b'\n', a, m) + 1, a),
            mb := x.find(b'\n', ma, b),
            ):
        case (_, -1, -1): # Midpoint is on the only string.
            return a, x[a : b]
        case (_, ma, -1): # Midpoint is on the last string.
            return ma, x[ma : b]
        case (_, -1, mb): # Midpoint is on the first string.
            return a, x[a : mb]
        case (_, ma, mb): # Midpoint is on an intermediate string.
            return ma, x[ma : mb]
    raise RuntimeError('Unreachable code has been reached!')


def is_in(e: bytes, x: mmap.mmap) -> bool:
    a = 0
    b = len(x)
    m, s = _find_midpoint(x, a, b)
    while True:
        if a >= b:
            return e == s
        if m < 0:
            return e == s
        elif e < s:
            b = m
            m, s = _find_midpoint(x, a, b)
        elif e > s:
            a = m + len(s) + 1
            m, s = _find_midpoint(x, a, b) # `+ 1` to go past '\n'.
        else:
            return True


#for j in difference((2*j+1 for j in range(1000000)),(2*j for j in range(100000))):
    #pass
#for j in difference((2*j+1 for j in range(10)),(2*j for j in range(10))):
    #print(j)
#exit()

#import pathlib
#import mmap
#with open('/tmp/a.txt', mode='wt') as fos:
#    fos.writelines(sorted(f'{j}\n' for j in range(1000)))
#with open('/tmp/a.txt') as fis:
#    m = mmap.mmap(fis.fileno(), 0, access=mmap.ACCESS_READ)
#    print(is_in(b'909', m))
#m = b'maid/decorators.py\nmaid/tasks.py\n'
#print(is_in(b'maid/tasks.py', m))
