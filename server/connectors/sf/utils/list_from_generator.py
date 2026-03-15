from typing import TypeVar
from collections.abc import Iterable
T = TypeVar('T')
def list_from_generator(
        generator_function: Iterable[Iterable[T]]
) -> list[T]:
    """Utility method for constructing a list from a generator function"""
    ret_val: list[T] = []
    for list_results in generator_function:
        ret_val.extend(list_results)
    return ret_val