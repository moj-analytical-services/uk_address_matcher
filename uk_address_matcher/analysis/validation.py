from __future__ import annotations

import inspect
from functools import wraps
from typing import TYPE_CHECKING, Callable, ParamSpec, TypeVar

if TYPE_CHECKING:
    import duckdb

P = ParamSpec("P")
R = TypeVar("R")

_UKAM_LABEL_ERROR_SUFFIX = (
    "requires a 'ukam_label' column in the match results. "
    "Add a ground-truth label column to the input addresses_to_match data."
)


def ensure_ukam_label_column(
    relation: duckdb.DuckDBPyRelation,
    *,
    function_name: str,
) -> None:
    """Raise a consistent error when ukam_label is unavailable."""
    if "ukam_label" not in relation.columns:
        raise ValueError(f"{function_name} {_UKAM_LABEL_ERROR_SUFFIX}")


def requires_ukam_label(
    relation_arg_name: str,
    *,
    function_name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator enforcing ukam_label availability for a relation argument."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        signature = inspect.signature(func)
        name_for_error = function_name or func.__name__

        @wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            bound = signature.bind_partial(*args, **kwargs)
            relation = bound.arguments.get(relation_arg_name)
            if relation is None:
                raise TypeError(
                    f"Missing required relation argument '{relation_arg_name}' "
                    f"for {func.__name__}."
                )
            ensure_ukam_label_column(relation, function_name=name_for_error)
            return func(*args, **kwargs)

        return wrapped

    return decorator
