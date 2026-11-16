"""Builtin declared mappings for map/filter nodes.

These are the *only* transforms a graph file may reference: a closed set of
declarative specs.  No arbitrary code is ever executed from a file.  The
Python API may additionally register pure callbacks directly on the graph.
"""

from .errors import GraphError


def build_map_fn(spec):
    """Return (fn, max_outputs) for a declared map spec."""
    if not isinstance(spec, dict) or "kind" not in spec:
        raise GraphError(f"bad map spec {spec!r}: expected dict with 'kind'")
    kind = spec["kind"]

    if kind == "identity":
        return (lambda v: [v]), 1

    if kind == "duplicate":
        count = spec.get("count")
        if not isinstance(count, int) or count < 1:
            raise GraphError("duplicate map needs integer 'count' >= 1")
        return (lambda v: [v] * count), count

    if kind == "dict_lookup":
        table = spec.get("dict")
        if not isinstance(table, dict):
            raise GraphError("dict_lookup map needs a 'dict' table")
        default = spec.get("default", "self")
        if default not in ("self", "drop"):
            raise GraphError("dict_lookup 'default' must be 'self' or 'drop'")
        table = dict(table)

        def lookup(v):
            if v in table:
                return [table[v]]
            return [v] if default == "self" else []

        return lookup, 1

    raise GraphError(f"unknown map spec kind {kind!r}")


def build_filter_fn(spec):
    """Return a predicate for a declared filter spec."""
    if not isinstance(spec, dict) or "kind" not in spec:
        raise GraphError(f"bad filter spec {spec!r}: expected dict with 'kind'")
    kind = spec["kind"]

    if kind == "all":
        return lambda v: True
    if kind == "none":
        return lambda v: False
    if kind == "in":
        values = frozenset(spec.get("values", ()))
        return lambda v: v in values
    if kind == "not_in":
        values = frozenset(spec.get("values", ()))
        return lambda v: v not in values
    if kind == "equals":
        target = spec.get("value")
        return lambda v: v == target

    raise GraphError(f"unknown filter spec kind {kind!r}")
