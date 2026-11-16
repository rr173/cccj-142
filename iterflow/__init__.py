"""iterflow: a local iterative dataflow runtime with logical rounds.

Public surface:
    Graph                     - declare the processing graph
    Runtime                   - execute it (bounded buffers, SQLite spill)
    simulate                  - independent single-threaded oracle
    load_graph                - graph-file entry (builtin specs only)
    ManualConsumer            - test sink consumer with manual ack
    CollectingConsumer        - default sink consumer (collect + auto-ack)
"""

from .errors import (
    DuplicateMessageError,
    GraphError,
    IterflowError,
    LimitExceededError,
    RunCancelledError,
    RunFailedError,
    RunStoppedError,
    SourceStateError,
    UndeclaredEpochError,
)
from .graph import Graph
from .graphfile import graph_from_dict, load_graph
from .runtime import (
    CollectingConsumer,
    ManualConsumer,
    Runtime,
    SinkRecord,
)
from .simulator import simulate

__all__ = [
    "Graph",
    "Runtime",
    "simulate",
    "load_graph",
    "graph_from_dict",
    "ManualConsumer",
    "CollectingConsumer",
    "SinkRecord",
    "IterflowError",
    "GraphError",
    "SourceStateError",
    "UndeclaredEpochError",
    "DuplicateMessageError",
    "RunStoppedError",
    "RunFailedError",
    "RunCancelledError",
    "LimitExceededError",
]
