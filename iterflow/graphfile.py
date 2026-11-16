"""Graph-file entry point: build a Graph from a JSON description.

Map/filter transforms in a graph file may only reference the builtin
declared specs (iterflow.builtins) — no arbitrary code is ever loaded or
executed from a file.  Pure Python callbacks can only be registered through
the Python API (Graph.add_map/add_filter with fn=/pred=).

Schema:
{
  "name": "optional-name",
  "nodes": [
    {"id": "src",  "type": "source"},
    {"id": "m",    "type": "map",    "spec": {"kind": "dict_lookup", ...}},
    {"id": "f",    "type": "filter", "spec": {"kind": "in", "values": [...]}},
    {"id": "u",    "type": "union"},
    {"id": "sink", "type": "sink"}
  ],
  "edges": [
    {"from": "src", "to": "m"},
    {"from": "m",   "to": "u", "feedback": true, "capacity": 1}
  ]
}
"""

import json

from .errors import GraphError
from .graph import Graph


def graph_from_dict(data):
    if not isinstance(data, dict):
        raise GraphError("graph file must contain a JSON object")
    g = Graph(data.get("name", "graph"))
    for node in data.get("nodes", []):
        nid = node.get("id")
        ntype = node.get("type")
        if not nid or not ntype:
            raise GraphError(f"node entry needs 'id' and 'type': {node!r}")
        if ntype == "source":
            g.add_source(nid)
        elif ntype == "map":
            g.add_map(nid, spec=node.get("spec"))
        elif ntype == "filter":
            g.add_filter(nid, spec=node.get("spec"))
        elif ntype == "union":
            g.add_union(nid)
        elif ntype == "sink":
            g.add_sink(nid)
        else:
            raise GraphError(f"unknown node type {ntype!r}")
    for edge in data.get("edges", []):
        src, dst = edge.get("from"), edge.get("to")
        capacity = edge.get("capacity")
        if edge.get("feedback"):
            g.add_feedback_edge(src, dst, capacity=capacity)
        else:
            g.add_edge(src, dst, capacity=capacity)
    g.validate()
    return g


def load_graph(path):
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    return graph_from_dict(data)
