"""Graph model: nodes, edges, validation, message identity minting.

Node kinds: source / map / filter / union / sink.
Edge kinds: plain (preserves (epoch, round)) and feedback (round + 1, epoch
unchanged).  Every cycle must pass through at least one feedback edge; the
graph with feedback edges removed must be a DAG.  This is validated up front.

A node's output is *broadcast* to all of its outgoing edges: each outgoing
edge carries the full output stream of the node.  Routing decisions are
expressed with filter nodes, fan-in (join) is expressed by giving a node
several incoming edges (an explicit union node is the pass-through form of
that).  Bag multiplicity is preserved everywhere; nothing dedups by value.
"""

from . import builtins
from .errors import GraphError

SOURCE = "source"
MAP = "map"
FILTER = "filter"
UNION = "union"
SINK = "sink"

NODE_KINDS = (SOURCE, MAP, FILTER, UNION, SINK)


class Msg:
    """One message instance travelling the graph.

    ``id`` is unique per instance and encodes its derivation path (path
    identity); ``parent`` is the id of the message it was derived from.
    """

    __slots__ = ("id", "epoch", "round", "value", "parent")

    def __init__(self, id, epoch, round, value, parent):
        self.id = id
        self.epoch = epoch
        self.round = round
        self.value = value
        self.parent = parent

    def __repr__(self):
        return f"Msg({self.id!r}, e{self.epoch}, r{self.round}, {self.value!r})"


class Node:
    def __init__(self, id, kind, fn=None, max_outputs=None):
        self.id = id
        self.kind = kind
        self.fn = fn
        self.max_outputs = max_outputs
        self.in_edges = []
        self.out_edges = []

    def __repr__(self):
        return f"Node({self.id}:{self.kind})"


class Edge:
    def __init__(self, id, src, dst, capacity=None, feedback=False):
        self.id = id
        self.src = src
        self.dst = dst
        self.capacity = capacity
        self.feedback = feedback

    def __repr__(self):
        fb = " (feedback)" if self.feedback else ""
        return f"Edge({self.id}{fb})"


def mint_instances(node_id, node_kind, out_edges, msg, values):
    """Mint the output message instances for one processed input.

    Shared by the runtime and the independent simulator so that message
    identities agree.  map outputs get fresh ids derived from the input id;
    filter/union forward the input id.  With more than one outgoing edge the
    edge id is appended so every broadcast copy has a unique identity.
    Feedback edges increment the round; plain edges preserve it.
    """
    if node_kind == MAP:
        bases = [f"{msg.id}>{node_id}[{i}]" for i in range(len(values))]
    else:  # filter / union forward the message identity
        bases = [msg.id] * len(values)
    fanout = len(out_edges)
    insts = []
    for edge in out_edges:
        for base, val in zip(bases, values):
            iid = base if fanout == 1 else f"{base}~{edge.id}"
            rnd = msg.round + 1 if edge.feedback else msg.round
            insts.append((edge, Msg(iid, msg.epoch, rnd, val, parent=msg.id)))
    return insts


class Graph:
    def __init__(self, name="graph"):
        self.name = name
        self.nodes = {}
        self.edges = []

    # ------------------------------------------------------------------ nodes
    def add_source(self, node_id):
        return self._add(Node(node_id, SOURCE))

    def add_map(self, node_id, fn=None, spec=None, max_outputs=None):
        """A synchronous 1-in / 0-or-more-out pure transform.

        Either a Python callback ``fn`` (pure; not available from graph files)
        or a declared builtin ``spec`` (see iterflow.builtins).  ``max_outputs``
        bounds the number of outputs per call; it is enforced at run time.
        """
        if fn is None:
            if spec is None:
                raise GraphError(f"map {node_id!r} needs fn= or spec=")
            fn, builtin_cap = builtins.build_map_fn(spec)
            if max_outputs is None:
                max_outputs = builtin_cap
        if max_outputs is None:
            raise GraphError(f"map {node_id!r} needs max_outputs with a custom fn")
        if max_outputs < 1:
            raise GraphError(f"map {node_id!r}: max_outputs must be >= 1")
        return self._add(Node(node_id, MAP, fn=fn, max_outputs=max_outputs))

    def add_filter(self, node_id, pred=None, spec=None):
        """Decides only whether the message continues; never transforms it."""
        if pred is None:
            if spec is None:
                raise GraphError(f"filter {node_id!r} needs pred= or spec=")
            pred = builtins.build_filter_fn(spec)
        return self._add(Node(node_id, FILTER, fn=pred, max_outputs=1))

    def add_union(self, node_id):
        """Pass-through join preserving bag multiplicity (no dedup)."""
        return self._add(Node(node_id, UNION, max_outputs=1))

    def add_sink(self, node_id):
        return self._add(Node(node_id, SINK))

    def _add(self, node):
        if node.id in self.nodes:
            raise GraphError(f"duplicate node id {node.id!r}")
        self.nodes[node.id] = node
        return node

    # ------------------------------------------------------------------ edges
    def add_edge(self, src, dst, capacity=None, edge_id=None):
        return self._add_edge(src, dst, capacity, edge_id, feedback=False)

    def add_feedback_edge(self, src, dst, capacity=None, edge_id=None):
        """The only edge kind that may close a cycle; increments round by 1."""
        return self._add_edge(src, dst, capacity, edge_id, feedback=True)

    def _add_edge(self, src, dst, capacity, edge_id, feedback):
        if src not in self.nodes:
            raise GraphError(f"edge source {src!r} is not a node")
        if dst not in self.nodes:
            raise GraphError(f"edge target {dst!r} is not a node")
        eid = edge_id or f"{src}->{dst}"
        if any(e.id == eid for e in self.edges):
            raise GraphError(f"duplicate edge id {eid!r}")
        if capacity is not None and capacity < 1:
            raise GraphError(f"edge {eid!r}: capacity must be >= 1")
        edge = Edge(eid, src, dst, capacity=capacity, feedback=feedback)
        self.edges.append(edge)
        self.nodes[src].out_edges.append(edge)
        self.nodes[dst].in_edges.append(edge)
        return edge

    def node(self, node_id):
        return self.nodes[node_id]

    @property
    def source_nodes(self):
        return [n for n in self.nodes.values() if n.kind == SOURCE]

    @property
    def sink_nodes(self):
        return [n for n in self.nodes.values() if n.kind == SINK]

    @property
    def worker_nodes(self):
        return [n for n in self.nodes.values() if n.kind in (MAP, FILTER, UNION, SINK)]

    # -------------------------------------------------------------- validation
    def validate(self):
        for node in self.nodes.values():
            if node.kind == SOURCE and node.in_edges:
                raise GraphError(f"source {node.id!r} must not have incoming edges")
            if node.kind == SINK and node.out_edges:
                raise GraphError(f"sink {node.id!r} must not have outgoing edges")
            if node.kind == SOURCE and not node.out_edges:
                raise GraphError(f"source {node.id!r} needs at least one outgoing edge")
            if node.kind == SINK and not node.in_edges:
                raise GraphError(f"sink {node.id!r} needs at least one incoming edge")
        # Removing feedback edges must leave a DAG: every cycle is feedback-cut.
        self.topological_order()
        return self

    def topological_order(self):
        """Kahn's algorithm over the DAG part (feedback edges removed)."""
        indeg = {nid: 0 for nid in self.nodes}
        for e in self.edges:
            if not e.feedback:
                indeg[e.dst] += 1
        ready = [nid for nid, d in indeg.items() if d == 0]
        order = []
        while ready:
            nid = ready.pop()
            order.append(nid)
            for e in self.nodes[nid].out_edges:
                if e.feedback:
                    continue
                indeg[e.dst] -= 1
                if indeg[e.dst] == 0:
                    ready.append(e.dst)
        if len(order) != len(self.nodes):
            raise GraphError(
                "cycle detected that does not pass through a feedback edge; "
                "every cycle must contain a feedback edge"
            )
        return order
