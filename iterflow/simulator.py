"""Independent single-threaded explicit-queue simulator.

This is an oracle for tests: it executes a graph with plain FIFO deques in
one thread and derives expected sink output, round-completion order, derived
message counts, and per-edge send counts.  It deliberately does NOT import
or reuse the runtime's progress tracking (iterflow.progress); completion is
recomputed here from explicit per-(epoch, round) presence counts over the
queues.  Only the graph structure and the pure id-minting rule are shared
with the runtime, so that message identities are comparable.

Sources are modelled as: all sends are loaded into the source edge queues at
t0 and every source is closed for every epoch at t0.  Sinks ack immediately.
"""

from collections import defaultdict, deque

from .graph import FILTER, MAP, SINK, SOURCE, UNION, Msg, mint_instances


class SimResult:
    def __init__(self, sink_records, round_completions, epoch_completions,
                 derived, edge_sent, failure):
        self.sink_records = sink_records          # list[Msg] in delivery order
        self.round_completions = round_completions  # list[(epoch, round)]
        self.epoch_completions = epoch_completions  # list[epoch]
        self.derived = derived
        self.edge_sent = edge_sent                # edge_id -> count
        self.failure = failure                    # (kind, detail) or None

    def sink_bag(self):
        return sorted((m.id, m.epoch, m.round, m.value) for m in self.sink_records)


def simulate(graph, sends, max_round=None, max_derived=None, max_steps=10_000_000):
    """Run the graph to quiescence (or a limit failure) and return SimResult.

    ``sends`` is a list of (source_id, epoch, value, msg_id).
    """
    graph.validate()
    queues = {e.id: deque() for e in graph.edges}
    edge_sent = {e.id: 0 for e in graph.edges}
    counts = defaultdict(int)          # (epoch, round) -> messages present
    ever_rounds = defaultdict(set)
    completed_rounds = []
    completed_round_set = set()
    completed_epochs = []
    completed_epoch_set = set()
    sink_records = []
    derived = 0
    failure = None

    epochs = sorted({ep for (_, ep, _, _) in sends})
    out_map = {nid: [e for e in graph.edges if e.src == nid] for nid in graph.nodes}
    in_map = {nid: [e for e in graph.edges if e.dst == nid] for nid in graph.nodes}

    def place(edge, msg):
        queues[edge.id].append(msg)
        edge_sent[edge.id] += 1
        counts[(msg.epoch, msg.round)] += 1
        ever_rounds[msg.epoch].add(msg.round)

    # t0: load all source sends; every source is closed for every epoch.
    for (sid, ep, val, mid) in sends:
        outs = out_map[sid]
        fanout = len(outs)
        for edge in outs:
            iid = mid if fanout == 1 else f"{mid}~{edge.id}"
            place(edge, Msg(iid, ep, 0, val, parent=None))

    def check_completions():
        # A round completes when no message of that epoch at any round <= r
        # is present anywhere (sources are all closed since t0).
        for ep in epochs:
            nonzero = [r for (e, r), n in counts.items() if e == ep and n > 0]
            mn = min(nonzero) if nonzero else None
            for r in sorted(ever_rounds[ep]):
                if (ep, r) in completed_round_set:
                    continue
                if mn is None or r < mn:
                    completed_round_set.add((ep, r))
                    completed_rounds.append((ep, r))
            if mn is None and ep not in completed_epoch_set:
                completed_epoch_set.add(ep)
                completed_epochs.append(ep)

    order = graph.topological_order()
    steps = 0
    while failure is None and any(queues[e.id] for e in graph.edges):
        progressed = False
        for nid in order:
            node = graph.nodes[nid]
            if node.kind == SOURCE:
                continue
            msg = None
            for edge in in_map[nid]:
                if queues[edge.id]:
                    msg = queues[edge.id].popleft()
                    counts[(msg.epoch, msg.round)] -= 1
                    break
            if msg is None:
                continue
            progressed = True
            if node.kind == SINK:
                sink_records.append(msg)  # delivered and acked immediately
            else:
                if node.kind == MAP:
                    values = list(node.fn(msg.value))
                    if len(values) > node.max_outputs:
                        failure = ("output_cap",
                                   f"map {nid!r} emitted {len(values)} > "
                                   f"cap {node.max_outputs}")
                        break
                elif node.kind == FILTER:
                    values = [msg.value] if node.fn(msg.value) else []
                else:  # UNION
                    values = [msg.value]
                insts = mint_instances(nid, node.kind, out_map[nid], msg, values)
                if max_round is not None:
                    over = [i for _, i in insts if i.round > max_round]
                    if over:
                        failure = ("round_limit",
                                   f"message would reach round "
                                   f"{over[0].round} > {max_round}")
                        break
                if max_derived is not None and derived + len(insts) > max_derived:
                    failure = ("derived_limit",
                               f"derived count would exceed {max_derived}")
                    break
                derived += len(insts)
                for edge, inst in insts:
                    place(edge, inst)
            check_completions()
        steps += 1
        if not progressed or steps > max_steps:
            break
    check_completions()
    return SimResult(sink_records, completed_rounds, completed_epochs,
                     derived, edge_sent, failure)
