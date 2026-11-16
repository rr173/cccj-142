"""Threaded dataflow runtime with bounded buffers and feedback spill.

Execution model
---------------
One worker thread per map/filter/union/sink node; sources are driven by the
caller's threads.  Every edge has a bounded in-memory buffer (capacity is
configurable down to 1).

* Plain edges apply backpressure: a sender blocks while the buffer is full
  (polls with a cancel check).  Because the graph minus feedback edges is a
  DAG, blocking sends cannot deadlock — the wait-for graph is acyclic.
* Feedback edges never block the sender: when the in-memory buffer is full
  the message is spilled to the SQLite mailbox.  This is what keeps
  feedback cycles deadlock-free even when every plain-edge buffer is full.

Who holds not-yet-sent messages
-------------------------------
* A node worker holds its current input and its computed-but-unsent outputs
  (at most 1 + max_outputs messages per worker).
* A source caller thread holds the batch item it is currently sending.
* A feedback-destined message is created (round already incremented) before
  the send happens; while held it is tracked as ``feedback_pending/held``,
  then ``buffered`` (memory) or ``spilled`` (SQLite mailbox).

All of these participate in progress (they are outstanding obligations) and
in the memory bound: total in-memory messages are bounded by
    sum(edge capacities) + sum(workers) * (1 + max_outputs) + #sinks
independently of how many messages flow through the run overall.  Any
excess lives only in the SQLite spill mailbox on disk.

Failure semantics
-----------------
map raising, a sink rejecting, a limit being exceeded, or caller
cancellation stops the whole graph: the stop event is set, every blocked
sender/worker/waiter is released, and no further completion is notified.
Data already consumed by sinks is not rolled back; the failure state and
the actual counters remain visible via stats().
"""

import itertools
import queue
import threading

from . import graph as gmod
from . import progress as T
from .db import Database
from .errors import (
    DuplicateMessageError,
    GraphError,
    IterflowError,
    LimitExceededError,
    RunCancelledError,
    RunFailedError,
    RunStoppedError,
)
from .graph import FILTER, MAP, SINK, SOURCE, UNION, Msg, mint_instances
from .progress import CompletionHandle, ProgressTracker

POLL = 0.02  # blocking-slice used everywhere so stop_event is always heeded

RUNNING = "RUNNING"
FINISHED = "FINISHED"
FAILED = "FAILED"
CANCELLED = "CANCELLED"
IDLE = "IDLE"


class EdgeStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.sent = 0
        self.received = 0
        self.spilled = 0
        self.spill_read = 0
        self.current_buffered = 0
        self.current_spilled = 0
        self.peak_buffered = 0
        self.peak_total = 0

    def _peaks(self):
        self.peak_buffered = max(self.peak_buffered, self.current_buffered)
        self.peak_total = max(
            self.peak_total, self.current_buffered + self.current_spilled
        )

    def snapshot(self):
        with self.lock:
            return {
                "sent": self.sent,
                "received": self.received,
                "spilled": self.spilled,
                "spill_read": self.spill_read,
                "current_buffered": self.current_buffered,
                "current_spilled": self.current_spilled,
                "peak_buffered": self.peak_buffered,
                "peak_total": self.peak_total,
            }


class BaseEdge:
    def __init__(self, spec, runtime):
        self.id = spec.id
        self.src = spec.src
        self.dst = spec.dst
        self.feedback = spec.feedback
        self.capacity = (
            spec.capacity if spec.capacity is not None else runtime.default_capacity
        )
        self.q = queue.Queue(maxsize=self.capacity)
        self.stats = EdgeStats()
        self.runtime = runtime

    @property
    def tracker(self):
        return self.runtime.tracker


class NormalEdge(BaseEdge):
    """Bounded buffer; the sender blocks (with cancel checks) when full."""

    def send(self, msg):
        rt = self.runtime
        while True:
            if rt.stop_event.is_set():
                raise RunStoppedError("send aborted: run stopped")
            try:
                self.q.put(msg, timeout=POLL)
                break
            except queue.Full:
                continue
        with self.stats.lock:
            self.stats.sent += 1
            self.stats.current_buffered += 1
            self.stats._peaks()
        self.tracker.move(
            msg.epoch, msg.round, (T.TRANSFORMING, self.src), (T.QUEUED, self.id)
        )

    def try_get(self, timeout):
        try:
            msg = self.q.get(timeout=timeout)
        except queue.Empty:
            return None
        with self.stats.lock:
            self.stats.received += 1
            self.stats.current_buffered -= 1
        self.tracker.move(
            msg.epoch, msg.round, (T.QUEUED, self.id), (T.TRANSFORMING, self.dst)
        )
        return msg


class FeedbackEdge(BaseEdge):
    """Bounded memory buffer + unbounded SQLite spill; the sender never blocks.

    ``gate`` is a test/diagnostic hook: when set to a threading.Event, a
    sender waits (cancellably) for it before handing the message off.  The
    message already exists as a ``feedback_pending/held`` obligation while
    it waits, so progress correctly refuses to complete the round.
    """

    def __init__(self, spec, runtime):
        super().__init__(spec, runtime)
        self._spill_seq = 0
        self.gate = None
        self.gate_arrived = threading.Event()

    def send(self, msg):
        rt = self.runtime
        if self.gate is not None:
            self.gate_arrived.set()
            while not self.gate.is_set():
                if rt.stop_event.is_set():
                    raise RunStoppedError("send aborted: run stopped")
                self.gate.wait(POLL)
        try:
            self.q.put_nowait(msg)
        except queue.Full:
            seq = self._spill_seq
            self._spill_seq += 1
            rt.db.spill_put(self.id, seq, msg)
            with self.stats.lock:
                self.stats.sent += 1
                self.stats.spilled += 1
                self.stats.current_spilled += 1
                self.stats._peaks()
            self.tracker.move(
                msg.epoch, msg.round, (T.FB_HELD, self.id), (T.FB_SPILLED, self.id)
            )
        else:
            with self.stats.lock:
                self.stats.sent += 1
                self.stats.current_buffered += 1
                self.stats._peaks()
            self.tracker.move(
                msg.epoch, msg.round, (T.FB_HELD, self.id),
                (T.FB_BUFFERED, self.id),
            )

    def try_get(self, timeout):
        try:
            msg = self.q.get(timeout=timeout)
        except queue.Empty:
            msg = self.runtime.db.spill_take(self.id)
            if msg is None:
                return None
            with self.stats.lock:
                self.stats.received += 1
                self.stats.spill_read += 1
                self.stats.current_spilled -= 1
            self.tracker.move(
                msg.epoch, msg.round, (T.FB_SPILLED, self.id),
                (T.TRANSFORMING, self.dst),
            )
            return msg
        with self.stats.lock:
            self.stats.received += 1
            self.stats.current_buffered -= 1
        self.tracker.move(
            msg.epoch, msg.round, (T.FB_BUFFERED, self.id),
            (T.TRANSFORMING, self.dst),
        )
        return msg


class NodeView:
    """Runtime view of a graph node: resolved runtime edges + counters."""

    def __init__(self, node, in_edges, out_edges):
        self.id = node.id
        self.kind = node.kind
        self.fn = node.fn
        self.max_outputs = node.max_outputs
        self.in_edges = in_edges
        self.out_edges = out_edges
        self.processed = 0
        self.emitted = 0
        self.dropped = 0


class SinkRecord:
    """One record handed to a sink consumer; ack() or reject() exactly once."""

    def __init__(self, msg):
        self.id = msg.id
        self.epoch = msg.epoch
        self.round = msg.round
        self.value = msg.value
        self.parent = msg.parent
        self._done = threading.Event()
        self._rejected = False
        self._reason = ""

    def ack(self):
        if self._done.is_set():
            raise IterflowError(f"sink record {self.id} already acked/rejected")
        self._done.set()

    def reject(self, reason=""):
        if self._done.is_set():
            raise IterflowError(f"sink record {self.id} already acked/rejected")
        self._rejected = True
        self._reason = reason
        self._done.set()

    def __repr__(self):
        return f"SinkRecord({self.id!r}, e{self.epoch}, r{self.round}, {self.value!r})"


class CollectingConsumer:
    """Default sink consumer: appends to a list and acks immediately.

    Convenience for tests/CLI; the list belongs to the consumer, not the
    runtime (the runtime itself never retains delivered records).
    """

    def __init__(self):
        self.records = []
        self.lock = threading.Lock()

    def __call__(self, rec):
        with self.lock:
            self.records.append(
                {"id": rec.id, "epoch": rec.epoch, "round": rec.round,
                 "value": rec.value}
            )
        rec.ack()


class ManualConsumer:
    """Test consumer: hands records to the test, which acks/rejects them."""

    def __init__(self):
        self._q = queue.Queue()

    def __call__(self, rec):
        self._q.put(rec)

    def next(self, timeout=5.0):
        return self._q.get(timeout=timeout)


class RunResult:
    def __init__(self, state, reason_kind, reason, derived, stats):
        self.state = state
        self.reason_kind = reason_kind
        self.reason = reason
        self.derived = derived
        self.stats = stats

    def __repr__(self):
        return (f"RunResult({self.state}, reason={self.reason!r}, "
                f"derived={self.derived})")


class Runtime:
    def __init__(self, graph, db_path=None, max_round=None, max_derived=None,
                 default_capacity=8):
        graph.validate()
        self.graph = graph
        self.max_round = max_round
        self.max_derived = max_derived
        self.default_capacity = default_capacity
        self.db = Database(db_path)
        self.stop_event = threading.Event()
        self._terminal_event = threading.Event()
        self._state_lock = threading.Lock()
        self._state = IDLE
        self._reason_kind = None
        self._reason = None
        self.tracker = ProgressTracker(
            source_ids=[n.id for n in graph.source_nodes],
            max_round=max_round,
            max_derived=max_derived,
            emit=self._on_progress_events,
        )
        # runtime edges
        self.edges = {}
        for spec in graph.edges:
            edge = FeedbackEdge(spec, self) if spec.feedback else NormalEdge(spec, self)
            self.edges[spec.id] = edge
        # runtime node views
        self.node_views = {}
        for node in graph.nodes.values():
            self.node_views[node.id] = NodeView(
                node,
                [self.edges[e.id] for e in node.in_edges],
                [self.edges[e.id] for e in node.out_edges],
            )
        self._consumers = {}
        self._collectors = {}
        for node in graph.sink_nodes:
            collector = CollectingConsumer()
            self._collectors[node.id] = collector
            self._consumers[node.id] = collector
        self._sources = {n.id: SourceHandle(self, n.id) for n in graph.source_nodes}
        self._workers = []

    # ------------------------------------------------------------ lifecycle
    def declare_epoch(self, epoch):
        self.tracker.declare_epoch(epoch)

    def attach_consumer(self, sink_id, consumer):
        with self._state_lock:
            if self._state != IDLE:
                raise IterflowError("consumers must be attached before start")
        if sink_id not in self._consumers:
            raise GraphError(f"{sink_id!r} is not a sink node")
        self._consumers[sink_id] = consumer

    def start(self):
        with self._state_lock:
            if self._state != IDLE:
                raise IterflowError("runtime already started")
            self._state = RUNNING
        self.tracker.start()
        for view in self.node_views.values():
            if view.kind in (MAP, FILTER, UNION, SINK):
                t = threading.Thread(
                    target=self._worker, args=(view,),
                    name=f"iterflow-{view.id}", daemon=True,
                )
                self._workers.append(t)
                t.start()
        return self

    def wait(self, timeout=None):
        """Wait for a terminal state; joins workers; returns a RunResult."""
        if not self._terminal_event.wait(timeout):
            raise TimeoutError("run did not reach a terminal state in time")
        for t in self._workers:
            t.join(timeout=5)
        return RunResult(
            self._state, self._reason_kind, self._reason,
            self.tracker.derived, self.stats(),
        )

    def cancel(self):
        with self._state_lock:
            if self._state != RUNNING:
                return
            self._state = CANCELLED
        self.db.log("run_cancelled")
        self.stop_event.set()
        self.tracker.set_terminal(RunCancelledError("run cancelled by caller"))
        self._terminal_event.set()

    def fail(self, kind, detail):
        with self._state_lock:
            if self._state != RUNNING:
                return
            self._state = FAILED
            self._reason_kind = kind
            self._reason = detail
        self.db.log("run_failed", detail=f"{kind}: {detail}")
        self.stop_event.set()
        self.tracker.set_terminal(RunFailedError(kind, detail))
        self._terminal_event.set()

    def shutdown(self):
        with self._state_lock:
            state = self._state
        if state == RUNNING:
            self.cancel()
        if state != IDLE:
            self.wait(timeout=10)
        self.db.close()

    # -------------------------------------------------------------- context
    def _require_running(self):
        with self._state_lock:
            state = self._state
        if state != RUNNING:
            raise RunStoppedError(f"run is not running (state={state})")

    def _finish(self):
        with self._state_lock:
            if self._state != RUNNING:
                return
            self._state = FINISHED
        self.db.log("run_finished")
        self.stop_event.set()
        self._terminal_event.set()

    def _on_progress_events(self, events):
        for ev in events:
            if ev["kind"] == "all_complete":
                self._finish()  # internal signal; run_finished is logged there
            else:
                self.db.log(ev["kind"], epoch=ev["epoch"], round=ev["round"])

    # --------------------------------------------------------------- workers
    def _worker(self, view):
        try:
            while not self.stop_event.is_set():
                msg = None
                for edge in view.in_edges:
                    msg = edge.try_get(POLL)
                    if msg is not None:
                        break
                if msg is None:
                    continue
                self._process(view, msg)
        except RunStoppedError:
            pass
        except LimitExceededError as exc:
            self.fail(exc.kind, exc.detail)
        except Exception as exc:  # defensive: a bug must fail the run loudly
            self.fail("internal", f"worker {view.id}: {exc!r}")

    def _process(self, view, msg):
        if view.kind == SINK:
            self._deliver(view, msg)
            return
        try:
            if view.kind == MAP:
                values = list(itertools.islice(view.fn(msg.value),
                                               view.max_outputs + 1))
                if len(values) > view.max_outputs:
                    self.fail(
                        "output_cap",
                        f"map {view.id!r} emitted {len(values)} outputs, "
                        f"cap is {view.max_outputs}",
                    )
                    raise RunStoppedError()
            elif view.kind == FILTER:
                values = [msg.value] if view.fn(msg.value) else []
            else:  # UNION
                values = [msg.value]
        except RunStoppedError:
            raise
        except Exception as exc:
            self.fail("transform_error", f"node {view.id!r}: {exc!r}")
            raise RunStoppedError()

        insts = mint_instances(view.id, view.kind, view.out_edges, msg, values)
        outputs = [
            (
                inst.round,
                T.FB_HELD if edge.feedback else T.TRANSFORMING,
                edge.id if edge.feedback else view.id,
            )
            for (edge, inst) in insts
        ]
        # Atomic under the tracker lock: retire input, create all outputs.
        self.tracker.transform(
            msg.epoch, msg.round, (T.TRANSFORMING, view.id), outputs
        )
        view.processed += 1
        view.emitted += len(insts)
        if not values:
            view.dropped += 1
        for (edge, inst) in insts:
            edge.send(inst)

    def _deliver(self, view, msg):
        self.tracker.move(
            msg.epoch, msg.round, (T.TRANSFORMING, view.id),
            (T.SINK_UNACKED, view.id),
        )
        rec = SinkRecord(msg)
        try:
            self._consumers[view.id](rec)
        except Exception as exc:
            self.fail("sink_error", f"sink {view.id!r} consumer raised {exc!r}")
            raise RunStoppedError()
        while not rec._done.wait(POLL):
            if self.stop_event.is_set():
                return  # leave the unacked obligation visible in the counts
        if rec._rejected:
            self.fail(
                "sink_rejected",
                f"sink {view.id!r} rejected {msg.id!r}: {rec._reason}",
            )
            raise RunStoppedError()
        self.tracker.retire(msg.epoch, msg.round, T.SINK_UNACKED, view.id)
        view.processed += 1

    # -------------------------------------------------------------- public
    def source(self, source_id):
        handle = self._sources.get(source_id)
        if handle is None:
            raise GraphError(f"{source_id!r} is not a source node")
        return handle

    def feedback_edge(self, edge_id):
        edge = self.edges.get(edge_id)
        if edge is None or not edge.feedback:
            raise GraphError(f"{edge_id!r} is not a feedback edge")
        return edge

    def subscribe_round(self, epoch, rnd):
        return self.tracker.subscribe_round(epoch, rnd)

    def subscribe_epoch(self, epoch):
        return self.tracker.subscribe_epoch(epoch)

    def wait_round(self, epoch, rnd, timeout=None):
        return self.subscribe_round(epoch, rnd).wait(timeout)

    def wait_epoch(self, epoch, timeout=None):
        return self.subscribe_epoch(epoch).wait(timeout)

    def frontier(self, epoch):
        return self.tracker.frontier(epoch)

    def collected(self, sink_id):
        return self._collectors[sink_id].records

    def progress_events(self):
        return self.db.events()

    def stats(self):
        return {
            "state": self._state,
            "reason_kind": self._reason_kind,
            "reason": self._reason,
            "derived": self.tracker.derived,
            "edges": {
                eid: dict(
                    {"capacity": e.capacity, "feedback": e.feedback},
                    **e.stats.snapshot(),
                )
                for eid, e in self.edges.items()
            },
            "nodes": {
                nid: {
                    "processed": v.processed,
                    "emitted": v.emitted,
                    "dropped": v.dropped,
                }
                for nid, v in self.node_views.items()
                if v.kind != SOURCE
            },
            "epochs": self.tracker.epoch_snapshot(),
        }


class SourceHandle:
    """Caller-facing handle for feeding one source node."""

    def __init__(self, runtime, source_id):
        self._rt = runtime
        self.id = source_id
        self._seq = 0
        self._lock = threading.Lock()

    def send(self, epoch, value, msg_id=None):
        """Send one record into the graph at (epoch, round=0).

        Blocks (with cancel checks) while the downstream buffer is full —
        this is how a slow sink applies controlled backpressure to callers.
        Raises deterministically: undeclared epoch, then send-after-close,
        then run-not-running.
        """
        rt = self._rt
        if msg_id is None:
            with self._lock:
                msg_id = f"{self.id}#{epoch}#{self._seq}"
                self._seq += 1
        elif not rt.db.seen_insert(self.id, msg_id):
            raise DuplicateMessageError(
                f"duplicate message id {msg_id!r} on source {self.id!r}"
            )
        out_edges = rt.node_views[self.id].out_edges
        fanout = len(out_edges)
        insts = [
            (edge, Msg(msg_id if fanout == 1 else f"{msg_id}~{edge.id}",
                       epoch, 0, value, parent=None))
            for edge in out_edges
        ]
        rt.tracker.source_send_begin(epoch, self.id, len(insts))
        for edge, inst in insts:
            edge.send(inst)
        return msg_id

    def close_epoch(self, epoch):
        """Declare that this source will send no more records for ``epoch``."""
        self._rt.tracker.source_close(epoch, self.id)

    def close_all(self):
        """Close every declared epoch on this source (idempotent)."""
        from .errors import SourceStateError

        for epoch in self._rt.tracker.declared_epochs():
            try:
                self.close_epoch(epoch)
            except SourceStateError:
                pass  # already closed by an earlier close_all / close_epoch
