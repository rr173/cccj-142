"""Logical-round progress tracking by obligation counting.

Every message instance is an *obligation* from the moment it is created
until it is acked at a sink, dropped by a filter, or transformed into its
downstream outputs.  A transform retires the input and creates all outputs
atomically under one lock, so the outstanding count of an (epoch, round)
never transiently hits zero while work is merely moving between nodes.

Completion rule for (epoch e, round r):
    (e, r) is complete iff every source has closed e AND the outstanding
    count of every round r' <= r of e is zero.

Rounds never decrease along an edge (plain edges preserve the round,
feedback edges increment it), so a message at round r' can only ever beget
messages at rounds >= r'.  Hence once the condition holds it holds forever:
no existing message can still produce a message at round <= r, and closed
sources cannot inject new ones.  Completion is therefore monotone and is
notified exactly once.  It is never inferred from empty queues, idle
workers, sleeps, or sentinel records.

Obligation categories (used for the frontier explanation):
    queued             in a plain edge's bounded in-memory buffer
    transforming       held by a node worker (input in hand, or outputs
                       computed but not yet handed to a plain edge), or by a
                       source sender between send() and enqueue
    feedback_pending   assigned to a feedback edge; sub-states:
                       held     - created (round already incremented) but
                                  the sender has not handed it off yet
                       buffered - in the feedback edge's in-memory buffer
                       spilled  - in the SQLite spill mailbox
    sink_unacked       delivered to the sink consumer, awaiting ack

"source not closed" is not a message but is reported as a frontier reason
because an open source may still inject round-0 messages.
"""

import threading
from collections import defaultdict

from .errors import (
    RunCancelledError,
    RunFailedError,
    RunStoppedError,
    SourceStateError,
    UndeclaredEpochError,
    LimitExceededError,
)

QUEUED = "queued"
TRANSFORMING = "transforming"
FB_HELD = "feedback_held"
FB_BUFFERED = "feedback_buffered"
FB_SPILLED = "feedback_spilled"
SINK_UNACKED = "sink_unacked"

_FEEDBACK_CATS = (FB_HELD, FB_BUFFERED, FB_SPILLED)


class CompletionHandle:
    """A waitable subscription to one logical completion."""

    def __init__(self, desc):
        self.desc = desc
        self._event = threading.Event()
        self._result = None
        self._exc = None

    def _set_result(self, result):
        self._result = result
        self._event.set()

    def _set_exception(self, exc):
        self._exc = exc
        self._event.set()

    def done(self):
        return self._event.is_set()

    def wait(self, timeout=None):
        if not self._event.wait(timeout):
            raise TimeoutError(f"timed out waiting for {self.desc}")
        if self._exc is not None:
            raise self._exc
        return self._result


class Frontier:
    """Snapshot: the minimum boundary of the incomplete logical-time set."""

    def __init__(self, epoch, complete, frontier_round, reasons):
        self.epoch = epoch
        self.complete = complete
        # Smallest incomplete round (None when the epoch is complete).
        self.frontier_round = frontier_round
        # Highest round known complete; None when none is (or when complete).
        self.completed_through = (
            None if (complete or frontier_round == 0) else frontier_round - 1
        )
        self.reasons = reasons

    def categories(self):
        return {r["category"] for r in self.reasons}

    def to_dict(self):
        return {
            "epoch": self.epoch,
            "complete": self.complete,
            "frontier_round": self.frontier_round,
            "completed_through": self.completed_through,
            "reasons": self.reasons,
        }

    def __repr__(self):
        if self.complete:
            return f"Frontier(epoch={self.epoch}, complete)"
        return (
            f"Frontier(epoch={self.epoch}, frontier_round={self.frontier_round}, "
            f"reasons={self.reasons})"
        )


class _EpochState:
    __slots__ = ("open_sources", "closed", "counts", "round_totals", "total",
                 "ever_rounds")

    def __init__(self, source_ids):
        self.open_sources = set(source_ids)
        self.closed = False
        self.counts = defaultdict(int)      # (category, location, round) -> n
        self.round_totals = defaultdict(int)  # round -> n (all categories)
        self.total = 0
        self.ever_rounds = set()


class ProgressTracker:
    def __init__(self, source_ids, max_round=None, max_derived=None, emit=None):
        self._lock = threading.RLock()
        self._source_ids = list(source_ids)
        self._max_round = max_round
        self._max_derived = max_derived
        self._emit = emit or (lambda events: None)
        self._epochs = {}
        self._started = False
        self._terminal_exc = None
        self._round_subs = defaultdict(list)   # epoch -> [(round, handle)]
        self._epoch_subs = defaultdict(list)   # epoch -> [handle]
        self._completed_rounds = set()         # (epoch, round) already notified
        self._completed_epochs = set()
        self.derived = 0

    # ------------------------------------------------------------- lifecycle
    def declare_epoch(self, epoch):
        with self._lock:
            if self._started:
                raise SourceStateError(
                    "epochs must be declared before the run starts"
                )
            if epoch in self._epochs:
                raise SourceStateError(f"epoch {epoch} declared twice")
            self._epochs[epoch] = _EpochState(self._source_ids)

    def start(self):
        with self._lock:
            self._started = True
            events = []
            for epoch, ep in self._epochs.items():
                # with no (remaining) open sources the input is vacuously
                # closed; epochs of a source-less graph complete at start
                if not ep.open_sources:
                    ep.closed = True
                events.extend(self._check_epoch_locked(epoch))
            if not self._epochs:
                # no epochs declared at all: nothing can ever be outstanding
                events.append({"kind": "all_complete", "epoch": None,
                               "round": None})
        self._emit(events)

    def set_terminal(self, exc):
        """Release every pending waiter with ``exc``; fire nothing further."""
        with self._lock:
            self._terminal_exc = exc
            for subs in self._round_subs.values():
                for _, handle in subs:
                    handle._set_exception(exc)
            for subs in self._epoch_subs.values():
                for handle in subs:
                    handle._set_exception(exc)
            self._round_subs.clear()
            self._epoch_subs.clear()

    # --------------------------------------------------------- count helpers
    def _inc(self, ep, category, location, rnd, n=1):
        ep.counts[(category, location, rnd)] += n
        ep.round_totals[rnd] += n
        ep.total += n
        ep.ever_rounds.add(rnd)

    def _dec(self, ep, category, location, rnd, n=1):
        key = (category, location, rnd)
        assert ep.counts[key] >= n, f"obligation underflow at {key}"
        ep.counts[key] -= n
        if ep.counts[key] == 0:
            del ep.counts[key]
        ep.round_totals[rnd] -= n
        if ep.round_totals[rnd] == 0:
            del ep.round_totals[rnd]
        ep.total -= n

    # ------------------------------------------------------------- mutations
    def source_send_begin(self, epoch, source_id, n):
        """Validate and register n new round-0 obligations (held by sender).

        Validation order is deterministic: undeclared epoch, then
        send-after-close, then run-not-running.
        """
        with self._lock:
            ep = self._epochs.get(epoch)
            if ep is None:
                raise UndeclaredEpochError(f"epoch {epoch} was not declared")
            if source_id not in ep.open_sources:
                raise SourceStateError(
                    f"source {source_id!r} already closed epoch {epoch}; "
                    "sends after close are rejected"
                )
            self._check_running()
            for _ in range(n):
                self._inc(ep, TRANSFORMING, source_id, 0)

    def source_close(self, epoch, source_id):
        with self._lock:
            ep = self._epochs.get(epoch)
            if ep is None:
                raise UndeclaredEpochError(f"epoch {epoch} was not declared")
            if source_id not in ep.open_sources:
                raise SourceStateError(
                    f"source {source_id!r} already closed epoch {epoch}; "
                    "duplicate close declaration"
                )
            self._check_running()
            ep.open_sources.discard(source_id)
            if not ep.open_sources:
                ep.closed = True
            events = self._check_epoch_locked(epoch)
        self._emit(events)

    def add(self, epoch, rnd, category, location):
        with self._lock:
            self._check_running()
            ep = self._epoch_or_raise(epoch)
            self._inc(ep, category, location, rnd)

    def move(self, epoch, rnd, frm, to):
        """Recategorize one obligation; round totals are unchanged."""
        with self._lock:
            ep = self._epochs.get(epoch)
            if ep is None or self._terminal_exc is not None:
                return  # run over; counts no longer matter
            self._dec(ep, frm[0], frm[1], rnd)
            self._inc(ep, to[0], to[1], rnd)

    def retire(self, epoch, rnd, category, location):
        with self._lock:
            ep = self._epochs.get(epoch)
            if ep is None:
                return
            self._dec(ep, category, location, rnd)
            events = [] if self._terminal_exc else self._check_epoch_locked(epoch)
        self._emit(events)

    def transform(self, epoch, in_round, in_catloc, outputs):
        """Atomically retire the input obligation and create all outputs.

        ``outputs`` is a list of (round, category, location).  Raises
        LimitExceededError *before* mutating if a run limit would be broken.
        """
        with self._lock:
            self._check_running()
            ep = self._epoch_or_raise(epoch)
            if self._max_round is not None:
                for (rnd, _, _) in outputs:
                    if rnd > self._max_round:
                        raise LimitExceededError(
                            "round_limit",
                            f"message would reach round {rnd} > max_round "
                            f"{self._max_round} in epoch {epoch}",
                        )
            if self._max_derived is not None and (
                self.derived + len(outputs) > self._max_derived
            ):
                raise LimitExceededError(
                    "derived_limit",
                    f"derived message count would exceed max_derived "
                    f"{self._max_derived} in epoch {epoch}",
                )
            self._dec(ep, in_catloc[0], in_catloc[1], in_round)
            for (rnd, category, location) in outputs:
                self._inc(ep, category, location, rnd)
            self.derived += len(outputs)
            events = self._check_epoch_locked(epoch)
        self._emit(events)

    # --------------------------------------------------------- subscriptions
    def subscribe_round(self, epoch, rnd):
        with self._lock:
            self._raise_if_terminal()
            ep = self._epoch_or_raise(epoch)
            handle = CompletionHandle(f"round ({epoch}, {rnd})")
            if self._round_satisfied_locked(ep, rnd):
                events = []
                if (epoch, rnd) not in self._completed_rounds:
                    self._record_round_locked(epoch, rnd)
                    events.append({"kind": "round_complete", "epoch": epoch,
                                   "round": rnd})
                handle._set_result({"epoch": epoch, "round": rnd})
            else:
                self._round_subs[epoch].append((rnd, handle))
                events = []
        self._emit(events)
        return handle

    def subscribe_epoch(self, epoch):
        with self._lock:
            self._raise_if_terminal()
            ep = self._epoch_or_raise(epoch)
            handle = CompletionHandle(f"epoch {epoch}")
            if epoch in self._completed_epochs:
                handle._set_result({"epoch": epoch})
            else:
                self._epoch_subs[epoch].append(handle)
        return handle

    # --------------------------------------------------------------- queries
    def frontier(self, epoch):
        with self._lock:
            ep = self._epoch_or_raise(epoch)
            if ep.closed and ep.total == 0:
                return Frontier(epoch, True, None, [])
            frontier_round = 0 if not ep.closed else min(ep.round_totals)
            reasons = []
            if not ep.closed:
                for sid in sorted(ep.open_sources):
                    reasons.append({
                        "category": "source_open",
                        "source": sid,
                        "detail": f"source {sid!r} has not closed epoch {epoch}",
                    })
            reasons.extend(self._reasons_locked(ep))
            return Frontier(epoch, False, frontier_round, reasons)

    def epoch_snapshot(self):
        with self._lock:
            return {
                e: {
                    "closed": ep.closed,
                    "complete": e in self._completed_epochs,
                    "outstanding": ep.total,
                }
                for e, ep in self._epochs.items()
            }

    def declared_epochs(self):
        with self._lock:
            return sorted(self._epochs)

    # ------------------------------------------------------- internal checks
    def _epoch_or_raise(self, epoch):
        ep = self._epochs.get(epoch)
        if ep is None:
            raise UndeclaredEpochError(f"epoch {epoch} was not declared")
        return ep

    def _check_running(self):
        if not self._started or self._terminal_exc is not None:
            raise RunStoppedError("run is not running")

    def _raise_if_terminal(self):
        if self._terminal_exc is not None:
            exc = self._terminal_exc
            if isinstance(exc, (RunFailedError, RunCancelledError)):
                raise exc
            raise RunStoppedError("run is no longer running")

    @staticmethod
    def _round_satisfied_locked(ep, rnd):
        if not ep.closed:
            return False
        # sum of round_totals over rounds <= rnd == 0  <=>  min nonzero > rnd
        return not ep.round_totals or min(ep.round_totals) > rnd

    def _record_round_locked(self, epoch, rnd):
        self._completed_rounds.add((epoch, rnd))

    def _check_epoch_locked(self, epoch):
        """Fire newly-satisfied completions for one epoch.  Lock must be held."""
        ep = self._epochs[epoch]
        if not ep.closed or self._terminal_exc is not None:
            return []
        events = []
        candidates = set(ep.ever_rounds)
        candidates.update(r for r, _ in self._round_subs.get(epoch, ()))
        for rnd in sorted(candidates):
            if (epoch, rnd) in self._completed_rounds:
                continue
            if self._round_satisfied_locked(ep, rnd):
                self._record_round_locked(epoch, rnd)
                events.append({"kind": "round_complete", "epoch": epoch,
                               "round": rnd})
        if ep.total == 0 and epoch not in self._completed_epochs:
            self._completed_epochs.add(epoch)
            events.append({"kind": "epoch_complete", "epoch": epoch,
                           "round": None})
            if all(e in self._completed_epochs for e in self._epochs):
                events.append({"kind": "all_complete", "epoch": None,
                               "round": None})
        events.extend(self._flush_epoch_events_locked(epoch))
        return events

    def _flush_epoch_events_locked(self, epoch):
        """Deliver handle results for everything already completed."""
        ep = self._epochs[epoch]
        pending = self._round_subs.get(epoch)
        if pending:
            still = []
            for (rnd, handle) in pending:
                if (epoch, rnd) in self._completed_rounds:
                    handle._set_result({"epoch": epoch, "round": rnd})
                else:
                    still.append((rnd, handle))
            if still:
                self._round_subs[epoch] = still
            else:
                self._round_subs.pop(epoch, None)
        if epoch in self._completed_epochs:
            subs = self._epoch_subs.pop(epoch, [])
            for handle in subs:
                handle._set_result({"epoch": epoch})
        return []

    def _reasons_locked(self, ep):
        grouped = defaultdict(lambda: {"count": 0, "rounds": defaultdict(int)})
        for (category, location, rnd), n in ep.counts.items():
            g = grouped[(category, location)]
            g["count"] += n
            g["rounds"][rnd] += n
        reasons = []
        feedback = defaultdict(lambda: {"held": 0, "buffered": 0, "spilled": 0,
                                        "rounds": defaultdict(int)})
        for (category, location), g in grouped.items():
            rounds = dict(sorted(g["rounds"].items()))
            if category == QUEUED:
                reasons.append({"category": "queued", "edge": location,
                                "count": g["count"], "rounds": rounds})
            elif category == TRANSFORMING:
                reasons.append({"category": "transforming", "node": location,
                                "count": g["count"], "rounds": rounds})
            elif category == SINK_UNACKED:
                reasons.append({"category": "sink_unacked", "node": location,
                                "count": g["count"], "rounds": rounds})
            elif category in _FEEDBACK_CATS:
                sub = feedback[location]
                key = {FB_HELD: "held", FB_BUFFERED: "buffered",
                       FB_SPILLED: "spilled"}[category]
                sub[key] += g["count"]
                for r, n in g["rounds"].items():
                    sub["rounds"][r] += n
        for edge, sub in feedback.items():
            reasons.append({
                "category": "feedback_pending",
                "edge": edge,
                "count": sub["held"] + sub["buffered"] + sub["spilled"],
                "held": sub["held"],
                "buffered": sub["buffered"],
                "spilled": sub["spilled"],
                "rounds": dict(sorted(sub["rounds"].items())),
            })
        reasons.sort(key=lambda r: (r["category"],
                                    r.get("edge") or r.get("node") or ""))
        return reasons
