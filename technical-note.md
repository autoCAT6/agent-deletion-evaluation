# Measuring deletion propagation beyond the receipt

Yanzhi Xin, October 22, 2026

For R3.2 evaluations, I suggest recording a deletion receipt separately from live query visibility, retained copies and behavior after replay or restore. A useful test should name its storage surfaces, retention deadline and allowed audit metadata before running.

The [Agent Privacy Risk Taxonomy v0.1](https://mlcommons.org/wp-content/uploads/2026/10/ML-Commons-Agent-Privacy-Taxonomy-v01-1.pdf) describes R3.2 as a failure to propagate data rights across agents and derived copies. R1.3 covers retention of execution traces without appropriate deletion controls. R5.2 addresses the need for privacy-preserving interaction logs. This fixture exercises a narrow deletion example across those boundaries.

## Proposed observations

| Observation | What to record |
|---|---|
| Request receipt | Whether the request is accepted or claimed complete, and its declared scope |
| Live visibility | Whether each known source or derived store still returns the target |
| Retained copies | Whether declared snapshots, queued payloads or logs retain it at the deadline |
| Replay and restore | Whether delayed input or restored state makes a deleted value readable again |
| Other data | Whether an unrelated subject remains intact |

A completion receipt should identify the surfaces it covers. A live-store receipt can precede a separately declared snapshot expiry. An evaluator should preserve both observations instead of treating the receipt as proof of the wider retention outcome.

## Executable example

The attached Python fixture uses two deterministic agent roles, two SQLite stores, a durable outbox and an append-only JSONL audit log. It takes real SQLite backups before deletion. All values are synthetic. There are no models or external services.

The source role commits local deletion and the outbox event together. The derived role commits deletion, an event receipt and a revision tombstone together. Fourteen pinned cases exercise successful propagation and deliberately faulty modes:

* A premature completion receipt leaves a readable downstream record.
* A lost acknowledgement retries the same event without applying the deletion twice. An exception before commit rolls back the derived changes and leaves the outbox available for retry.
* Event-ID deduplication rejects an identical retry. A stale copy with a new ID resurrects the value when the tombstone is disabled. A deletion revision blocks that stale copy when enabled.
* Live queries can return no target while a snapshot or payload-bearing audit entry still retains it.
* Restoring a snapshot can also restore the old value and remove its tombstone. Reapplying an authoritative deletion ledger before serving the restored store prevents that result in the fixture.

All 14 cases matched their stated expectations, including the deliberate failures. A separate verifier reopens the saved databases and scans the retained files. The result is a tested evaluation design, not a measured failure rate for deployed agents.

## Scope

The inspection deadline is logical tick 6, not a wall-clock latency claim. The fixture permits synthetic subject IDs, event digests and deletion metadata to remain. It checks exact-canary retention in the declared files; it does not prove removal from deleted filesystem blocks, caches, training data, semantic derivatives or undeclared stores. Snapshot expiry removes files without claiming secure media erasure.

Revision authority, newly granted permissions and concurrent distributed execution are outside this fixture. A single injected transaction exception is also narrower than a crash-recovery guarantee. This is a proposed evaluation fixture, not an MLCommons benchmark, a real-model evaluation or a privacy guarantee.

Would separate receipt, live-visibility and retention observations be useful in the R3.2 evaluation design? The cases could also test whether an adapter's deletion scope includes derived stores and restore paths.
