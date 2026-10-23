# Deletion propagation evaluation fixture

A proposed local fixture for feedback on MLCommons Agent Privacy Risk Taxonomy v0.1, R3.2. Start with [the technical note](technical-note.md).

The fixture runs two deterministic storage roles. It uses Python's standard library and SQLite, with synthetic values and no network calls. It is not an MLCommons benchmark or a real-model evaluation.

## Run

Python 3.10 or newer is required. The included run used Python 3.14.3 and SQLite 3.51.3. No package installation is needed.

From this directory:

```sh
python3 deletion_fixture.py --output my-results
python3 verify_artifacts.py my-results --report my-verification.json
```

Use a fresh output path. The runner refuses to overwrite an earlier run. Place a verification report outside the run directory because its inventory rejects untracked files.

To check the included results:

```sh
python3 verify_artifacts.py results
```

## Files and observations

`cases.json` pins each scenario's expected result. `results/summary.json` records all 14 outcomes and the source and case-file hashes. Per-case directories contain SQLite stores, the persisted audit log, recorded intermediate observations and the final result. Retained snapshots are included only where the case deliberately keeps them. `results/sha256.json` covers the saved artifacts.

`verify_artifacts.py` independently reads the durable final state, checks exact-canary presence and compares it with the saved result. It also checks recorded intermediate expectations and audit decisions for consistency. Intermediate checkpoints are observations made by the runner, not independently recoverable historical database states.

| Cases | What they exercise |
|---|---|
| 01 | Detection control with no deletion request |
| 02, 03 | Premature completion and undelivered propagation |
| 04 | Propagation followed by snapshot expiry |
| 05, 06 | Lost acknowledgement and duplicate requests or deliveries |
| 07, 08 | Stale copy with a new event ID, without and with a tombstone |
| 09, 10 | Snapshot and payload-log retention after live queries are clean |
| 11, 12 | Snapshot restore without and with a deletion-ledger barrier |
| 13 | Transaction rollback before commit, followed by retry |
| 14 | Conflicting reuse of a request ID and event ID |

The source store is Agent A. Agent B owns the derived store. These names describe deterministic roles, not model behavior. The deletion request is already valid and identified; natural-language interpretation, identity authentication and permission negotiation are not modeled.

The test's exact synthetic payload is `SYNTHETIC_R32_DELETE_ME_7d08a9`. The unrelated record must survive every checkpoint. The external delayed message is a test input, not an unreported retained in-system queue. For replay tests, both the original event ID and a new ID carrying revision 1 are delivered after revision 2 has been deleted.

The normal receipt marks the two live-store operations complete after delivery acknowledgement. Snapshot expiry has a separate fixture deadline at logical tick 6. `query_clean` means neither live table returns the target. `inspected_storage_clean` also checks the declared snapshot files, queued payloads, audit text and exact bytes in existing SQLite files. It does not mean every form of information about the subject has disappeared. IDs, digests and tombstones intentionally remain.

SQLite uses `journal_mode=DELETE` and `secure_delete=ON` to make existing-file inspection clear. The experiment does not evaluate erased disk blocks, operating-system caches, WAL operation, concurrency, distributed outages or model training data. The revision rule blocks old events at or below the deletion revision; authorization of genuinely newer writes is outside scope.

All 14 included cases match their pinned expectations. This includes controls that deliberately retain or resurrect a value. These counts describe the fixture, not a product's privacy performance or a population failure rate.

## Source

[MLCommons Agent Privacy Risk Taxonomy v0.1](https://mlcommons.org/wp-content/uploads/2026/10/ML-Commons-Agent-Privacy-Taxonomy-v01-1.pdf), R3.2 on PDF page 9; related R1.3 on pages 3 and 4, R5.2 on pages 11 and 12. The PDF's causal assessment separates observed behavior from downstream harm. This fixture records synthetic storage behavior only.

Prepared by Yanzhi Xin for technical feedback. Contact: erindotsing@gmail.com.
