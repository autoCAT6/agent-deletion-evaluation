"""Proposed local evaluation fixture for deletion propagation across two roles.

Python standard library only. No models, networks, user data or external services.
The deliberate faulty modes are controls, not claims about a deployed product.
"""

import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import platform
import shutil
import sqlite3

CANARY = "SYNTHETIC_R32_DELETE_ME_7d08a9"
TARGET = "synthetic-subject-delete"
OTHER = "synthetic-subject-keep"
KEEP = "synthetic-non-target-value"


def dump(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


@contextmanager
def connection(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA secure_delete=ON")
    try:
        with db:
            yield db
    finally:
        db.close()


class AgentA:
    """Deterministic source-store role with a durable deletion outbox."""

    def __init__(self, path):
        self.path = path
        with connection(path) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS records (
                    subject TEXT PRIMARY KEY, value TEXT NOT NULL, revision INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS requests (
                    request_id TEXT PRIMARY KEY, subject TEXT NOT NULL,
                    revision INTEGER NOT NULL, status TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS outbox (
                    event_id TEXT PRIMARY KEY, body TEXT NOT NULL);
            """)

    def seed(self, subject, value):
        with connection(self.path) as db:
            db.execute("INSERT INTO records VALUES (?, ?, 1)", (subject, value))

    def request_delete(self, request_id, subject=TARGET, revision=2, premature=False):
        with connection(self.path) as db:
            old = db.execute("SELECT * FROM requests WHERE request_id=?", (request_id,)).fetchone()
            if old:
                if (old["subject"], old["revision"]) != (subject, revision):
                    raise ValueError("request_id reused with a different deletion target")
                return {"request_id": request_id, "status": old["status"], "duplicate": True}
            event = {"id": "delete-" + request_id, "kind": "delete", "subject": subject,
                     "revision": revision}
            # Source deletion, durable request and outbox enqueue commit together.
            db.execute("DELETE FROM records WHERE subject=? AND revision<=?", (subject, revision))
            status = "completed" if premature else "accepted"
            db.execute("INSERT INTO requests VALUES (?, ?, ?, ?)", (request_id, subject, revision, status))
            db.execute("INSERT INTO outbox VALUES (?, ?)", (event["id"], dump(event)))
            return {"request_id": request_id, "status": status, "duplicate": False}

    def pending(self):
        with connection(self.path) as db:
            return [json.loads(r[0]) for r in db.execute("SELECT body FROM outbox ORDER BY event_id")]

    def acknowledge(self, event):
        with connection(self.path) as db:
            db.execute("DELETE FROM outbox WHERE event_id=?", (event["id"],))
            db.execute("UPDATE requests SET status='completed' WHERE request_id=?",
                       (event["id"].removeprefix("delete-"),))

    def deletions(self):
        with connection(self.path) as db:
            return [dict(r) for r in db.execute("SELECT * FROM requests ORDER BY request_id")]


class AgentB:
    """Deterministic derived-store role with atomic deduplication and tombstones."""

    def __init__(self, path, use_tombstone=True):
        self.path, self.use_tombstone = path, use_tombstone
        with connection(path) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS memories (
                    subject TEXT PRIMARY KEY, value TEXT NOT NULL, revision INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS tombstones (
                    subject TEXT PRIMARY KEY, revision INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS processed (
                    event_id TEXT PRIMARY KEY, body_sha256 TEXT NOT NULL, decision TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS counters (
                    name TEXT PRIMARY KEY, value INTEGER NOT NULL);
                INSERT OR IGNORE INTO counters VALUES ('applied_deletes', 0);
            """)

    def apply(self, event, fail_before_commit=False):
        digest = hashlib.sha256(dump(event).encode()).hexdigest()
        with connection(self.path) as db:
            old = db.execute("SELECT * FROM processed WHERE event_id=?", (event["id"],)).fetchone()
            if old:
                if old["body_sha256"] != digest:
                    raise ValueError("event_id reused with different content")
                return "duplicate"
            if event["kind"] == "delete":
                db.execute("DELETE FROM memories WHERE subject=? AND revision<=?",
                           (event["subject"], event["revision"]))
                if self.use_tombstone:
                    db.execute("""INSERT INTO tombstones VALUES (?, ?)
                        ON CONFLICT(subject) DO UPDATE SET revision=max(revision, excluded.revision)""",
                               (event["subject"], event["revision"]))
                db.execute("UPDATE counters SET value=value+1 WHERE name='applied_deletes'")
                decision = "deleted"
            elif event["kind"] == "upsert":
                tombstone = db.execute("SELECT revision FROM tombstones WHERE subject=?",
                                       (event["subject"],)).fetchone()
                if tombstone and event["revision"] <= tombstone[0]:
                    decision = "blocked_by_tombstone"
                else:
                    db.execute("""INSERT INTO memories VALUES (?, ?, ?)
                        ON CONFLICT(subject) DO UPDATE SET value=excluded.value, revision=excluded.revision
                        WHERE excluded.revision >= memories.revision""",
                               (event["subject"], event["value"], event["revision"]))
                    decision = "stored"
            else:
                raise ValueError("unknown event kind")
            db.execute("INSERT INTO processed VALUES (?, ?, ?)", (event["id"], digest, decision))
            if fail_before_commit:
                raise RuntimeError("injected failure before derived transaction commit")
            return decision


def backup(source, destination):
    with sqlite3.connect(source) as src, sqlite3.connect(destination) as dst:
        src.backup(dst)


def count(path, table, subject):
    with sqlite3.connect(path) as db:
        return db.execute(f"SELECT COUNT(*) FROM {table} WHERE subject=?", (subject,)).fetchone()[0]


def observe(directory, stage, tick):
    """Reopen durable state; do not infer deletion from a receipt or handler result."""
    a, b = directory / "agent_a.sqlite", directory / "agent_b.sqlite"
    snapshots = {}
    for name, table in (("snapshot_a.sqlite", "records"), ("snapshot_b.sqlite", "memories")):
        path = directory / name
        snapshots[name] = bool(path.exists() and count(path, table, TARGET))
    with sqlite3.connect(a) as db:
        statuses = [r[0] for r in db.execute("SELECT status FROM requests ORDER BY request_id")]
        pending = db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        queue_canary = any(CANARY in r[0] for r in db.execute("SELECT body FROM outbox"))
    with sqlite3.connect(b) as db:
        delete_count = db.execute("SELECT value FROM counters WHERE name='applied_deletes'").fetchone()[0]
        tombstones = db.execute("SELECT COUNT(*) FROM tombstones WHERE subject=?", (TARGET,)).fetchone()[0]
    main_visible, derived_visible = bool(count(a, "records", TARGET)), bool(count(b, "memories", TARGET))
    audit_canary = CANARY in (directory / "audit.jsonl").read_text()
    raw_paths = [p.name for p in sorted(directory.glob("*.sqlite")) if CANARY.encode() in p.read_bytes()]
    clean_query = not main_visible and not derived_visible
    return {"stage": stage, "logical_tick": tick, "main_query_visible": main_visible,
            "derived_query_visible": derived_visible, "query_clean": clean_query,
            "snapshot_query_visible": snapshots, "snapshot_retains_canary": any(snapshots.values()),
            "audit_retains_canary": audit_canary, "outbox_retains_canary": queue_canary,
            "raw_sqlite_files_containing_canary": raw_paths,
            "inspected_storage_clean": clean_query and not any(snapshots.values()) and not audit_canary
                and not queue_canary and not raw_paths,
            "request_statuses": statuses, "receipt_completed": statuses == ["completed"],
            "pending_outbox": pending, "applied_deletes": delete_count, "tombstones": tombstones,
            "unrelated_subject_preserved": count(a, "records", OTHER) == 1 and count(b, "memories", OTHER) == 1}


def run_case(case, directory):
    directory.mkdir()
    a = AgentA(directory / "agent_a.sqlite")
    b = AgentB(directory / "agent_b.sqlite", use_tombstone=case.get("tombstone", True))
    observations, actions = [], []
    log = directory / "audit.jsonl"
    log.write_text("")

    def audit(tick, action, **values):
        entry = {"tick": tick, "action": action, **values}
        with log.open("a") as f:
            f.write(dump(entry) + "\n")
        actions.append(entry)

    original = {"id": "upsert-target-v1", "kind": "upsert", "subject": TARGET,
                "revision": 1, "value": CANARY}
    for subject, value in ((TARGET, CANARY), (OTHER, KEEP)):
        a.seed(subject, value)
        event = original if subject == TARGET else {
            "id": "upsert-other-v1", "kind": "upsert", "subject": subject, "revision": 1, "value": value}
        b.apply(event)
        fields = {"subject": subject, "event_id": event["id"]}
        if case.get("payload_log"):
            fields["payload"] = value
        audit(0, "initial_copy", **fields)
    backup(a.path, directory / "snapshot_a.sqlite")
    backup(b.path, directory / "snapshot_b.sqlite")
    observations.append(observe(directory, "before_request", 0))
    request_id = "r-delete-target"
    if not case.get("skip_delete"):
        receipt = a.request_delete(request_id, premature=case.get("premature_receipt", False))
        audit(1, "delete_requested", **receipt)
        if case.get("duplicate_request"):
            audit(1, "delete_retried", **a.request_delete(request_id))
        if case.get("conflicting_request"):
            try:
                a.request_delete(request_id, subject=OTHER)
            except ValueError:
                audit(1, "conflicting_request_rejected")
            else:
                raise AssertionError("conflicting request unexpectedly accepted")
    observations.append(observe(directory, "after_receipt", 1))
    last_delete = None
    if not case.get("skip_delete") and not case.get("drop_delivery"):
        for tick in (2, 3):
            for event in a.pending():
                last_delete = event
                try:
                    decision = b.apply(event, fail_before_commit=bool(case.get("fail_before_commit") and tick == 2))
                except RuntimeError:
                    audit(tick, "delivery_failed_before_commit", event_id=event["id"])
                    continue
                audit(tick, "delivery_result", event_id=event["id"], decision=decision)
                if case.get("lose_ack") and tick == 2:
                    audit(tick, "delivery_ack_lost", event_id=event["id"])
                else:
                    a.acknowledge(event)
            observations.append(observe(directory, "after_delivery_" + str(tick), tick))
        if case.get("duplicate_delivery"):
            audit(3, "duplicate_delivery_result", decision=b.apply(last_delete))
        if case.get("conflicting_event"):
            changed = dict(last_delete, subject=OTHER)
            try:
                b.apply(changed)
            except ValueError:
                audit(3, "conflicting_event_rejected")
            else:
                raise AssertionError("conflicting event unexpectedly accepted")
    if case.get("restore_snapshot"):
        shutil.copyfile(directory / "snapshot_b.sqlite", b.path)
        audit(4, "derived_snapshot_restored")
        # The fixture models a restore barrier: no public read is served before
        # the authoritative deletion ledger is reapplied in guarded mode.
        if case.get("restore_guard"):
            for deletion in a.deletions():
                b.apply({"id": "restore-" + deletion["request_id"], "kind": "delete",
                         "subject": deletion["subject"], "revision": deletion["revision"]})
            audit(4, "deletion_ledger_applied_before_serving")
        observations.append(observe(directory, "after_restore_barrier", 4))
    if case.get("stale_replay"):
        # The immutable test input is the external delayed message. It is not a
        # retained in-system queue. Both identical and newly identified retries
        # are exercised, so event-id deduplication alone cannot pass this case.
        audit(5, "identical_upsert_replay", decision=b.apply(original))
        audit(5, "new_id_stale_upsert_replay", decision=b.apply(dict(original, id="delayed-copy-v1")))
        observations.append(observe(directory, "after_stale_replay", 5))
    if not case.get("retain_snapshots"):
        for path in directory.glob("snapshot_*.sqlite"):
            path.unlink()
        audit(6, "snapshot_retention_deadline_applied")
    final = observe(directory, "deadline_inspection", 6)
    observations.append(final)
    checks = {}
    for field, expected in case["expected_final"].items():
        checks[field] = final[field] == expected
    checks["target_present_in_both_initial_stores"] = all(
        observations[0][name] for name in ("main_query_visible", "derived_query_visible"))
    checks["unrelated_subject_survives_every_observation"] = all(
        observation["unrelated_subject_preserved"] for observation in observations)
    for action, expected_count in case.get("expected_actions", {}).items():
        checks["action_count:" + action] = sum(a["action"] == action for a in actions) == expected_count
    for action, expected_decisions in case.get("expected_action_decisions", {}).items():
        checks["action_decisions:" + action] = [a.get("decision") for a in actions
                                               if a["action"] == action] == expected_decisions
    for expectation in case.get("expected_observations", []):
        found = [o for o in observations if o["stage"] == expectation["stage"]]
        checks["observation:" + expectation["stage"]] = len(found) == 1 and all(
            found[0][key] == value for key, value in expectation.items() if key != "stage")
    write_json(directory / "observations.json", observations)
    result = {"case": case["id"], "description": case["description"], "final": final,
              "checks": checks, "all_expected_observations": all(checks.values())}
    write_json(directory / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cases", type=Path, default=Path(__file__).with_name("cases.json"))
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit("Output already exists. Choose a fresh directory to preserve prior runs.")
    args.output.mkdir(parents=True)
    cases = json.loads(args.cases.read_text())
    if len({c["id"] for c in cases}) != len(cases):
        raise SystemExit("Duplicate case identifiers")
    results = [run_case(case, args.output / case["id"]) for case in cases]
    summary = {"fixture": "proposed deterministic deletion evaluation v0.1",
               "python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
               "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               "cases_sha256": hashlib.sha256(args.cases.read_bytes()).hexdigest(),
               "case_count": len(results), "results": results,
               "all_expected_observations": all(r["all_expected_observations"] for r in results)}
    write_json(args.output / "summary.json", summary)
    manifest = {p.relative_to(args.output).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(args.output.rglob("*")) if p.is_file()}
    write_json(args.output / "sha256.json", manifest)
    print(f"{sum(r['all_expected_observations'] for r in results)}/{len(results)} cases matched pinned expectations")
    for result in results:
        final = result["final"]
        print(result["case"], "query_clean=" + str(final["query_clean"]),
              "storage_clean=" + str(final["inspected_storage_clean"]),
              "expected=" + str(result["all_expected_observations"]))
    if not summary["all_expected_observations"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
