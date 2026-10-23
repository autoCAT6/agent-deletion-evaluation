"""Read durable fixture artifacts independently, without importing its implementation."""

import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

CANARY = "SYNTHETIC_R32_DELETE_ME_7d08a9"
TARGET = "synthetic-subject-delete"
OTHER = "synthetic-subject-keep"


def read_json(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path, query, parameters=()):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        return db.execute(query, parameters).fetchall()


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def verify(root, packet):
    summary = read_json(root / "summary.json")
    cases = read_json(packet / "cases.json")
    manifest = read_json(root / "sha256.json")
    actual_files = {p.relative_to(root).as_posix() for p in root.rglob("*")
                    if p.is_file() and p.name != "sha256.json"}
    require(actual_files == set(manifest), "Missing or untracked artifact file")
    for name, expected_sha in manifest.items():
        require(sha(root / name) == expected_sha, "Artifact hash mismatch: " + name)
    require(summary["source_sha256"] == sha(packet / "deletion_fixture.py"), "Fixture source mismatch")
    require(summary["cases_sha256"] == sha(packet / "cases.json"), "Pinned cases mismatch")
    ids = [c["id"] for c in cases]
    require(len(ids) == len(set(ids)) == 14, "Expected 14 unique pinned cases")
    require({r["case"] for r in summary["results"]} == set(ids), "Summary case coverage mismatch")
    require(len(summary["results"]) == 14, "Duplicate summary result")
    require(summary["all_expected_observations"] is True, "Fixture reported a failed expectation")
    checked = []
    for case in cases:
        directory = root / case["id"]
        result = read_json(directory / "result.json")
        final = result["final"]
        a, b = directory / "agent_a.sqlite", directory / "agent_b.sqlite"
        main = rows(a, "SELECT value FROM records WHERE subject=?", (TARGET,))
        derived = rows(b, "SELECT value FROM memories WHERE subject=?", (TARGET,))
        unrelated = rows(a, "SELECT value FROM records WHERE subject=?", (OTHER,)) == [
            ("synthetic-non-target-value",)] and rows(b, "SELECT value FROM memories WHERE subject=?", (OTHER,)) == [
                ("synthetic-non-target-value",)]
        require(all(row[0] == CANARY for row in main + derived), "Unexpected target value")
        snapshots = {}
        for filename, table in (("snapshot_a.sqlite", "records"), ("snapshot_b.sqlite", "memories")):
            path = directory / filename
            snapshots[filename] = path.exists() and bool(rows(path, f"SELECT value FROM {table} WHERE subject=?", (TARGET,)))
        text = (directory / "audit.jsonl").read_text()
        events = [json.loads(line) for line in text.splitlines()]
        require([e["tick"] for e in events] == sorted(e["tick"] for e in events), "Audit order mismatch")
        queue = rows(a, "SELECT body FROM outbox")
        raw_files = [p.name for p in sorted(directory.glob("*.sqlite")) if CANARY.encode() in p.read_bytes()]
        statuses = [r[0] for r in rows(a, "SELECT status FROM requests ORDER BY request_id")]
        actual = {
            "main_query_visible": bool(main), "derived_query_visible": bool(derived),
            "query_clean": not main and not derived,
            "snapshot_query_visible": snapshots, "snapshot_retains_canary": any(snapshots.values()),
            "audit_retains_canary": CANARY in text,
            "outbox_retains_canary": any(CANARY in row[0] for row in queue),
            "raw_sqlite_files_containing_canary": raw_files,
            "pending_outbox": len(queue), "request_statuses": statuses,
            "receipt_completed": statuses == ["completed"],
            "applied_deletes": rows(b, "SELECT value FROM counters WHERE name='applied_deletes'")[0][0],
            "tombstones": rows(b, "SELECT COUNT(*) FROM tombstones WHERE subject=?", (TARGET,))[0][0],
            "unrelated_subject_preserved": unrelated,
        }
        actual["inspected_storage_clean"] = (actual["query_clean"] and not actual["snapshot_retains_canary"]
            and not actual["audit_retains_canary"] and not actual["outbox_retains_canary"] and not raw_files)
        require(all(final[k] == v for k, v in actual.items()), "Persisted state differs: " + case["id"])
        require(all(actual[k] == v for k, v in case["expected_final"].items()), "Pinned observation differs: " + case["id"])
        require(unrelated, "Unrelated subject was changed")
        for name, expected in case.get("expected_action_decisions", {}).items():
            require([e.get("decision") for e in events if e["action"] == name] == expected,
                    "Replay or retry decision differs: " + case["id"])
        for name, expected in case.get("expected_actions", {}).items():
            require(sum(event["action"] == name for event in events) == expected,
                    "Audit action count differs: " + case["id"] + "/" + name)
        observations = read_json(directory / "observations.json")
        stages = [observation["stage"] for observation in observations]
        require(len(stages) == len(set(stages)), "Duplicate checkpoint")
        require(observations[0]["stage"] == "before_request" and all(
            observations[0][key] for key in ("main_query_visible", "derived_query_visible")),
            "Recorded initial conditions differ")
        require(all(observation["unrelated_subject_preserved"] for observation in observations),
                "Recorded unrelated-subject checkpoint differs")
        for expectation in case.get("expected_observations", []):
            found = [o for o in observations if o["stage"] == expectation["stage"]]
            require(len(found) == 1 and all(found[0][key] == value
                    for key, value in expectation.items() if key != "stage"),
                    "Recorded checkpoint expectation differs: " + case["id"])
        require(observations[-1] == final, "Final checkpoint mismatch")
        require(result["all_expected_observations"] is True and result["checks"]
                and all(value is True for value in result["checks"].values()),
                "Fixture result contains failed or missing checks: " + case["id"])
        require(next(r for r in summary["results"] if r["case"] == case["id"]) == result,
                "Summary/result mismatch")
        checked.append({"case": case["id"], "durable_state_matches": True,
                        "query_clean": actual["query_clean"],
                        "inspected_storage_clean": actual["inspected_storage_clean"]})
    return {"case_count": len(checked), "files_hashed": len(manifest),
            "all_durable_state_checks_passed": True, "cases": checked}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(args.directory, Path(__file__).parent)
    if args.report:
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
