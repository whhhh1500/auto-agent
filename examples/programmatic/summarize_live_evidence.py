#!/usr/bin/env python3
"""Summarize safe live-evidence JSON without reading model payloads or secrets."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


LIVE_SCHEMA = "harness.programmatic.live-evidence/v1"
REPOSITORY_SCHEMA = "harness.programmatic.live-repository/v1"
REPOSITORY_AVAILABILITY_SCHEMA = "harness.programmatic.live-repository-availability/v1"
MEMORY_VALIDITY_SCHEMA = "harness.programmatic.live-memory-validity/v1"
MEMORY_AUTHORITY_SCHEMA = "harness.programmatic.live-memory-authority/v1"
MEMORY_AUTHORITY_LOOKUP_SCHEMA = "harness.programmatic.live-memory-authority-lookup/v1"
MEMORY_SCOPE_SCHEMA = "harness.programmatic.live-memory-scope/v1"
SUMMARY_ROLLOVER_SCHEMA = "harness.programmatic.live-summary-rollover/v1"
SQLITE_RECOVERY_SCHEMA = "harness.programmatic.live-sqlite-recovery/v1"
SIDECAR_SCHEMAS = {
    "harness.programmatic.live-wire-evidence/v1",
    "harness.programmatic.live-disclosure-evidence/v1",
}
SUMMARY_SCHEMA = "harness.programmatic.live-evidence-summary/v1"


class DuplicatePhysicalFileError(ValueError):
    """Raised before aggregation when two paths refer to one JSON file."""


def _nonnegative_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _required_string(record: dict[str, Any], key: str) -> str | None:
    value = record.get(key)
    return value if isinstance(value, str) and value else None


def _group_key(record: dict[str, Any]) -> tuple[str, str, str] | None:
    values = tuple(_required_string(record, key) for key in ("source_revision", "requested_model", "case_id"))
    return values if all(values) else None  # type: ignore[return-value]


def _new_group() -> dict[str, Any]:
    return {
        "samples": 0,
        "passed": 0,
        "failed": 0,
        "reported_usage_subtotal": {"input_tokens": 0, "output_tokens": 0},
        "unknown_usage_samples": 0,
        "ordinary_invocation_evidence": {
            "verified_samples": 0,
            "unverified_samples": 0,
            "unavailable_samples": 0,
            "actual_adapter_calls": 0,
            "unknown_adapter_call_samples": 0,
            "pacing_canceled_before_adapter": 0,
        },
        "measurement_counts": {
            "live_model_rounds": 0,
            "summary_experiment_adapter_invocations": 0,
            "memory_scope_model_rounds": 0,
            "memory_authority_lookup_model_rounds": 0,
            "rollover_adapter_invocations": 0,
            "unknown_rollover_adapter_invocation_samples": 0,
            "sqlite_recovery_adapter_invocations": 0,
            "unknown_sqlite_recovery_adapter_invocation_samples": 0,
        },
    }


def _add_usage(group: dict[str, Any], input_tokens: int, output_tokens: int, complete: bool) -> None:
    usage = group["reported_usage_subtotal"]
    usage["input_tokens"] += input_tokens
    usage["output_tokens"] += output_tokens
    if not complete:
        group["unknown_usage_samples"] += 1


def _live_measurement(record: dict[str, Any]) -> tuple[int, int, bool, int, int] | None:
    """Return input/output, complete, live rounds, and summary invocations."""
    summary = record.get("summary")
    if isinstance(summary, dict):
        # Summary experiments intentionally have no generic rounds. Their cost
        # comes from the summary's total ledger, which includes answer calls.
        total_usage = summary.get("total_usage")
        requests = _nonnegative_int(summary.get("total_requests"))
        complete = summary.get("usage_reported")
        if not isinstance(total_usage, dict) or requests is None or not isinstance(complete, bool):
            return None
        input_tokens = _nonnegative_int(total_usage.get("input_tokens"))
        output_tokens = _nonnegative_int(total_usage.get("output_tokens"))
        if input_tokens is None or output_tokens is None:
            return None
        return input_tokens, output_tokens, complete, 0, requests

    rounds = record.get("rounds")
    if not isinstance(rounds, list):
        return None
    input_tokens = 0
    output_tokens = 0
    # An ordinary live record has no provider-usage evidence when it has no
    # rounds. Summary experiments take the separate branch above.
    complete = bool(rounds)
    for round_record in rounds:
        if not isinstance(round_record, dict):
            return None
        reported = round_record.get("usage_reported")
        current_input = _nonnegative_int(round_record.get("input_tokens"))
        current_output = _nonnegative_int(round_record.get("output_tokens"))
        if current_input is None or current_output is None or not isinstance(reported, bool):
            return None
        if reported:
            input_tokens += current_input
            output_tokens += current_output
        complete = complete and reported
    if "invocation_evidence" in record:
        # A new record cannot claim complete accounting solely because all
        # rounds contain numbers when its invocation reconciliation disagrees.
        # Historical records retain their old reported-usage interpretation;
        # their missing invocation verification is counted independently.
        _, _, verified = _ordinary_invocation_measurement(record)
        complete = complete and verified
    return input_tokens, output_tokens, complete, len(rounds), 0


def _ordinary_invocation_measurement(record: dict[str, Any]) -> tuple[int | None, int, bool]:
    evidence = record.get("invocation_evidence")
    rounds = record.get("rounds")
    if not isinstance(evidence, dict) or not isinstance(rounds, list):
        return None, 0, False
    calls = _nonnegative_int(evidence.get("actual_adapter_calls"))
    canceled = _nonnegative_int(evidence.get("pacing_canceled_before_adapter"))
    predicates = ("reported_usage_complete", "ledger_usage_matched", "unique_ledger_invocation_ids", "usage_protocol_consistent")
    if (evidence.get("adapter_calls_observed") is not True or calls is None or canceled is None
            or calls + canceled > len(rounds)
            or any(not isinstance(evidence.get(name), bool) for name in predicates)):
        return None, 0, False
    verified = bool(rounds) and calls + canceled == len(rounds) and all(evidence[name] for name in predicates)
    return calls, canceled, verified


def _memory_scope_measurement(record: dict[str, Any]) -> tuple[int, int, bool, int] | None:
    input_tokens = _nonnegative_int(record.get("reported_input_tokens"))
    output_tokens = _nonnegative_int(record.get("reported_output_tokens"))
    model_rounds = _nonnegative_int(record.get("model_rounds"))
    complete = record.get("usage_complete")
    if input_tokens is None or output_tokens is None or model_rounds is None or not isinstance(complete, bool):
        return None
    return input_tokens, output_tokens, complete, model_rounds


def _memory_authority_lookup_measurement(record: dict[str, Any]) -> tuple[int, int, bool, int] | None:
    """Return only the lookup experiment's safe usage counters and rounds."""
    input_tokens = _nonnegative_int(record.get("usage_input_tokens"))
    output_tokens = _nonnegative_int(record.get("usage_output_tokens"))
    model_rounds = _nonnegative_int(record.get("model_rounds"))
    run_attempts = _nonnegative_int(record.get("run_attempts"))
    tool_calls = _nonnegative_int(record.get("tool_calls"))
    complete = record.get("usage_complete")
    runtime_status = record.get("runtime_status")
    predicates = (
        "profile_only_pair",
        "lookup_key_exact",
        "lookup_found",
        "lookup_entry_structured",
        "authority_structured",
        "conflict_recognized",
        "context_exact_pair",
        "journal_exact_pair",
        "current_answer",
        "current_durable_answer",
        "peer_scope_isolated",
        "foreign_absent",
    )
    if (
        input_tokens is None
        or output_tokens is None
        or model_rounds is None
        or run_attempts is None
        or tool_calls is None
        or not isinstance(complete, bool)
        or not isinstance(runtime_status, str)
        or not runtime_status
        or any(not isinstance(record.get(name), bool) for name in predicates)
    ):
        return None
    if record.get("acceptance_passed") is True and not (
        runtime_status == "completed"
        and run_attempts == 1
        and model_rounds == 2
        and tool_calls == 2
        and complete
        and all(record[name] for name in predicates)
    ):
        return None
    return input_tokens, output_tokens, complete, model_rounds


def _summary_rollover_measurement(record: dict[str, Any]) -> tuple[int, int, bool, int, bool]:
    """Return reported three-run subtotals and independent adapter calls.

    A malformed or absent run is unknown, but it must not erase usage safely
    reported by other rollover runs, including failed runs.
    """
    run_usage = record.get("run_usage")
    input_tokens = 0
    output_tokens = 0
    complete = (
        record.get("usage_complete") is True
        and record.get("usage_ledger_matches") is True
        and isinstance(run_usage, list)
        and len(run_usage) == 3
    )
    if isinstance(run_usage, list):
        for run in run_usage:
            if not isinstance(run, dict):
                complete = False
                continue
            current_input = _nonnegative_int(run.get("input_tokens"))
            current_output = _nonnegative_int(run.get("output_tokens"))
            reports = _nonnegative_int(run.get("reports"))
            run_complete = run.get("complete")
            if current_input is None or current_output is None:
                complete = False
            else:
                input_tokens += current_input
                output_tokens += current_output
            if reports != 1 or run_complete is not True:
                complete = False
    model_calls = _nonnegative_int(record.get("model_calls"))
    if model_calls is None:
        return input_tokens, output_tokens, False, 0, False
    return input_tokens, output_tokens, complete and model_calls == 3, model_calls, True


def _sqlite_recovery_measurement(record: dict[str, Any]) -> tuple[int, int, bool, int, bool]:
    """Return reported two-run recovery subtotals and adapter call count."""
    run_usage = record.get("run_usage")
    input_tokens = 0
    output_tokens = 0
    complete = (
        record.get("usage_complete") is True
        and record.get("usage_ledger_matches") is True
        and isinstance(run_usage, list)
        and len(run_usage) == 2
    )
    if isinstance(run_usage, list):
        for run in run_usage:
            if not isinstance(run, dict):
                complete = False
                continue
            current_input = _nonnegative_int(run.get("input_tokens"))
            current_output = _nonnegative_int(run.get("output_tokens"))
            invocation_id = run.get("invocation_id")
            if current_input is None or current_output is None:
                complete = False
            else:
                input_tokens += current_input
                output_tokens += current_output
            if not isinstance(invocation_id, str) or not invocation_id or run.get("complete") is not True:
                complete = False
    model_calls = _nonnegative_int(record.get("model_calls"))
    if model_calls is None:
        return input_tokens, output_tokens, False, 0, False
    return input_tokens, output_tokens, complete and model_calls == 2, model_calls, True


def _add_record(groups: dict[tuple[str, str, str], dict[str, Any]], record: dict[str, Any]) -> bool:
    key = _group_key(record)
    accepted = record.get("acceptance_passed")
    if key is None or not isinstance(accepted, bool):
        return False
    schema = record.get("schema")
    if schema in {LIVE_SCHEMA, REPOSITORY_SCHEMA, REPOSITORY_AVAILABILITY_SCHEMA, MEMORY_VALIDITY_SCHEMA, MEMORY_AUTHORITY_SCHEMA}:
        measurement = _live_measurement(record)
        if measurement is None:
            return False
        input_tokens, output_tokens, complete, rounds, invocations = measurement
    elif schema == MEMORY_SCOPE_SCHEMA:
        measurement = _memory_scope_measurement(record)
        if measurement is None:
            return False
        input_tokens, output_tokens, complete, rounds = measurement
        invocations = 0
        invocation_known = True
    elif schema == MEMORY_AUTHORITY_LOOKUP_SCHEMA:
        measurement = _memory_authority_lookup_measurement(record)
        if measurement is None:
            return False
        input_tokens, output_tokens, complete, rounds = measurement
        invocations = 0
        invocation_known = True
    elif schema == SUMMARY_ROLLOVER_SCHEMA:
        input_tokens, output_tokens, complete, invocations, invocation_known = _summary_rollover_measurement(record)
        rounds = 0
    elif schema == SQLITE_RECOVERY_SCHEMA:
        input_tokens, output_tokens, complete, invocations, invocation_known = _sqlite_recovery_measurement(record)
        rounds = 0
    else:
        return False

    group = groups[key]
    group["samples"] += 1
    group["passed" if accepted else "failed"] += 1
    _add_usage(group, input_tokens, output_tokens, complete)
    measurements = group["measurement_counts"]
    if schema in {LIVE_SCHEMA, REPOSITORY_SCHEMA, REPOSITORY_AVAILABILITY_SCHEMA, MEMORY_VALIDITY_SCHEMA, MEMORY_AUTHORITY_SCHEMA}:
        measurements["live_model_rounds"] += rounds
        measurements["summary_experiment_adapter_invocations"] += invocations
        if not isinstance(record.get("summary"), dict):
            accounting = group["ordinary_invocation_evidence"]
            calls, canceled, verified = _ordinary_invocation_measurement(record)
            if calls is None:
                accounting["unavailable_samples"] += 1
                accounting["unknown_adapter_call_samples"] += 1
            else:
                accounting["actual_adapter_calls"] += calls
                accounting["pacing_canceled_before_adapter"] += canceled
                accounting["verified_samples" if verified else "unverified_samples"] += 1
    elif schema == MEMORY_SCOPE_SCHEMA:
        measurements["memory_scope_model_rounds"] += rounds
    elif schema == MEMORY_AUTHORITY_LOOKUP_SCHEMA:
        measurements["memory_authority_lookup_model_rounds"] += rounds
    elif schema == SUMMARY_ROLLOVER_SCHEMA:
        measurements["rollover_adapter_invocations"] += invocations
        if not invocation_known:
            measurements["unknown_rollover_adapter_invocation_samples"] += 1
    else:
        measurements["sqlite_recovery_adapter_invocations"] += invocations
        if not invocation_known:
            measurements["unknown_sqlite_recovery_adapter_invocation_samples"] += 1
    return True


def _json_paths(directory: Path) -> Iterable[Path]:
    for path in sorted(directory.rglob("*.json")):
        if path.is_file():
            yield path


def summarize_directory(directory: Path) -> dict[str, Any]:
    """Build a deterministic, payload-free aggregation for an explicit directory."""
    if not directory.is_dir():
        raise ValueError("evidence directory is not readable")

    counts = {
        "accepted_files": 0,
        "sidecar_files_ignored": 0,
        "unknown_schema_files_skipped": 0,
        "invalid_json_files_skipped": 0,
        "invalid_record_files_skipped": 0,
    }
    groups: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(_new_group)
    physical_files: set[tuple[int, int]] = set()
    for path in _json_paths(directory):
        try:
            stat = path.stat()
        except OSError:
            counts["invalid_json_files_skipped"] += 1
            continue
        identity = (stat.st_dev, stat.st_ino)
        if identity in physical_files:
            raise DuplicatePhysicalFileError("duplicate physical evidence file")
        physical_files.add(identity)
        try:
            with path.open("r", encoding="utf-8") as stream:
                record = json.load(stream)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            counts["invalid_json_files_skipped"] += 1
            continue
        if not isinstance(record, dict):
            counts["invalid_record_files_skipped"] += 1
            continue
        schema = record.get("schema")
        if not isinstance(schema, str) or not schema:
            counts["invalid_record_files_skipped"] += 1
            continue
        if schema in SIDECAR_SCHEMAS:
            counts["sidecar_files_ignored"] += 1
            continue
        if schema not in {LIVE_SCHEMA, REPOSITORY_SCHEMA, REPOSITORY_AVAILABILITY_SCHEMA, MEMORY_VALIDITY_SCHEMA, MEMORY_AUTHORITY_SCHEMA, MEMORY_SCOPE_SCHEMA, MEMORY_AUTHORITY_LOOKUP_SCHEMA, SUMMARY_ROLLOVER_SCHEMA, SQLITE_RECOVERY_SCHEMA}:
            counts["unknown_schema_files_skipped"] += 1
            continue
        if not _add_record(groups, record):
            counts["invalid_record_files_skipped"] += 1
            continue
        counts["accepted_files"] += 1

    output_groups = []
    for (source_revision, requested_model, case_id), values in sorted(groups.items()):
        output_groups.append(
            {
                "source_revision": source_revision,
                "requested_model": requested_model,
                "case_id": case_id,
                **values,
            }
        )
    return {"schema": SUMMARY_SCHEMA, "files": counts, "groups": output_groups}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize safe programmatic live-evidence JSON.")
    parser.add_argument("directory", type=Path, help="explicit evidence directory to scan recursively")
    args = parser.parse_args(argv)
    try:
        summary = summarize_directory(args.directory)
    except DuplicatePhysicalFileError:
        json.dump({"schema": SUMMARY_SCHEMA, "error": {"code": "duplicate_physical_file"}}, sys.stdout, sort_keys=True)
        sys.stdout.write("\n")
        return 2
    except ValueError:
        json.dump({"schema": SUMMARY_SCHEMA, "error": {"code": "invalid_directory"}}, sys.stdout, sort_keys=True)
        sys.stdout.write("\n")
        return 2
    json.dump(summary, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
