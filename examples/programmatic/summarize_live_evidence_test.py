"""Offline tests for the payload-free live evidence summarizer."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from summarize_live_evidence import DuplicatePhysicalFileError, summarize_directory


class SummarizeLiveEvidenceTests(unittest.TestCase):
    def write_record(self, directory: Path, name: str, record: object) -> Path:
        path = directory / name
        path.write_text(json.dumps(record), encoding="utf-8")
        return path

    def test_invocation_verification_preserves_failed_usage_and_legacy_unknowns(self) -> None:
        evidence = {
            "adapter_calls_observed": True, "actual_adapter_calls": 2,
            "pacing_canceled_before_adapter": 0, "reported_usage_complete": True,
            "ledger_usage_matched": True, "unique_ledger_invocation_ids": True,
            "usage_protocol_consistent": True,
        }
        variants = {
            "verified": evidence,
            "mismatched": {**evidence, "ledger_usage_matched": False},
            "protocol_conflict": {**evidence, "usage_protocol_consistent": False},
            "impossible_count": {**evidence, "actual_adapter_calls": 3},
            "invalid_boolean_count": {**evidence, "actual_adapter_calls": True},
            "legacy": None,
        }
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for name, invocation in variants.items():
                record = {
                    "schema": "harness.programmatic.live-evidence/v1",
                    "source_revision": "revision-a", "requested_model": "model-a", "case_id": name,
                    "acceptance_passed": name == "verified",
                    "rounds": [{"input_tokens": 3, "output_tokens": 5, "usage_reported": True},
                               {"input_tokens": 7, "output_tokens": 11, "usage_reported": True}],
                }
                if invocation is not None:
                    record["invocation_evidence"] = invocation
                self.write_record(directory, name + ".json", record)
            summary = summarize_directory(directory)
            self.assertEqual(summary["files"]["accepted_files"], len(variants))
            groups = {item["case_id"]: item for item in summary["groups"]}
            for name, group in groups.items():
                self.assertEqual(group["reported_usage_subtotal"], {"input_tokens": 10, "output_tokens": 16})
                self.assertEqual(group["unknown_usage_samples"], int(name not in {"verified", "legacy"}))
                accounting = group["ordinary_invocation_evidence"]
                self.assertEqual(accounting["verified_samples"], int(name == "verified"))
                self.assertEqual(accounting["unverified_samples"], int(name in {"mismatched", "protocol_conflict"}))
                self.assertEqual(accounting["unavailable_samples"], int(name in {"legacy", "impossible_count", "invalid_boolean_count"}))

    def test_canceled_round_is_not_an_adapter_call(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write_record(directory, "canceled.json", {
                "schema": "harness.programmatic.live-evidence/v1",
                "source_revision": "revision-a", "requested_model": "model-a", "case_id": "canceled",
                "acceptance_passed": False,
                "rounds": [{"input_tokens": 3, "output_tokens": 5, "usage_reported": True},
                           {"input_tokens": 0, "output_tokens": 0, "usage_reported": False}],
                "invocation_evidence": {
                    "adapter_calls_observed": True, "actual_adapter_calls": 1,
                    "pacing_canceled_before_adapter": 1, "reported_usage_complete": True,
                    "ledger_usage_matched": True, "unique_ledger_invocation_ids": True,
                    "usage_protocol_consistent": True,
                },
            })
            group = summarize_directory(directory)["groups"][0]
            self.assertEqual(group["reported_usage_subtotal"], {"input_tokens": 3, "output_tokens": 5})
            self.assertEqual(group["measurement_counts"]["live_model_rounds"], 2)
            self.assertEqual(group["ordinary_invocation_evidence"]["actual_adapter_calls"], 1)
            self.assertEqual(group["ordinary_invocation_evidence"]["pacing_canceled_before_adapter"], 1)

    def test_repository_schema_keeps_quality_failure_separate_from_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write_record(directory, "repository.json", {
                "schema": "harness.programmatic.live-repository/v1",
                "source_revision": "revision-repo", "requested_model": "model-a", "case_id": "repo-limits",
                "acceptance_passed": False,
                "rounds": [{"input_tokens": 7, "output_tokens": 3, "usage_reported": True}],
                "invocation_evidence": {
                    "adapter_calls_observed": True, "actual_adapter_calls": 1,
                    "pacing_canceled_before_adapter": 0, "reported_usage_complete": True,
                    "ledger_usage_matched": True, "unique_ledger_invocation_ids": True,
                    "usage_protocol_consistent": True,
                },
                "repository": {"answer_quality": {"passed": False}, "source": "not-for-aggregate"},
            })
            result = summarize_directory(directory)
            group = result["groups"][0]
            self.assertEqual(result["files"]["accepted_files"], 1)
            self.assertEqual(group["failed"], 1)
            self.assertEqual(group["passed"], 0)
            self.assertEqual(group["ordinary_invocation_evidence"]["verified_samples"], 1)
            self.assertEqual(group["reported_usage_subtotal"], {"input_tokens": 7, "output_tokens": 3})
            self.assertNotIn("not-for-aggregate", json.dumps(result))

    def test_memory_validity_retains_partial_usage_and_quality_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for complete in (False, True):
                self.write_record(directory, str(complete) + ".json", {
                    "schema": "harness.programmatic.live-memory-validity/v1",
                    "source_revision": "revision-memory", "requested_model": "model-a",
                    "case_id": str(complete), "acceptance_passed": False,
                    "rounds": [{"input_tokens": 7, "output_tokens": 3, "usage_reported": True},
                               {"input_tokens": 11, "output_tokens": 5, "usage_reported": complete}],
                    "invocation_evidence": {
                        "adapter_calls_observed": True, "actual_adapter_calls": 2,
                        "pacing_canceled_before_adapter": 0, "reported_usage_complete": complete,
                        "ledger_usage_matched": complete, "unique_ledger_invocation_ids": True,
                        "usage_protocol_consistent": True,
                    },
                    "memory_validity": {"answer_matches_arm": False, "private": "do-not-aggregate"},
                })
            result = summarize_directory(directory)
            self.assertEqual(result["files"]["accepted_files"], 2)
            groups = {group["case_id"]: group for group in result["groups"]}
            for complete in (False, True):
                group = groups[str(complete)]
                self.assertEqual(group["failed"], 1)
                self.assertEqual(group["unknown_usage_samples"], int(not complete))
                self.assertEqual(group["reported_usage_subtotal"], {
                    "input_tokens": 18 if complete else 7, "output_tokens": 8 if complete else 3})
                self.assertEqual(group["ordinary_invocation_evidence"]["actual_adapter_calls"], 2)
                self.assertEqual(group["ordinary_invocation_evidence"]["verified_samples"], int(complete))
            self.assertNotIn("do-not-aggregate", json.dumps(result))

    def test_memory_authority_keeps_route_details_out_of_cost_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write_record(directory, "authority.json", {
                "schema": "harness.programmatic.live-memory-authority/v1",
                "source_revision": "revision-authority", "requested_model": "model-a",
                "case_id": "automatic_conflict", "acceptance_passed": True,
                "rounds": [{"input_tokens": 13, "output_tokens": 2, "usage_reported": True},
                           {"input_tokens": 17, "output_tokens": 3, "usage_reported": True}],
                "invocation_evidence": {
                    "adapter_calls_observed": True, "actual_adapter_calls": 2,
                    "pacing_canceled_before_adapter": 0, "reported_usage_complete": True,
                    "ledger_usage_matched": True, "unique_ledger_invocation_ids": True,
                    "usage_protocol_consistent": True,
                },
                "memory_authority": {
                    "route": "authority_only", "conflict_exposed": False,
                    "private": "do-not-aggregate-authority",
                },
            })
            result = summarize_directory(directory)
            group = result["groups"][0]
            self.assertEqual(result["files"]["accepted_files"], 1)
            self.assertEqual(group["passed"], 1)
            self.assertEqual(group["reported_usage_subtotal"], {"input_tokens": 30, "output_tokens": 5})
            self.assertEqual(group["ordinary_invocation_evidence"]["actual_adapter_calls"], 2)
            self.assertEqual(group["ordinary_invocation_evidence"]["verified_samples"], 1)
            self.assertNotIn("do-not-aggregate-authority", json.dumps(result))

    def test_groups_live_summary_and_memory_without_sidecar_costs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            secret = "do-not-persist-this-prompt"
            self.write_record(
                directory,
                "live.json",
                {
                    "schema": "harness.programmatic.live-evidence/v1",
                    "source_revision": "revision-a",
                    "requested_model": "model-a",
                    "case_id": "case-a",
                    "acceptance_passed": True,
                    "rounds": [
                        {"input_tokens": 3, "output_tokens": 5, "usage_reported": True},
                        {"input_tokens": 7, "output_tokens": 11, "usage_reported": False},
                    ],
                    "prompt": secret,
                },
            )
            self.write_record(
                directory,
                "summary.json",
                {
                    "schema": "harness.programmatic.live-evidence/v1",
                    "source_revision": "revision-a",
                    "requested_model": "model-a",
                    "case_id": "summary-case",
                    "acceptance_passed": False,
                    "rounds": [],
                    "summary": {
                        "total_requests": 2,
                        "total_usage": {"input_tokens": 13, "output_tokens": 17},
                        "usage_reported": False,
                    },
                },
            )
            self.write_record(
                directory,
                "memory.json",
                {
                    "schema": "harness.programmatic.live-memory-scope/v1",
                    "source_revision": "revision-b",
                    "requested_model": "model-b",
                    "case_id": "scope-case",
                    "acceptance_passed": True,
                    "reported_input_tokens": 19,
                    "reported_output_tokens": 23,
                    "usage_complete": True,
                    "model_rounds": 2,
                },
            )
            self.write_record(
                directory,
                "empty-rounds.json",
                {
                    "schema": "harness.programmatic.live-evidence/v1",
                    "source_revision": "revision-c",
                    "requested_model": "model-c",
                    "case_id": "empty-rounds",
                    "acceptance_passed": False,
                    "rounds": [],
                },
            )
            self.write_record(
                directory,
                "rollover-partial.json",
                {
                    "schema": "harness.programmatic.live-summary-rollover/v1",
                    "source_revision": "revision-d",
                    "requested_model": "model-d",
                    "case_id": "rollover-partial",
                    "acceptance_passed": False,
                    "usage_complete": False,
                    "usage_ledger_matches": True,
                    "model_calls": 3,
                    "run_usage": [
                        {"input_tokens": 2, "output_tokens": 3, "reports": 1, "complete": True},
                        {"input_tokens": 5, "output_tokens": 7, "reports": 1, "complete": False},
                        {"input_tokens": 11, "output_tokens": 13, "reports": 1, "complete": True},
                    ],
                },
            )
            self.write_record(
                directory,
                "rollover-missing.json",
                {
                    "schema": "harness.programmatic.live-summary-rollover/v1",
                    "source_revision": "revision-e",
                    "requested_model": "model-e",
                    "case_id": "rollover-missing",
                    "acceptance_passed": True,
                    "usage_complete": False,
                    "run_usage": [{"input_tokens": 17, "output_tokens": 19, "reports": 1, "complete": True}],
                },
            )
            self.write_record(
                directory,
                "rollover-reports-conflict.json",
                {
                    "schema": "harness.programmatic.live-summary-rollover/v1",
                    "source_revision": "revision-f",
                    "requested_model": "model-f",
                    "case_id": "rollover-reports-conflict",
                    "acceptance_passed": True,
                    "usage_complete": True,
                    "usage_ledger_matches": True,
                    "model_calls": 3,
                    "run_usage": [
                        {"input_tokens": 2, "output_tokens": 3, "reports": 2, "complete": True},
                        {"input_tokens": 5, "output_tokens": 7, "reports": 1, "complete": True},
                        {"input_tokens": 11, "output_tokens": 13, "reports": 1, "complete": True},
                    ],
                },
            )
            self.write_record(
                directory,
                "rollover-model-calls-conflict.json",
                {
                    "schema": "harness.programmatic.live-summary-rollover/v1",
                    "source_revision": "revision-g",
                    "requested_model": "model-g",
                    "case_id": "rollover-model-calls-conflict",
                    "acceptance_passed": True,
                    "usage_complete": True,
                    "usage_ledger_matches": True,
                    "model_calls": 2,
                    "run_usage": [
                        {"input_tokens": 2, "output_tokens": 3, "reports": 1, "complete": True},
                        {"input_tokens": 5, "output_tokens": 7, "reports": 1, "complete": True},
                        {"input_tokens": 11, "output_tokens": 13, "reports": 1, "complete": True},
                    ],
                },
            )
            self.write_record(
                directory,
                "rollover-ledger-conflict.json",
                {
                    "schema": "harness.programmatic.live-summary-rollover/v1",
                    "source_revision": "revision-h",
                    "requested_model": "model-h",
                    "case_id": "rollover-ledger-conflict",
                    "acceptance_passed": True,
                    "usage_complete": True,
                    "usage_ledger_matches": False,
                    "model_calls": 3,
                    "run_usage": [
                        {"input_tokens": 2, "output_tokens": 3, "reports": 1, "complete": True},
                        {"input_tokens": 5, "output_tokens": 7, "reports": 1, "complete": True},
                        {"input_tokens": 11, "output_tokens": 13, "reports": 1, "complete": True},
                    ],
                },
            )
            self.write_record(
                directory,
                "sqlite-recovery-partial.json",
                {
                    "schema": "harness.programmatic.live-sqlite-recovery/v1",
                    "source_revision": "revision-i",
                    "requested_model": "model-i",
                    "case_id": "sqlite-recovery-partial",
                    "acceptance_passed": False,
                    "usage_complete": False,
                    "usage_ledger_matches": True,
                    "model_calls": 2,
                    "run_usage": [
                        {"invocation_id": "model:first", "input_tokens": 17, "output_tokens": 19, "complete": True},
                        {"invocation_id": "model:second", "input_tokens": 23, "output_tokens": 29, "complete": False},
                    ],
                },
            )
            self.write_record(
                directory,
                "wire.json",
                {"schema": "harness.programmatic.live-wire-evidence/v1", "input_tokens": 1000},
            )
            self.write_record(directory, "unknown.json", {"schema": "future/live/v9"})
            self.write_record(directory, "schema-list.json", {"schema": []})
            self.write_record(directory, "schema-object.json", {"schema": {}})

            output = summarize_directory(directory)

            self.assertEqual(output["files"], {
                "accepted_files": 10,
                "sidecar_files_ignored": 1,
                "unknown_schema_files_skipped": 1,
                "invalid_json_files_skipped": 0,
                "invalid_record_files_skipped": 2,
            })
            groups = {(item["source_revision"], item["requested_model"], item["case_id"]): item for item in output["groups"]}
            live = groups[("revision-a", "model-a", "case-a")]
            self.assertEqual(live["reported_usage_subtotal"], {"input_tokens": 3, "output_tokens": 5})
            self.assertEqual(live["unknown_usage_samples"], 1)
            self.assertEqual(live["measurement_counts"], {"live_model_rounds": 2, "summary_experiment_adapter_invocations": 0, "memory_scope_model_rounds": 0, "memory_authority_lookup_model_rounds": 0, "rollover_adapter_invocations": 0, "unknown_rollover_adapter_invocation_samples": 0, "sqlite_recovery_adapter_invocations": 0, "unknown_sqlite_recovery_adapter_invocation_samples": 0})
            summary = groups[("revision-a", "model-a", "summary-case")]
            self.assertEqual(summary["reported_usage_subtotal"], {"input_tokens": 13, "output_tokens": 17})
            self.assertEqual(summary["unknown_usage_samples"], 1)
            self.assertEqual(summary["measurement_counts"], {"live_model_rounds": 0, "summary_experiment_adapter_invocations": 2, "memory_scope_model_rounds": 0, "memory_authority_lookup_model_rounds": 0, "rollover_adapter_invocations": 0, "unknown_rollover_adapter_invocation_samples": 0, "sqlite_recovery_adapter_invocations": 0, "unknown_sqlite_recovery_adapter_invocation_samples": 0})
            memory = groups[("revision-b", "model-b", "scope-case")]
            self.assertEqual(memory["measurement_counts"], {"live_model_rounds": 0, "summary_experiment_adapter_invocations": 0, "memory_scope_model_rounds": 2, "memory_authority_lookup_model_rounds": 0, "rollover_adapter_invocations": 0, "unknown_rollover_adapter_invocation_samples": 0, "sqlite_recovery_adapter_invocations": 0, "unknown_sqlite_recovery_adapter_invocation_samples": 0})
            empty = groups[("revision-c", "model-c", "empty-rounds")]
            self.assertEqual(empty["reported_usage_subtotal"], {"input_tokens": 0, "output_tokens": 0})
            self.assertEqual(empty["unknown_usage_samples"], 1)
            partial = groups[("revision-d", "model-d", "rollover-partial")]
            self.assertEqual(partial["reported_usage_subtotal"], {"input_tokens": 18, "output_tokens": 23})
            self.assertEqual(partial["unknown_usage_samples"], 1)
            self.assertEqual(partial["failed"], 1)
            self.assertEqual(partial["measurement_counts"]["rollover_adapter_invocations"], 3)
            missing = groups[("revision-e", "model-e", "rollover-missing")]
            self.assertEqual(missing["reported_usage_subtotal"], {"input_tokens": 17, "output_tokens": 19})
            self.assertEqual(missing["unknown_usage_samples"], 1)
            self.assertEqual(missing["measurement_counts"]["unknown_rollover_adapter_invocation_samples"], 1)
            for revision, case_id in (("revision-f", "rollover-reports-conflict"), ("revision-g", "rollover-model-calls-conflict"), ("revision-h", "rollover-ledger-conflict")):
                conflict = groups[(revision, "model-" + revision[-1], case_id)]
                self.assertEqual(conflict["reported_usage_subtotal"], {"input_tokens": 18, "output_tokens": 23})
                self.assertEqual(conflict["unknown_usage_samples"], 1)
            sqlite = groups[("revision-i", "model-i", "sqlite-recovery-partial")]
            self.assertEqual(sqlite["reported_usage_subtotal"], {"input_tokens": 40, "output_tokens": 48})
            self.assertEqual(sqlite["unknown_usage_samples"], 1)
            self.assertEqual(sqlite["failed"], 1)
            self.assertEqual(sqlite["measurement_counts"]["sqlite_recovery_adapter_invocations"], 2)
            self.assertNotIn(secret, json.dumps(output))

    def test_memory_authority_lookup_aggregates_safe_usage_and_rounds(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            base = {
                "schema": "harness.programmatic.live-memory-authority-lookup/v1",
                "source_revision": "lookup-revision",
                "requested_model": "lookup-model",
            }
            success_contract = {
                "runtime_status": "completed",
                "run_attempts": 1,
                "tool_calls": 2,
                "profile_only_pair": True,
                "lookup_key_exact": True,
                "lookup_found": True,
                "lookup_entry_structured": True,
                "authority_structured": True,
                "conflict_recognized": True,
                "context_exact_pair": True,
                "journal_exact_pair": True,
                "current_answer": True,
                "current_durable_answer": True,
                "peer_scope_isolated": True,
                "foreign_absent": True,
            }
            failed_contract = {
                **success_contract,
                "runtime_status": "failed",
                "run_attempts": 2,
                "tool_calls": 1,
                "profile_only_pair": False,
                "lookup_found": False,
            }
            self.write_record(directory, "passed.json", {
                **base, **success_contract, "case_id": "passed", "acceptance_passed": True,
                "usage_input_tokens": 11, "usage_output_tokens": 13,
                "usage_complete": True, "model_rounds": 2,
                "entity": "FORBIDDEN-ENTITY-VALUE",
                "model_text": "FORBIDDEN-MODEL-TEXT",
            })
            self.write_record(directory, "failed-unknown.json", {
                **base, **failed_contract, "case_id": "failed-unknown", "acceptance_passed": False,
                "usage_input_tokens": 17, "usage_output_tokens": 19,
                "usage_complete": False, "model_rounds": 3,
            })
            missing_predicate = {
                **base, **success_contract, "case_id": "missing", "acceptance_passed": True,
                "usage_input_tokens": 2, "usage_output_tokens": 3,
                "usage_complete": True, "model_rounds": 2,
            }
            del missing_predicate["foreign_absent"]
            self.write_record(directory, "missing-predicate.json", missing_predicate)
            for name, record in {
                "negative-usage": {**base, **failed_contract, "case_id": "negative", "acceptance_passed": False, "usage_input_tokens": -1, "usage_output_tokens": 2, "usage_complete": True, "model_rounds": 1},
                "malformed-rounds": {**base, **failed_contract, "case_id": "malformed", "acceptance_passed": False, "usage_input_tokens": 2, "usage_output_tokens": 3, "usage_complete": True, "model_rounds": True},
                "malformed-complete": {**base, **failed_contract, "case_id": "complete", "acceptance_passed": False, "usage_input_tokens": 2, "usage_output_tokens": 3, "usage_complete": "yes", "model_rounds": 1},
                "false-success": {**base, **success_contract, "case_id": "false-success", "acceptance_passed": True, "usage_input_tokens": 2, "usage_output_tokens": 3, "usage_complete": True, "model_rounds": 2, "lookup_found": False},
                "wrong-budget": {**base, **success_contract, "case_id": "wrong-budget", "acceptance_passed": True, "usage_input_tokens": 2, "usage_output_tokens": 3, "usage_complete": True, "model_rounds": 3},
                "wrong-status": {**base, **success_contract, "case_id": "wrong-status", "acceptance_passed": True, "usage_input_tokens": 2, "usage_output_tokens": 3, "usage_complete": True, "model_rounds": 2, "runtime_status": "failed"},
            }.items():
                self.write_record(directory, name + ".json", record)
            self.write_record(directory, "future.json", {"schema": "future/live/v9"})

            output = summarize_directory(directory)
            self.assertEqual(output["files"], {
                "accepted_files": 2,
                "sidecar_files_ignored": 0,
                "unknown_schema_files_skipped": 1,
                "invalid_json_files_skipped": 0,
                "invalid_record_files_skipped": 7,
            })
            groups = {item["case_id"]: item for item in output["groups"]}
            passed = groups["passed"]
            self.assertEqual(passed["passed"], 1)
            self.assertEqual(passed["reported_usage_subtotal"], {"input_tokens": 11, "output_tokens": 13})
            self.assertEqual(passed["unknown_usage_samples"], 0)
            self.assertEqual(passed["measurement_counts"]["memory_authority_lookup_model_rounds"], 2)
            failed = groups["failed-unknown"]
            self.assertEqual(failed["failed"], 1)
            self.assertEqual(failed["reported_usage_subtotal"], {"input_tokens": 17, "output_tokens": 19})
            self.assertEqual(failed["unknown_usage_samples"], 1)
            self.assertEqual(failed["measurement_counts"]["memory_authority_lookup_model_rounds"], 3)
            rendered = json.dumps(output)
            self.assertNotIn("FORBIDDEN-ENTITY-VALUE", rendered)
            self.assertNotIn("FORBIDDEN-MODEL-TEXT", rendered)

    def test_rejects_two_paths_for_one_physical_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            original = self.write_record(
                directory,
                "record.json",
                {
                    "schema": "harness.programmatic.live-evidence/v1",
                    "source_revision": "revision",
                    "requested_model": "model",
                    "case_id": "case",
                    "acceptance_passed": True,
                    "rounds": [],
                },
            )
            duplicate = directory / "duplicate.json"
            try:
                os.link(original, duplicate)
            except OSError as error:
                self.skipTest(f"hard links unavailable: {error}")
            with self.assertRaises(DuplicatePhysicalFileError):
                summarize_directory(directory)


if __name__ == "__main__":
    unittest.main()
