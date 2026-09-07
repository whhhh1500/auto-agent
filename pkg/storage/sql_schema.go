package storage

// SQLSchemaVersion is the on-disk schema this store writes. Stores with a
// HIGHER recorded version are refused (this code is too old to read them).
// Lower versions are upgraded in place. Most migrations add objects; semantic
// migrations may atomically rebuild a derived projection before advancing the
// recorded version.
const SQLSchemaVersion = 47

const sqlSchemaV1 = `
CREATE TABLE IF NOT EXISTS store_meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
	id          TEXT PRIMARY KEY,
	version     BIGINT NOT NULL,
	tenant_id   TEXT NOT NULL,
	user_id     TEXT NOT NULL,
	profile_id  TEXT NOT NULL,
	status      TEXT NOT NULL DEFAULT 'active',
	event_count BIGINT NOT NULL,
	header      TEXT NOT NULL,
	updated_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_listing
	ON sessions (tenant_id, user_id, updated_at);
CREATE TABLE IF NOT EXISTS event_chunks (
	session_id TEXT NOT NULL,
	start_seq  BIGINT NOT NULL,
	payload    TEXT NOT NULL,
	PRIMARY KEY (session_id, start_seq)
);
`

// sqlSchemaV2 adds the v2 subsystem tables: distributed leases, the release
// journal, and durable memory / retrieval backing stores. Every statement is
// additive, so running it over a v1 database performs the upgrade.
const sqlSchemaV2 = sqlSchemaV1 + `
CREATE TABLE IF NOT EXISTS session_leases (
	session_id TEXT PRIMARY KEY,
	holder     TEXT NOT NULL,
	expires_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS profile_releases (
	profile_id  TEXT NOT NULL,
	version     INTEGER NOT NULL,
	scope       TEXT NOT NULL,
	created_at  BIGINT NOT NULL,
	rolled_back INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (profile_id, version)
);
CREATE TABLE IF NOT EXISTS memory_entries (
	scope      TEXT NOT NULL,
	id         TEXT NOT NULL,
	key        TEXT NOT NULL,
	content    TEXT NOT NULL,
	tags       TEXT NOT NULL DEFAULT '',
	created_at BIGINT NOT NULL,
	PRIMARY KEY (scope, id)
);
CREATE INDEX IF NOT EXISTS memory_key ON memory_entries (scope, key);
CREATE TABLE IF NOT EXISTS rag_documents (
	scope   TEXT NOT NULL,
	id      TEXT NOT NULL,
	source  TEXT NOT NULL DEFAULT '',
	content TEXT NOT NULL,
	tags    TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (scope, id)
);
`

// sqlSchemaV3 adds the control-plane tables: accounts and login tokens for
// the built-in identity store, tenants, and deployment settings.
const sqlSchemaV3 = sqlSchemaV2 + `
CREATE TABLE IF NOT EXISTS accounts (
	account_id           TEXT PRIMARY KEY,
	email                TEXT,
	password_hash        TEXT NOT NULL,
	role                 TEXT NOT NULL,
	tenant_id            TEXT NOT NULL,
	status               TEXT NOT NULL DEFAULT 'active',
	must_change_password BIGINT NOT NULL DEFAULT 0,
	created_at           BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_tokens (
	token_hash  TEXT PRIMARY KEY,
	account_id TEXT NOT NULL,
	expires_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS tenants (
	id         TEXT PRIMARY KEY,
	name       TEXT NOT NULL,
	created_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS settings (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`

// sqlSchemaV4 adds the audit trail for administrative operations.
const sqlSchemaV4 = sqlSchemaV3 + `
CREATE TABLE IF NOT EXISTS audit_events (
	id        TEXT PRIMARY KEY,
	time      BIGINT NOT NULL,
	actor     TEXT NOT NULL DEFAULT '',
	role      TEXT NOT NULL DEFAULT '',
	tenant    TEXT NOT NULL DEFAULT '',
	action    TEXT NOT NULL,
	target    TEXT NOT NULL DEFAULT '',
	detail    TEXT NOT NULL DEFAULT '',
	remote_ip TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS audit_time ON audit_events (time);
CREATE INDEX IF NOT EXISTS audit_actor ON audit_events (actor, time);
`

// sqlSchemaV5 adds observability watch rules and their recorded hits.
const sqlSchemaV5 = sqlSchemaV4 + `
CREATE TABLE IF NOT EXISTS obs_rules (
	id         TEXT PRIMARY KEY,
	name       TEXT NOT NULL,
	kind       TEXT NOT NULL,
	pattern    TEXT NOT NULL,
	enabled    INTEGER NOT NULL DEFAULT 1,
	created_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS obs_hits (
	id         TEXT PRIMARY KEY,
	time       BIGINT NOT NULL,
	rule_id    TEXT NOT NULL,
	rule_name  TEXT NOT NULL DEFAULT '',
	kind       TEXT NOT NULL,
	pattern    TEXT NOT NULL DEFAULT '',
	session_id TEXT NOT NULL,
	run_id     TEXT NOT NULL DEFAULT '',
	actor      TEXT NOT NULL DEFAULT '',
	tenant     TEXT NOT NULL DEFAULT '',
	snippet    TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS obs_hits_rule ON obs_hits (rule_id, time);
CREATE INDEX IF NOT EXISTS obs_hits_session ON obs_hits (session_id);
`

// sqlSchemaV6 adds the multi-instance binding journal and run metrics.
const sqlSchemaV6 = sqlSchemaV5 + `
CREATE TABLE IF NOT EXISTS admin_bindings (
	id         TEXT PRIMARY KEY,
	kind       TEXT NOT NULL,
	summary    TEXT NOT NULL DEFAULT '',
	payload    TEXT NOT NULL DEFAULT '',
	created_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS run_stats (
	run_id        TEXT PRIMARY KEY,
	session_id    TEXT NOT NULL,
	tenant        TEXT NOT NULL DEFAULT '',
	status        TEXT NOT NULL DEFAULT '',
	input_tokens  BIGINT NOT NULL DEFAULT 0,
	output_tokens BIGINT NOT NULL DEFAULT 0,
	duration_ms   BIGINT NOT NULL DEFAULT 0,
	created_at    BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS run_stats_tenant ON run_stats (tenant, created_at);
`

// sqlSchemaV7 adds durable run lifecycle and cross-instance cancellation.
const sqlSchemaV7 = sqlSchemaV6 + `
CREATE TABLE IF NOT EXISTS run_control (
	run_id           TEXT PRIMARY KEY,
	session_id       TEXT NOT NULL,
	tenant_id        TEXT NOT NULL,
	subject_id       TEXT NOT NULL,
	status           TEXT NOT NULL,
	cancel_requested INTEGER NOT NULL DEFAULT 0,
	error_code       TEXT NOT NULL DEFAULT '',
	created_at       BIGINT NOT NULL,
	updated_at       BIGINT NOT NULL,
	completed_at     BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS run_control_session
	ON run_control (session_id, created_at);
CREATE INDEX IF NOT EXISTS run_control_active
	ON run_control (session_id, status, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS run_control_one_active
	ON run_control (session_id) WHERE status = 'running';
`

// sqlSchemaV8 adds a durable queue for asynchronous run workers.
const sqlSchemaV8 = sqlSchemaV7 + `
CREATE TABLE IF NOT EXISTS run_queue (
	run_id           TEXT PRIMARY KEY,
	message          TEXT NOT NULL,
	available_at     BIGINT NOT NULL,
	worker_id        TEXT NOT NULL DEFAULT '',
	lease_expires_at BIGINT NOT NULL DEFAULT 0,
	attempt          INTEGER NOT NULL DEFAULT 0,
	max_attempts     INTEGER NOT NULL DEFAULT 3
);
CREATE INDEX IF NOT EXISTS run_queue_available
	ON run_queue (available_at, run_id);
CREATE INDEX IF NOT EXISTS run_queue_lease
	ON run_queue (lease_expires_at, worker_id);
`

// sqlSchemaV9 adds a durable side-effect journal for guarded tool calls.
const sqlSchemaV9 = sqlSchemaV8 + `
CREATE TABLE IF NOT EXISTS tool_invocations (
	tenant_id     TEXT NOT NULL,
	subject_id    TEXT NOT NULL,
	session_id    TEXT NOT NULL,
	run_id        TEXT NOT NULL,
	call_id       TEXT NOT NULL,
	capability_id TEXT NOT NULL,
	args_digest   TEXT NOT NULL,
	idempotent    INTEGER NOT NULL DEFAULT 0,
	state         TEXT NOT NULL,
	result_json   TEXT NOT NULL DEFAULT '',
	error_code    TEXT NOT NULL DEFAULT '',
	started_at    BIGINT NOT NULL,
	updated_at    BIGINT NOT NULL,
	completed_at  BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (session_id, run_id, call_id)
);
CREATE INDEX IF NOT EXISTS tool_invocations_tenant
	ON tool_invocations (tenant_id, updated_at);
CREATE INDEX IF NOT EXISTS tool_invocations_state
	ON tool_invocations (state, updated_at);
`

// sqlSchemaV10 adds durable human approvals. The run_queue generation column
// is added by migrateSQLSchema because ALTER TABLE must run for existing v9
// databases as well as fresh databases assembled from the prior DDL chain.
const sqlSchemaV10 = sqlSchemaV9 + `
CREATE TABLE IF NOT EXISTS approval_requests (
	id             TEXT PRIMARY KEY,
	tenant_id      TEXT NOT NULL,
	subject_id     TEXT NOT NULL,
	session_id     TEXT NOT NULL,
	run_id         TEXT NOT NULL,
	call_id        TEXT NOT NULL,
	capability_id  TEXT NOT NULL,
	args_digest    TEXT NOT NULL,
	manifest_digest TEXT NOT NULL,
	request_json   TEXT NOT NULL,
	status         TEXT NOT NULL,
	requested_at   BIGINT NOT NULL,
	expires_at     BIGINT NOT NULL,
	decided_at     BIGINT NOT NULL DEFAULT 0,
	decided_by     TEXT NOT NULL DEFAULT '',
	UNIQUE (session_id, run_id, call_id)
);
CREATE INDEX IF NOT EXISTS approval_requests_tenant
	ON approval_requests (tenant_id, status, requested_at);
CREATE INDEX IF NOT EXISTS approval_requests_expiry
	ON approval_requests (status, expires_at);
DROP INDEX IF EXISTS run_control_one_active;
CREATE UNIQUE INDEX IF NOT EXISTS run_control_one_active
	ON run_control (session_id) WHERE status IN ('running', 'waiting_approval');
`

// sqlSchemaV11 adds client-submission idempotency for durable asynchronous
// runs. Only the SHA-256 of the opaque client key is stored.
const sqlSchemaV11 = sqlSchemaV10 + `
CREATE TABLE IF NOT EXISTS run_submissions (
	tenant_id       TEXT NOT NULL,
	subject_id      TEXT NOT NULL,
	session_id      TEXT NOT NULL,
	key_hash        TEXT NOT NULL,
	request_digest  TEXT NOT NULL,
	run_id          TEXT NOT NULL UNIQUE,
	created_at      BIGINT NOT NULL,
	PRIMARY KEY (tenant_id, subject_id, session_id, key_hash)
);
CREATE INDEX IF NOT EXISTS run_submissions_created
	ON run_submissions (created_at);
`

// sqlSchemaV12 adds the minimal durable W3C trace carrier to run_queue. The
// columns are applied by migrateSQLSchema so historical DDL remains accurate.
const sqlSchemaV12 = sqlSchemaV11

// sqlSchemaV13 adds durable evaluation datasets, run summaries, and
// incrementally committed case results.
const sqlSchemaV13 = sqlSchemaV12 + `
CREATE TABLE IF NOT EXISTS evaluation_datasets (
	id              TEXT NOT NULL,
	version         INTEGER NOT NULL,
	revision        TEXT NOT NULL,
	name            TEXT NOT NULL,
	profile_id      TEXT NOT NULL,
	case_count      INTEGER NOT NULL,
	definition_json TEXT NOT NULL,
	created_at      BIGINT NOT NULL,
	PRIMARY KEY (id, version)
);
CREATE INDEX IF NOT EXISTS evaluation_datasets_created
	ON evaluation_datasets (created_at);
CREATE TABLE IF NOT EXISTS evaluation_runs (
	id                 TEXT PRIMARY KEY,
	dataset_id         TEXT NOT NULL,
	dataset_version    INTEGER NOT NULL,
	dataset_revision   TEXT NOT NULL,
	tenant_id          TEXT NOT NULL,
	subject_id         TEXT NOT NULL,
	profile_id         TEXT NOT NULL,
	baseline_run_id    TEXT NOT NULL DEFAULT '',
	status             TEXT NOT NULL,
	score              DOUBLE PRECISION NOT NULL DEFAULT 0,
	passed             INTEGER NOT NULL DEFAULT 0,
	total_cases        INTEGER NOT NULL,
	passed_cases       INTEGER NOT NULL DEFAULT 0,
	allow_capabilities TEXT NOT NULL DEFAULT '[]',
	metadata_json      TEXT NOT NULL DEFAULT '{}',
	error_message      TEXT NOT NULL DEFAULT '',
	created_at         BIGINT NOT NULL,
	completed_at       BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS evaluation_runs_dataset
	ON evaluation_runs (dataset_id, dataset_version, created_at);
CREATE INDEX IF NOT EXISTS evaluation_runs_tenant
	ON evaluation_runs (tenant_id, created_at);
CREATE TABLE IF NOT EXISTS evaluation_case_results (
	run_id      TEXT NOT NULL,
	case_id     TEXT NOT NULL,
	result_json TEXT NOT NULL,
	score       DOUBLE PRECISION NOT NULL,
	passed      INTEGER NOT NULL DEFAULT 0,
	completed_at BIGINT NOT NULL,
	PRIMARY KEY (run_id, case_id)
);
CREATE INDEX IF NOT EXISTS evaluation_case_results_run
	ON evaluation_case_results (run_id, completed_at);
`

// sqlSchemaV14 makes profile releases restorable across process lifetimes by
// persisting the complete layer artifact and its scoped revision.
const sqlSchemaV14 = sqlSchemaV13

// sqlSchemaV15 adds durable staged canary rollout state and gate evidence.
const sqlSchemaV15 = sqlSchemaV14 + `
CREATE TABLE IF NOT EXISTS profile_canaries (
	id                         TEXT PRIMARY KEY,
	profile_id                 TEXT NOT NULL,
	scope                      TEXT NOT NULL,
	layer_json                 TEXT NOT NULL,
	revision                   TEXT NOT NULL,
	status                     TEXT NOT NULL,
	basis_points               INTEGER NOT NULL,
	baseline_evaluation_run_id TEXT NOT NULL DEFAULT '',
	candidate_evaluation_run_id TEXT NOT NULL,
	gate_json                  TEXT NOT NULL,
	created_at                 BIGINT NOT NULL,
	updated_at                 BIGINT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS profile_canaries_one_active
	ON profile_canaries (profile_id, scope) WHERE status IN ('active', 'paused');
CREATE INDEX IF NOT EXISTS profile_canaries_history
	ON profile_canaries (profile_id, created_at);
`

// sqlSchemaV16 makes canary promotion recoverable and idempotent. A canary id
// is used as the release operation id, and promoting counts as an open rollout
// for the profile/scope uniqueness invariant.
const sqlSchemaV16 = sqlSchemaV15 + `
CREATE UNIQUE INDEX IF NOT EXISTS profile_releases_operation
	ON profile_releases (operation_id) WHERE operation_id <> '';
CREATE UNIQUE INDEX IF NOT EXISTS profile_canaries_one_open
	ON profile_canaries (profile_id, scope) WHERE status IN ('active', 'paused', 'promoting');
`

// sqlSchemaV17 persists the Release baseline against which a Canary was
// evaluated and staged. The migration adds the column before current stores
// read or write Canary records.
const sqlSchemaV17 = sqlSchemaV16

// sqlSchemaV18 adds one global, monotonically increasing control-plane
// revision. Release and Canary mutations bump it in the same transaction as
// their durable state, allowing request paths to avoid full history scans.
const sqlSchemaV18 = sqlSchemaV17 + `
INSERT INTO store_meta (key, value) VALUES ('control_revision', '0')
	ON CONFLICT (key) DO NOTHING;
`

// sqlSchemaV19 adds durable Evaluation-level Composition metadata so an
// interrupted evaluation can recreate the same Case Composition on resume.
const sqlSchemaV19 = sqlSchemaV18

// sqlSchemaV20 adds indexed Evaluation artifact revisions used by the
// revision-filtered admin query surface.
const sqlSchemaV20 = sqlSchemaV19 + `
CREATE INDEX IF NOT EXISTS evaluation_case_composition_revision
	ON evaluation_case_results (composition_revision, run_id);
CREATE INDEX IF NOT EXISTS evaluation_case_assignment_revision
	ON evaluation_case_results (assignment_revision, run_id);
`

// sqlSchemaV21 adds the append-only projection for ordinary and Backtest Run
// Composition segments. The event chunks remain canonical; this table is an
// indexed query sidecar rebuilt from them during migration.
const sqlSchemaV21 = sqlSchemaV20 + `
CREATE TABLE IF NOT EXISTS run_evidence (
	session_id           TEXT NOT NULL,
	run_id               TEXT NOT NULL,
	segment_seq          BIGINT NOT NULL,
	kind                 TEXT NOT NULL,
	tenant_id            TEXT NOT NULL,
	subject_id           TEXT NOT NULL,
	profile_id           TEXT NOT NULL,
	composition_revision TEXT NOT NULL DEFAULT '',
	assignment_revision  TEXT NOT NULL DEFAULT '',
	assignment_variant   TEXT NOT NULL DEFAULT 'unassigned',
	status               TEXT NOT NULL DEFAULT '',
	created_at           BIGINT NOT NULL,
	PRIMARY KEY (session_id, run_id, segment_seq)
);
CREATE INDEX IF NOT EXISTS run_evidence_composition
	ON run_evidence (composition_revision, tenant_id, created_at);
CREATE INDEX IF NOT EXISTS run_evidence_assignment
	ON run_evidence (assignment_revision, tenant_id, created_at);
CREATE INDEX IF NOT EXISTS run_evidence_owner
	ON run_evidence (tenant_id, subject_id, created_at);
`

// sqlSchemaV22 adds Run/Backtest status filtering to the Evidence sidecar.
const sqlSchemaV22 = sqlSchemaV21 + `
CREATE INDEX IF NOT EXISTS run_evidence_status
	ON run_evidence (status, tenant_id, created_at);
`

// sqlSchemaV23 adds low-cardinality Assignment variant aggregation.
const sqlSchemaV23 = sqlSchemaV22 + `
CREATE INDEX IF NOT EXISTS run_evidence_variant_status
	ON run_evidence (assignment_variant, status, tenant_id, created_at);
`

// sqlSchemaV24 adds the durable private-runner task queue. Arguments and
// outcomes are canonical JSON payloads; the indexed columns are the queue and
// fencing projections needed by workers and recovery.
const sqlSchemaV24 = sqlSchemaV23 + `
CREATE TABLE IF NOT EXISTS runner_tasks (
	id                  TEXT PRIMARY KEY,
	tenant_id           TEXT NOT NULL DEFAULT '',
	subject_id          TEXT NOT NULL DEFAULT '',
	scope               TEXT NOT NULL DEFAULT '',
	capability          TEXT NOT NULL,
	idempotency_key     TEXT NOT NULL DEFAULT '',
	args_digest         TEXT NOT NULL,
	args_json           TEXT NOT NULL,
	state               TEXT NOT NULL,
	cancel_requested    INTEGER NOT NULL DEFAULT 0,
	worker_id           TEXT NOT NULL DEFAULT '',
	lease_expires_at    BIGINT NOT NULL DEFAULT 0,
	available_at        BIGINT NOT NULL,
	attempt             INTEGER NOT NULL DEFAULT 0,
	max_attempts        INTEGER NOT NULL,
	generation          BIGINT NOT NULL DEFAULT 0,
	result_json         TEXT NOT NULL DEFAULT '',
	error_code          TEXT NOT NULL DEFAULT '',
	created_at          BIGINT NOT NULL,
	updated_at          BIGINT NOT NULL,
	completed_at        BIGINT NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS runner_tasks_idempotency
	ON runner_tasks (tenant_id, subject_id, scope, capability, idempotency_key)
	WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS runner_tasks_state_available
	ON runner_tasks (state, available_at, id);
CREATE INDEX IF NOT EXISTS runner_tasks_state_lease
	ON runner_tasks (state, lease_expires_at, id);
`

// sqlSchemaV25 adds the durable W3C trace carrier to runner tasks. Existing
// runner_tasks tables are upgraded by migrateSQLSchema because SQLite does
// not support an additive ALTER TABLE clause with IF NOT EXISTS.
const sqlSchemaV25 = sqlSchemaV24

// sqlSchemaV26 adds canonical JSON tag projections for the legacy
// comma-separated Memory/RAG tags and makes Memory keys unique per scope.
// Existing tables receive tags_json in migrateMemoryRagV26 because SQLite
// cannot add a column with IF NOT EXISTS.
const sqlSchemaV26 = sqlSchemaV25 + `
CREATE UNIQUE INDEX IF NOT EXISTS memory_scope_key ON memory_entries (scope, key);
`

// sqlSchemaV27 adds rebuildable inverted projections for RAG document tokens
// and tags. rag_documents remains canonical: the projection tables deliberately
// have no foreign keys so they can be cleared and rebuilt in one migration.
const sqlSchemaV27 = sqlSchemaV26 + `
CREATE TABLE IF NOT EXISTS rag_document_tokens (
	scope       TEXT NOT NULL,
	document_id TEXT NOT NULL,
	token       TEXT NOT NULL,
	PRIMARY KEY (scope, document_id, token)
);
CREATE INDEX IF NOT EXISTS rag_tokens_lookup
	ON rag_document_tokens (token, scope, document_id);
CREATE TABLE IF NOT EXISTS rag_document_tags (
	scope       TEXT NOT NULL,
	document_id TEXT NOT NULL,
	tag         TEXT NOT NULL,
	PRIMARY KEY (scope, document_id, tag)
);
CREATE INDEX IF NOT EXISTS rag_tags_lookup
	ON rag_document_tags (tag, scope, document_id);
`

// sqlSchemaV28 adds derived lower-cased Memory search text and exact-tag
// lookup rows. memory_entries.key, memory_entries.content, and tags_json stay
// canonical; the projection deliberately has no foreign key so it can be
// cleared and rebuilt transactionally.
const sqlSchemaV28 = sqlSchemaV27 + `
CREATE TABLE IF NOT EXISTS memory_entry_tags (
	scope TEXT NOT NULL,
	key   TEXT NOT NULL,
	tag   TEXT NOT NULL,
	PRIMARY KEY (scope, key, tag)
);
CREATE INDEX IF NOT EXISTS memory_tags_lookup
	ON memory_entry_tags (tag, scope, key);
`

// sqlSchemaV29 adds the bounded disclosure-flow log for tool-library search,
// explore, and choose traces used to improve ranking later.
const sqlSchemaV29 = sqlSchemaV28 + `
CREATE TABLE IF NOT EXISTS tool_library_observations (
	id        TEXT PRIMARY KEY,
	time      INTEGER NOT NULL,
	action    TEXT NOT NULL,
	query     TEXT NOT NULL DEFAULT '',
	tool_id   TEXT NOT NULL DEFAULT '',
	library   TEXT NOT NULL DEFAULT '',
	name      TEXT NOT NULL DEFAULT '',
	hit_ids   TEXT NOT NULL DEFAULT '[]',
	hits      INTEGER NOT NULL DEFAULT 0,
	explore   INTEGER NOT NULL DEFAULT 0,
	call_id   TEXT NOT NULL DEFAULT '',
	scope     TEXT NOT NULL DEFAULT '',
	tenant_id TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS tool_library_observations_time
	ON tool_library_observations (time, id);
CREATE INDEX IF NOT EXISTS tool_library_observations_action
	ON tool_library_observations (action, time);
`

const sqlSchemaV30 = sqlSchemaV29

// sqlSchemaV31 persists metadata-only parent/child delegation links. The
// composite parent key makes capability replay idempotent while the unique
// child key prevents one child session from being attached to two parents.
const sqlDelegationLinksV31 = `
CREATE TABLE IF NOT EXISTS delegation_links (
	parent_session_id TEXT NOT NULL,
	parent_run_id     TEXT NOT NULL,
	parent_call_id    TEXT NOT NULL,
	child_session_id  TEXT NOT NULL UNIQUE,
	child_run_id      TEXT NOT NULL,
	tenant_id         TEXT NOT NULL,
	subject_id        TEXT NOT NULL,
	depth             INTEGER NOT NULL,
	created_at        BIGINT NOT NULL,
	updated_at        BIGINT NOT NULL,
	PRIMARY KEY (parent_session_id, parent_run_id, parent_call_id),
	CHECK (depth > 0 AND depth <= 64),
	CHECK (length(parent_session_id) BETWEEN 1 AND 128 AND length(parent_run_id) BETWEEN 1 AND 128 AND
		length(parent_call_id) BETWEEN 1 AND 128 AND length(child_session_id) BETWEEN 1 AND 128 AND
		length(child_run_id) BETWEEN 1 AND 128 AND length(tenant_id) BETWEEN 1 AND 128 AND
		length(subject_id) BETWEEN 1 AND 128)
);
CREATE INDEX IF NOT EXISTS delegation_links_parent_run
	ON delegation_links (parent_session_id, parent_run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS delegation_links_tenant_created
	ON delegation_links (tenant_id, created_at DESC);
`

const sqlSchemaV31 = sqlSchemaV30 + sqlDelegationLinksV31

// sqlSchemaV32 separates the durable account identity from optional email
// metadata. Existing v31 tables are rebuilt by migrateAccountsV32.
const sqlSchemaV32 = sqlSchemaV31 + `
CREATE INDEX IF NOT EXISTS auth_tokens_account_id ON auth_tokens (account_id);
`

// sqlSchemaV33 adds the durable runtime module-effect journal. Payloads are
// stored as base64 text by the SQL adapter so the same DDL works on SQLite and
// PostgreSQL; the runtime adapter owns any required payload sealing policy.
const sqlSchemaV33 = sqlSchemaV32 + `
CREATE TABLE IF NOT EXISTS runtime_effect_sequences (
	composition_revision TEXT PRIMARY KEY,
	next_ordinal        BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_effects (
	effect_id                 TEXT PRIMARY KEY,
	composition_revision      TEXT NOT NULL,
	module_id                 TEXT NOT NULL,
	module_revision_major     INTEGER NOT NULL,
	module_revision_minor     INTEGER NOT NULL,
	module_revision_patch     INTEGER NOT NULL,
	phase                     TEXT NOT NULL,
	forward_kind              TEXT NOT NULL,
	forward_target            TEXT NOT NULL,
	forward_payload           TEXT NOT NULL,
	inverse_kind              TEXT NOT NULL,
	inverse_target            TEXT NOT NULL,
	inverse_payload           TEXT NOT NULL,
	state                     TEXT NOT NULL,
	ordinal                   BIGINT NOT NULL,
	created_at                BIGINT NOT NULL,
	updated_at                BIGINT NOT NULL,
	UNIQUE (composition_revision, ordinal),
	CHECK (phase IN ('stage', 'activate', 'reconcile')),
	CHECK (state IN ('prepared', 'applied', 'unknown', 'reverted'))
);
CREATE INDEX IF NOT EXISTS runtime_effects_composition_order
	ON runtime_effects (composition_revision, ordinal);
`

// sqlSchemaV34 adds the single CAS document for durable runtime host desired
// state. The document is runtime composition metadata only; it never stores
// credentials. Revision is intentionally duplicated outside state_json so the
// SQL adapter can compare-and-swap without a read-before-write window.
const sqlSchemaV34 = sqlSchemaV33 + `
CREATE TABLE IF NOT EXISTS runtime_host_state (
	host_key   TEXT PRIMARY KEY NOT NULL CHECK (host_key = 'module_host'),
	revision   TEXT NOT NULL CHECK (length(revision) BETWEEN 1 AND 128),
	state_json TEXT NOT NULL CHECK (length(state_json) BETWEEN 1 AND 16777216),
	updated_at BIGINT NOT NULL
);
`

// sqlSchemaV35 adds the generation-fenced runtime host ownership lease. The
// holder is opaque and cleared on release while generation remains monotonic.
const sqlSchemaV35 = sqlSchemaV34 + `
CREATE TABLE IF NOT EXISTS runtime_host_ownership (
	host_key   TEXT PRIMARY KEY NOT NULL CHECK (host_key = 'module_host'),
	holder     TEXT NOT NULL DEFAULT '' CHECK (length(holder) <= 128),
	generation BIGINT NOT NULL CHECK (generation >= 0),
	expires_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_fence_journal (
	request_id            TEXT PRIMARY KEY,
	composition_revision  TEXT NOT NULL CHECK (length(composition_revision) BETWEEN 1 AND 128),
	actor_id              TEXT NOT NULL CHECK (length(actor_id) BETWEEN 1 AND 128),
	reason                TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 512),
	decision              TEXT NOT NULL CHECK (decision IN ('execute', 'replay', 'unknown')),
	completed             BIGINT NOT NULL DEFAULT 0 CHECK (completed IN (0, 1)),
	remaining_leases      BIGINT NOT NULL DEFAULT 0 CHECK (remaining_leases >= 0),
	created_at            BIGINT NOT NULL CHECK (created_at > 0),
	updated_at            BIGINT NOT NULL CHECK (updated_at > 0),
	CHECK (length(request_id) BETWEEN 1 AND 128)
);
`

// sqlSchemaV36 adds the one durable resource-object migration slot and its
// metadata-only mutation journal. Credentials and object bodies never enter
// either table. The resource_key primary key makes concurrent migrations for
// the same resource domain impossible at the database boundary.
const sqlSchemaV36 = sqlSchemaV35 + `
CREATE TABLE IF NOT EXISTS artifact_migrations (
	resource_key            TEXT PRIMARY KEY NOT NULL CHECK (resource_key = 'resources'),
	migration_id            TEXT NOT NULL UNIQUE CHECK (length(migration_id) BETWEEN 1 AND 128),
	desired_revision        TEXT NOT NULL CHECK (length(desired_revision) BETWEEN 1 AND 128),
	source_backend          TEXT NOT NULL CHECK (source_backend IN ('local', 's3')),
	source_identity         TEXT NOT NULL CHECK (length(source_identity) BETWEEN 1 AND 256),
	target_backend          TEXT NOT NULL CHECK (target_backend IN ('local', 's3')),
	target_identity         TEXT NOT NULL CHECK (length(target_identity) BETWEEN 1 AND 256),
	state                   TEXT NOT NULL CHECK (state IN ('local_active', 'syncing', 'catching_up', 'verifying', 's3_active', 'apply_failed', 'cancelled')),
	generation              BIGINT NOT NULL CHECK (generation > 0),
	store_revision          BIGINT NOT NULL CHECK (store_revision > 0),
	listing_cursor          TEXT NOT NULL DEFAULT '' CHECK (length(listing_cursor) <= 4096),
	mutation_high_water     BIGINT NOT NULL DEFAULT 0 CHECK (mutation_high_water >= 0),
	copied_objects          BIGINT NOT NULL DEFAULT 0 CHECK (copied_objects >= 0),
	copied_bytes            BIGINT NOT NULL DEFAULT 0 CHECK (copied_bytes >= 0),
	verified_objects        BIGINT NOT NULL DEFAULT 0 CHECK (verified_objects >= 0),
	verified_bytes          BIGINT NOT NULL DEFAULT 0 CHECK (verified_bytes >= 0),
	error_code              TEXT NOT NULL DEFAULT '' CHECK (length(error_code) <= 128),
	error_detail            TEXT NOT NULL DEFAULT '' CHECK (length(error_detail) <= 1024),
	lease_owner             TEXT NOT NULL DEFAULT '' CHECK (length(lease_owner) <= 128),
	lease_expires_at        BIGINT NOT NULL DEFAULT 0,
	created_at              BIGINT NOT NULL CHECK (created_at > 0),
	updated_at              BIGINT NOT NULL CHECK (updated_at > 0),
	verified_at             BIGINT NOT NULL DEFAULT 0,
	activated_at            BIGINT NOT NULL DEFAULT 0,
	CHECK (source_backend <> target_backend),
	CHECK (verified_objects <= copied_objects),
	CHECK (verified_bytes <= copied_bytes)
);
CREATE INDEX IF NOT EXISTS artifact_migrations_state_updated
	ON artifact_migrations (state, updated_at);
CREATE TABLE IF NOT EXISTS artifact_migration_mutations (
	migration_id            TEXT NOT NULL CHECK (length(migration_id) BETWEEN 1 AND 128),
	sequence                BIGINT NOT NULL CHECK (sequence > 0),
	operation               TEXT NOT NULL CHECK (operation IN ('put', 'delete')),
	object_key              TEXT NOT NULL CHECK (length(object_key) BETWEEN 1 AND 4096),
	etag                    TEXT NOT NULL DEFAULT '' CHECK (length(etag) <= 256),
	digest                  TEXT NOT NULL DEFAULT '' CHECK (length(digest) <= 256),
	state                   TEXT NOT NULL CHECK (state IN ('pending', 'applied')),
	created_at              BIGINT NOT NULL CHECK (created_at > 0),
	updated_at              BIGINT NOT NULL CHECK (updated_at > 0),
	PRIMARY KEY (migration_id, sequence)
);
CREATE INDEX IF NOT EXISTS artifact_migration_mutations_pending
	ON artifact_migration_mutations (migration_id, state, sequence);
`

// sqlSchemaV37 adds the durable, append-only checkpoint fact used by the
// experimental Graph-G1 execution slice. The JSON documents carry only the
// bounded graph contract; node implementations, prompts, credentials, and
// object bodies are deliberately outside this schema.
const sqlSchemaV37 = sqlSchemaV36 + `
CREATE TABLE IF NOT EXISTS graph_checkpoints (
	tenant_id       TEXT NOT NULL CHECK (length(tenant_id) BETWEEN 1 AND 128),
	session_id      TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 128),
	run_id          TEXT NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
	revision        BIGINT NOT NULL CHECK (revision > 0),
	checkpoint_json TEXT NOT NULL CHECK (length(checkpoint_json) BETWEEN 1 AND 655360),
	PRIMARY KEY (tenant_id, session_id, run_id)
);
CREATE TABLE IF NOT EXISTS graph_transitions (
	tenant_id       TEXT NOT NULL CHECK (length(tenant_id) BETWEEN 1 AND 128),
	session_id      TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 128),
	run_id          TEXT NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
	revision        BIGINT NOT NULL CHECK (revision > 0),
	transition_id   TEXT NOT NULL CHECK (length(transition_id) = 64),
	transition_json TEXT NOT NULL CHECK (length(transition_json) BETWEEN 1 AND 65536),
	PRIMARY KEY (tenant_id, session_id, run_id, revision),
	UNIQUE (transition_id)
);
`

// sqlSchemaV38 adds tenant-scoped notification target metadata and an opaque
// encrypted provider configuration. The configuration is never exposed by a
// descriptor query; channel adapters resolve it through their private seam.
const sqlSchemaV38 = sqlSchemaV37 + `
CREATE TABLE IF NOT EXISTS notification_targets (
	tenant_id          TEXT NOT NULL CHECK (length(tenant_id) BETWEEN 1 AND 256),
	target_ref         TEXT NOT NULL CHECK (length(target_ref) BETWEEN 1 AND 512),
	channel_id         TEXT NOT NULL CHECK (length(channel_id) BETWEEN 1 AND 128),
	channel_version    TEXT NOT NULL CHECK (length(channel_version) BETWEEN 1 AND 64),
	label              TEXT NOT NULL DEFAULT '' CHECK (length(label) <= 128),
	formats_json       TEXT NOT NULL DEFAULT '[]' CHECK (length(formats_json) <= 4096),
	config_ciphertext  TEXT NOT NULL DEFAULT '' CHECK (length(config_ciphertext) <= 262144),
	enabled            INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
	revision           BIGINT NOT NULL CHECK (revision > 0),
	created_at         BIGINT NOT NULL CHECK (created_at > 0),
	updated_at         BIGINT NOT NULL CHECK (updated_at > 0),
	PRIMARY KEY (tenant_id, target_ref)
);
CREATE INDEX IF NOT EXISTS notification_targets_tenant_enabled
	ON notification_targets (tenant_id, enabled, target_ref);
`

// sqlSchemaV39 adds the durable generation-fenced Graph segment lease.
const sqlSchemaV39 = sqlSchemaV38 + `
CREATE TABLE IF NOT EXISTS graph_segment_leases (
	tenant_id       TEXT NOT NULL CHECK (length(tenant_id) BETWEEN 1 AND 128),
	session_id      TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 128),
	run_id          TEXT NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
	segment_id      TEXT NOT NULL CHECK (length(segment_id) BETWEEN 32 AND 128),
	holder_id       TEXT NOT NULL CHECK (length(holder_id) BETWEEN 32 AND 128),
	host_generation BIGINT NOT NULL CHECK (host_generation > 0),
	expires_at      BIGINT NOT NULL CHECK (expires_at > 0),
	released        INTEGER NOT NULL DEFAULT 0 CHECK (released IN (0, 1)),
	created_at      BIGINT NOT NULL CHECK (created_at > 0),
	updated_at      BIGINT NOT NULL CHECK (updated_at > 0),
	PRIMARY KEY (tenant_id, session_id, run_id)
);
`

// sqlSchemaV40 adds indexes for approval lookup by run and stale run recovery.
const sqlSchemaV40 = sqlSchemaV39 + `
CREATE INDEX IF NOT EXISTS approval_requests_run_status
	ON approval_requests (run_id, status);
CREATE INDEX IF NOT EXISTS run_control_stale
	ON run_control (status, updated_at);
`

// sqlSchemaV41CheckpointHistory is applied only inside the v41 migration
// transaction. A complete v41 upgrade must never expose this table without
// the corresponding backfill, head-write fence, and schema marker.
const sqlSchemaV41CheckpointHistory = `
CREATE TABLE IF NOT EXISTS graph_checkpoint_versions (
	tenant_id         TEXT NOT NULL CHECK (length(tenant_id) BETWEEN 1 AND 128),
	session_id        TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 128),
	run_id            TEXT NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
	revision          BIGINT NOT NULL CHECK (revision > 0),
	version_id        TEXT NOT NULL CHECK (length(version_id) = 64),
	parent_version_id TEXT NOT NULL DEFAULT '' CHECK (length(parent_version_id) IN (0, 64)),
	origin            TEXT NOT NULL CHECK (origin IN ('commit', 'migration_floor', 'fork')),
	checkpoint_hash   TEXT NOT NULL CHECK (length(checkpoint_hash) = 64),
	checkpoint_json   TEXT NOT NULL CHECK (length(checkpoint_json) BETWEEN 1 AND 655360),
	created_at        BIGINT NOT NULL CHECK (created_at > 0),
	PRIMARY KEY (tenant_id, session_id, run_id, revision),
	UNIQUE (version_id)
);
`

// sqlSchemaV42 adds a separate durable authorization epoch. It deliberately
// does not reuse control_revision: release and canary synchronizers use that
// older revision to detect their own concurrent writes, while account and
// dynamic-binding mutations are independent authorization changes.
const sqlSchemaV42AuthorizationEpoch = `
INSERT INTO store_meta (key, value) VALUES ('authorization_epoch', '0')
	ON CONFLICT (key) DO NOTHING;
`

// sqlSchemaV43CompletedToolResultRecoverySidecars stores immutable SQL-local
// proof beside a canonical completed tool/result. It deliberately does not
// add a Session event, run_evidence segment, acknowledgement, or execution
// grant. Future native coordination must separately prove current authority.
const sqlSchemaV43CompletedToolResultRecoverySidecars = `
CREATE TABLE IF NOT EXISTS completed_tool_result_recovery_sidecars (
	protocol                 TEXT NOT NULL CHECK (protocol = 'completed_tool_result_sidecar/v1'),
	tenant_id                TEXT NOT NULL CHECK (length(tenant_id) BETWEEN 1 AND 256),
	subject_id               TEXT NOT NULL CHECK (length(subject_id) BETWEEN 1 AND 256),
	session_id               TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 128),
	run_id                   TEXT NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
	call_id                  TEXT NOT NULL CHECK (length(call_id) BETWEEN 1 AND 128),
	capability_id            TEXT NOT NULL CHECK (length(capability_id) BETWEEN 1 AND 256),
	args_digest              TEXT NOT NULL CHECK (length(args_digest) = 64),
	idempotent               INTEGER NOT NULL CHECK (idempotent IN (0, 1)),
	origin_step_seq          BIGINT NOT NULL CHECK (origin_step_seq >= 0),
	call_event_seq           BIGINT NOT NULL CHECK (call_event_seq >= 0),
	result_event_seq         BIGINT NOT NULL CHECK (result_event_seq = call_event_seq + 1),
	result_sha256            TEXT NOT NULL CHECK (length(result_sha256) = 64),
	authorization_epoch      BIGINT NOT NULL CHECK (authorization_epoch >= 0),
	profile_snapshot_id      TEXT NOT NULL CHECK (length(profile_snapshot_id) BETWEEN 1 AND 512),
	capability_snapshot_id   TEXT NOT NULL CHECK (length(capability_snapshot_id) BETWEEN 1 AND 512),
	composition_revision     TEXT NOT NULL CHECK (length(composition_revision) = 64),
	assignment_revision      TEXT NOT NULL CHECK (length(assignment_revision) = 64),
	composition_json         TEXT NOT NULL CHECK (length(composition_json) BETWEEN 1 AND 1048576),
	composition_sha256       TEXT NOT NULL CHECK (length(composition_sha256) = 64),
	created_at               BIGINT NOT NULL CHECK (created_at > 0),
	PRIMARY KEY (session_id, run_id, call_id),
	UNIQUE (session_id, result_event_seq)
);
CREATE INDEX IF NOT EXISTS completed_tool_result_recovery_sidecars_run_created
	ON completed_tool_result_recovery_sidecars (run_id, created_at);
`

// sqlSchemaV44NativeQueuedToolEffectWitnesses stores immutable evidence that
// one queued tool effect reached its SQL admission boundary. It is not proof
// that the provider ran, a continuation grant, or current account authority.
const sqlSchemaV44NativeQueuedToolEffectWitnesses = `
CREATE TABLE IF NOT EXISTS native_queued_tool_effect_witnesses (
	protocol                    TEXT NOT NULL CHECK (protocol = 'native_queued_tool_effect/v1'),
	tenant_id                   TEXT NOT NULL CHECK (length(tenant_id) BETWEEN 1 AND 256),
	subject_id                  TEXT NOT NULL CHECK (length(subject_id) BETWEEN 1 AND 256),
	session_id                  TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 128),
	run_id                      TEXT NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
	call_id                     TEXT NOT NULL CHECK (length(call_id) BETWEEN 1 AND 128),
	capability_id               TEXT NOT NULL CHECK (length(capability_id) BETWEEN 1 AND 256),
	args_digest                 TEXT NOT NULL CHECK (length(args_digest) = 64),
	idempotent                  INTEGER NOT NULL CHECK (idempotent IN (0, 1)),
	authorization_epoch         BIGINT NOT NULL CHECK (authorization_epoch >= 0),
	queue_generation            BIGINT NOT NULL CHECK (queue_generation > 0),
	lease_holder_sha256         TEXT NOT NULL CHECK (length(lease_holder_sha256) = 64),
	run_start_seq               BIGINT NOT NULL CHECK (run_start_seq >= 0),
	origin_step_seq             BIGINT NOT NULL CHECK (origin_step_seq > run_start_seq),
	tool_call_seq               BIGINT NOT NULL CHECK (tool_call_seq > origin_step_seq),
	session_version_after_call  BIGINT NOT NULL CHECK (session_version_after_call = tool_call_seq + 1),
	profile_snapshot_id         TEXT NOT NULL CHECK (length(profile_snapshot_id) BETWEEN 1 AND 512),
	capability_snapshot_id      TEXT NOT NULL CHECK (length(capability_snapshot_id) BETWEEN 1 AND 512),
	composition_revision        TEXT NOT NULL CHECK (length(composition_revision) = 64),
	assignment_revision         TEXT NOT NULL CHECK (length(assignment_revision) IN (0, 64)),
	composition_sha256          TEXT NOT NULL CHECK (length(composition_sha256) = 64),
	bootstrap_revision          TEXT NOT NULL CHECK (length(bootstrap_revision) BETWEEN 1 AND 512),
	capability_manifest_sha256  TEXT NOT NULL CHECK (length(capability_manifest_sha256) = 64),
	provider_revision           TEXT NOT NULL CHECK (length(provider_revision) <= 512),
	capability_contract_sha256  TEXT NOT NULL CHECK (length(capability_contract_sha256) = 64),
	created_at                  BIGINT NOT NULL CHECK (created_at > 0),
	PRIMARY KEY (session_id, run_id, call_id, queue_generation),
	UNIQUE (session_id, tool_call_seq, queue_generation)
);
CREATE INDEX IF NOT EXISTS native_queued_tool_effect_witnesses_run_created
	ON native_queued_tool_effect_witnesses (run_id, created_at);
`

// sqlSchemaV45NativeQueuedModelInvocations stores one immutable admission for
// each durable main-model step. This first slice records attempts only; rows
// are permanent replay fences until a future canonical outcome/GC protocol.
const sqlSchemaV45NativeQueuedModelInvocations = `
CREATE TABLE IF NOT EXISTS native_queued_model_invocations (
	protocol                    TEXT NOT NULL CHECK (protocol = 'native_queued_model_call/v1'),
	tenant_id                   TEXT NOT NULL CHECK (length(tenant_id) BETWEEN 1 AND 256),
	subject_id                  TEXT NOT NULL CHECK (length(subject_id) BETWEEN 1 AND 256),
	session_id                  TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 128),
	run_id                      TEXT NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
	invocation_id               TEXT NOT NULL CHECK (length(invocation_id) BETWEEN 7 AND 128),
	step_index                  INTEGER NOT NULL CHECK (step_index >= 0),
	step_start_seq              BIGINT NOT NULL CHECK (step_start_seq >= 0),
	session_version_at_admission BIGINT NOT NULL CHECK (session_version_at_admission > step_start_seq),
	request_json                TEXT NOT NULL CHECK (length(request_json) BETWEEN 1 AND 1048576),
	request_sha256              TEXT NOT NULL CHECK (length(request_sha256) = 64),
	authorization_epoch         BIGINT NOT NULL CHECK (authorization_epoch >= 0),
	queue_generation            BIGINT NOT NULL CHECK (queue_generation > 0),
	lease_holder_sha256         TEXT NOT NULL CHECK (length(lease_holder_sha256) = 64),
	run_start_seq               BIGINT NOT NULL CHECK (run_start_seq >= 0),
	profile_snapshot_id         TEXT NOT NULL CHECK (length(profile_snapshot_id) BETWEEN 1 AND 512),
	capability_snapshot_id      TEXT NOT NULL CHECK (length(capability_snapshot_id) BETWEEN 1 AND 512),
	composition_revision        TEXT NOT NULL CHECK (length(composition_revision) = 64),
	assignment_revision         TEXT NOT NULL CHECK (length(assignment_revision) IN (0, 64)),
	composition_sha256          TEXT NOT NULL CHECK (length(composition_sha256) = 64),
	bootstrap_revision          TEXT NOT NULL CHECK (length(bootstrap_revision) BETWEEN 1 AND 512),
	model_contract_sha256       TEXT NOT NULL CHECK (length(model_contract_sha256) = 64),
	created_at                  BIGINT NOT NULL CHECK (created_at > 0),
	PRIMARY KEY (session_id, run_id, invocation_id),
	UNIQUE (session_id, step_start_seq)
);
CREATE INDEX IF NOT EXISTS native_queued_model_invocations_run_created
	ON native_queued_model_invocations (run_id, created_at);
`

// sqlSchemaV46NativeQueuedModelOutcomes binds one immutable canonical model
// outcome to its v45 attempt and Session event pair. It is historical delivery
// evidence, not a continuation grant or permission to replay a provider call.
const sqlSchemaV46NativeQueuedModelOutcomes = `
CREATE TABLE IF NOT EXISTS native_queued_model_invocation_outcomes (
	protocol                       TEXT NOT NULL CHECK (protocol = 'native_queued_model_outcome/v1'),
	session_id                     TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 128),
	run_id                         TEXT NOT NULL CHECK (length(run_id) BETWEEN 1 AND 128),
	invocation_id                  TEXT NOT NULL CHECK (length(invocation_id) BETWEEN 7 AND 128),
	attempt_request_sha256         TEXT NOT NULL CHECK (length(attempt_request_sha256) = 64),
	assistant_event_seq            BIGINT NOT NULL CHECK (assistant_event_seq >= 0),
	usage_event_seq                BIGINT NOT NULL CHECK (usage_event_seq = assistant_event_seq + 1),
	session_version_after_outcome  BIGINT NOT NULL CHECK (session_version_after_outcome = usage_event_seq + 1),
	outcome_sha256                 TEXT NOT NULL CHECK (length(outcome_sha256) = 64),
	created_at                     BIGINT NOT NULL CHECK (created_at > 0),
	PRIMARY KEY (session_id, run_id, invocation_id),
	UNIQUE (session_id, assistant_event_seq),
	UNIQUE (session_id, usage_event_seq)
);
CREATE INDEX IF NOT EXISTS native_queued_model_invocation_outcomes_run_created
	ON native_queued_model_invocation_outcomes (run_id, created_at);
`
