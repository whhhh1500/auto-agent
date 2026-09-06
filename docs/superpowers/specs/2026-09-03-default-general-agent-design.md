# Default General Agent

## Outcome

`cmd/server` ships with one usable default Agent profile so a new installation
does not require an operator to understand Profile management before creating a
session. The built-in profile ID is `general`, its Console label is
`通用智能体`, and an omitted `profile_id` selects it.

The foundation remains integration-friendly: `pkg/core` does not gain a
product-specific profile, and embedders that construct `server.Server` keep the
current explicit-profile behavior unless they configure a default profile ID.

## Considered approaches

1. **Application-owned built-in profile with an explicit selected model
   (selected).** A small application component mounts the `general` base layer
   into the existing profile registry. It reads the authoritative database
   model setting at startup and refreshes the mounted layer after a successful
   model-settings update. Runs continue to record the exact wire model.
2. **A magic `default` model alias in core.** This is smaller at first, but the
   profile snapshot and run evidence would contain an alias while the provider
   calls a different model. That breaks the existing audit invariant and is
   rejected.
3. **Persist a normal profile row during database migration.** This mixes
   application defaults into storage schema evolution, complicates upgrades and
   makes the default harder for embedders to replace. It is rejected.

## Architecture

### Default profile component

A focused application package owns:

- exported profile ID `general`;
- the neutral name, description and prompt fragments;
- bounded default execution limits;
- mounting and refreshing the built-in layer;
- synchronization of concurrent model-setting writes.

The component depends only on the profile registry, its mount scope, and a
model-selection source. It does not read SQL, environment variables, HTTP
requests, credentials or provider implementations directly.

`cmd/server` mounts the built-in layer at the global scope. This is required so
the first platform administrator (`global/session`) and all deployment,
product, tenant and user descendants can resolve the same default. More
specific scoped layers may still override it normally.

The executable assembly adapts the current database-first legacy model settings
to that source. A present database record is authoritative. Only when the row is
absent may the existing environment fallback supply the initial model, matching
the current migration contract.

The base profile has no tools. Tools remain an explicit allow-list and can be
added through ordinary scoped profile layers. This avoids silently granting a
newly installed dangerous capability to every account.

### Model-setting synchronization

The model-settings application service receives an optional post-save observer.
After the repository save succeeds, the observer refreshes the built-in profile
with the exact selected provider and wire model. The refresh mounts the new
layer before unmounting the old one and serializes concurrent refreshes; it does
not accumulate one registry layer per settings update.

If the observer fails, the settings request returns an error instead of claiming
that the runtime switched models. The database remains the source of truth, so
a retry or restart reconciles the in-memory profile. No credential value enters
the profile or logs.

When no active model configuration exists, `general` remains visible but a Run
fails with the existing explicit model-configuration error. The Console should
direct the administrator to configure the model; it must not invent a mock
provider or silently use an unrelated environment credential.

### Session API

`server.Config` gains an optional `DefaultProfileID`. Session creation resolves:

1. the request's non-empty `profile_id`, otherwise
2. `Config.DefaultProfileID`, otherwise
3. the existing `400 profile_id is required` response.

`cmd/server` sets this option to `general`. Library users and tests that do not
set it retain the old contract. OpenAPI marks `profile_id` optional and explains
that omission only works when the deployment has configured a default.

Unknown explicit profile IDs still fail; they never fall back to `general`.

### Console behavior

The Playground prefers `general` whenever it is present instead of depending on
catalog order. It still allows selecting another profile. Creating a session
sends the selected ID for transparency, while API clients may omit it.

When `general` is the only visible profile, the Playground renders its Chinese
name as read-only state instead of showing a one-choice selector. Existing
session continuation is kept in a collapsed optional section.

The default Console navigation exposes only `开始使用` and `对话`. Capability,
Profile release, approval, evaluation, evidence, delegation, runner, policy,
credential, resource, session, binding, audit and observability surfaces remain
available under an explicit `高级管理` disclosure. The start page puts model and
API-key configuration first; Base URL, allowed-model policy, storage mutation
and runtime details are collapsed by default. This changes disclosure only and
does not remove any API, field or extension seam.

User-facing text calls profiles `智能体配置`; release, layer, scope and canary
language remains under advanced administration and is outside this focused
change.

## Data flow

```text
database model setting ──> model-selection source ──> general profile layer
          │                                              │
          └── successful settings update ──> refresh ────┘

POST /v1/sessions (profile omitted)
          └──> Config.DefaultProfileID ──> general ──> immutable run snapshot
```

## Compatibility and safety

- No schema migration is required.
- Existing explicit profiles and sessions are unchanged.
- The built-in profile is an application-layer contribution and can be
  extended or overridden by normal scoped layers.
- API keys stay solely in the encrypted settings repository.
- Exact selected model evidence is preserved; no magic alias reaches a Run.
- The default capability set is empty, so installing a plugin cannot silently
  expand `general` authority.

## Verification

Tests must cover:

- `general` exists after executable bootstrap and contains the configured exact
  model, neutral prompt and no tools;
- database configuration wins over the environment fallback;
- a successful model-settings update refreshes `general` without growing the
  live binding count;
- refresh failure is returned and restart reconciliation uses database state;
- omitted `profile_id` uses `general` only when configured on the server;
- explicit valid/invalid profile behavior is unchanged;
- the Console prefers `general` independent of profile catalog order;
- a global-mounted `general` resolves for both platform-admin sessions and
  product/tenant/user descendants;
- the Console's basic view exposes only start/chat actions, while every prior
  administrative route remains available after expanding advanced management;
- OpenAPI verification and the existing Go test suite pass.

## Out of scope

- Automatically attaching all installed tools;
- provider catalog and multi-protocol routing work planned for the model-control
  milestone;
- changing existing profile release, canary or inheritance semantics;
- creating a mock/offline conversational model.
