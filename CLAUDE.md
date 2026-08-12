# Klokka

Persistent background jobs for Ktor: a coroutines-native job queue and scheduler with
typed suspend handlers, at-least-once execution on the user's database, transactional
enqueue, and a dashboard. Norwegian for "the clock"; runs like one.

Canonical sources, in order of authority:
1. `docs/design.md`: the design document. Sections 12 and 14 are the decisions log.
2. GitHub issues and milestones (M1 through M3): the backlog and sequencing.
3. This file: operational rules for working on the code.

## Architecture invariants (never violate without a recorded decision)

- `klokka-core` is Ktor-free and KMP-structured. `commonMain` is pure Kotlin plus
  kotlinx libraries only (coroutines, serialization, datetime, kotlin.time). JVM-specific
  code (slf4j, JDBC-adjacent IO) lives in `jvmMain` behind small expect/actual seams.
  Ktor glue (`install(Klokka)`, lifecycle wiring, dashboard mounting) belongs in the
  separate `klokka-ktor` module.
- The `JobStore` SPI KDoc (`spi/JobStore.kt`) is the binding portability contract that
  the TCK enforces. Changing it is a design decision, not a refactor. Its core rules:
  - No SPI method accepts caller-supplied time for comparisons. Due-ness is decided by
    the store's own clock (database time). Node clocks are never trusted.
  - `claim` is one atomic operation (SKIP LOCKED or CAS), never an in-process check.
  - The `queues` parameter is an ordered `List`: declared order IS priority. There are
    no per-job priority numbers, permanently.
  - A `uniqueKey` conflict with a non-terminal job is idempotent success returning the
    existing id, not an error.
  - `transition` is CAS-only: stores check state and fence equality, never state-machine
    edge legality (that is the runtime's job).
  - Transitions out of `Running` must carry the fence from `ClaimedJob`; omitting it
    silently disables zombie-worker protection.
  - `heartbeat` ignores expired leases, including the calling worker's own. Revival goes
    through `claim`, which bumps attempt and fence.
- Execution semantics are at-least-once. Never write "exactly-once" anywhere: code,
  KDoc, docs, or README.
- `RetentionPolicy` (and any future sweep configuration) stays declarative so stores can
  compile it to bulk SQL. Never accept a lambda for retention decisions.
- Typed job descriptors only: a stable string kind plus a `@Serializable` payload.
  Never serialize lambdas or derive job identity from class or function names.
- Cron is 5-field standard with seconds opt-in. Quartz `L`/`W`/`#` operators are
  permanently out of scope; complex calendars are served by the typed DSL.
- The Postgres store uses plain JDBC internally. Exposed is recommended in docs and
  examples but never required; its integration is the `klokka-exposed` bridge artifact.
- The dashboard mounts via a `Route` extension and fails closed: no authentication in
  effect means it refuses to serve unless `allowAnonymous = true` is explicit.

## Build and verify

- `./gradlew :klokka-core:build` runs compile, lenient ktlint/detekt, apiCheck, and tests.
- After any public API change: `./gradlew apiDump`, then review and include the
  `klokka-core/api/*.api` diff deliberately. Binary-compatibility-validator is enforced.
- Gradle 9.6.1 wrapper, Kotlin per `gradle/libs.versions.toml`, JVM toolchain 17.
- Kotlin gotcha already hit once: `vararg` of a value class (`Duration`, `JobId`) does
  not compile. Use `List` parameters.

## Code conventions

- `explicitApi()` mode: every public declaration is explicitly `public` and carries KDoc.
  On the SPI, KDoc is contract text: write it as binding semantics, not commentary.
- No wildcard imports. Match the existing style; ktlint is lenient on purpose because
  formatting is not settled: do not mass-reformat files you are not otherwise changing.
- Tests live in `commonTest` (kotlin-test, kotlinx-coroutines-test). Drive time with the
  injectable clock (`TestClock`), never with sleeps or the system clock.
- Store implementations must stay faithful to the SPI KDoc; when an implementation
  reveals a contract ambiguity, fix the KDoc in the same change and note it.

## Publishing and licensing

- Maven group `com.eventslooped` (verified namespace shared with kurier). Publishing
  copies kurier's vanniktech maven-publish + Central Portal setup (issue #17).
- Apache-2.0. The design doc section 11 lists features that must never move behind a
  paid tier. Do not draft or suggest "pro tier" gating for anything on that list.

## Working norms for Claude sessions

- Leave changes uncommitted for the owner to review and commit, unless explicitly asked
  to commit. Never add AI attribution or co-author trailers to commits or PRs.
- Delegate straightforward, well-specified implementation to Sonnet subagents; keep API
  and contract design in the main session. Everything a subagent writes gets reviewed
  against the SPI contract before it lands.
- Prose style in all project writing: no em-dashes; use commas, colons, or separate
  sentences.
- When a design question is resolved in conversation, record it: design doc decisions
  log for architecture, issue edits for scope, KDoc for contracts.
