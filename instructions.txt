1) Fail loudly on unexpected states
- If something “should never happen” (missing required fields, invalid values, unexpected event types, channel closed, lock poisoned, etc.), emit a strong warning with enough context to debug, and then error / stop the workflow. Do not just “continue”.
- Do not silently coerce missing/invalid values into defaults like 0, "", false, empty arrays, “unknown”, etc. If missing is acceptable, represent it explicitly and handle it deliberately.
- Never ignore errors “because it’s probably fine”. If you intentionally ignore an error, it must be clearly justified and logged/counted.

2) No silent parsing failures
- Any parse failure (JSON/YAML/XML/CSV/binary decode) must either:
  - return an error, or
  - be dropped with a warning + a counter/metric (rate-limited), including: source, event/type, and a truncated payload sample.
- If you keep “last-good” cached data, also track: `parse_fail_count`, `last_parse_error`, `last_good_ts`.
- Do not hide NaNs/infinities: treat them as bugs (filter + warn) or error.

3) External I/O must be validated (HTTP, WS, SDKs, files, OS APIs)
- Validate endpoints/paths before use (scheme/host/port/path, file existence/permissions). If invalid, error immediately.
- HTTP/SDK calls:
  - set explicit timeouts
  - validate status codes and response types
  - on non-success, log: method + endpoint + status + truncated body and error out
  - on parse/schema mismatch, log a truncated raw snippet + expected shape and error out
- WebSockets/streams:
  - verify subscribe/auth message formats against docs (and/or a known-good captured sample)
  - treat send failures as meaningful events (log them; they often imply connection death)
  - handle unexpected message types explicitly (warn + count, or error)
- File writes (logs, caches, exports): treat write/flush errors as errors (no `.ok()`-style swallowing).

4) Retries/reconnects are not “free”
- Fail with a clear error 
- Do NOT silently retry forever.
- If retries/reconnects exist, make policy explicit:
  - bounded attempts
  - exponential backoff with cap (+ jitter if appropriate)
  - log: reason, attempt number, and next sleep duration
- Do not implement “fallbacks” to alternate endpoints/formats unless the user explicitly asked for it.

5) Configuration must be explicit and auditable
- Required production values must be required (credentials, IDs, endpoints, paths). Avoid sentinel defaults.
- If config comes from file/env/flags, log the resolved values and the source at startup (never log secrets).
- Validate config ranges at startup (intervals > 0, sizes >= 0, paths exist, enums known) and fail fast if invalid.

6) Concurrency/state safety
- Treat poisoned/shared-state failures as serious (log loudly; prefer erroring out over silently continuing).
- Avoid busy-wait loops; prefer event-driven waits or backoff.
- Do not block an event loop / UI thread with long work; use the correct threading/async model for the environment.

7) Modularize to reduce risk. As a ball park, a single class should not be over 1000 lines, except where REALLY needed. If code exceeds this length and appropriate, suggest a potential refactor. 

8) Observability (make failures diagnosable)
- Every critical workflow should have structured logs (level, context IDs, key parameters) and counters for dropped/rejected events.
- Errors should include enough context to reproduce: inputs (sanitized), source, and expected vs actual.

9) Resource lifecycle
- Anything opened must be closed: files, connections, handles, temp resources.
- Use language-appropriate patterns (defer, try-with-resources, context managers, RAII).
- On error paths, ensure cleanup still runs (no early returns that leak).

10) Graceful shutdown
- Handle termination signals; do not exit mid-write or mid-transaction.
- Drain in-progress work or persist enough state to resume.
- Log shutdown reason and duration.

11) Agent operating mode (meta)
- If requirements are ambiguous, ask one clarifying question before proceeding.
- Propose structural changes (new files, renamed exports, changed interfaces) before implementing.
- Stay scoped: fix what's asked; flag adjacent issues but don't fix them without approval.
- If a task would require changes across >N files, summarize the plan first.

12) When running python code, you must activate the .venv virtual environment

13) Sometimes the user may suggest a suggestion that doesn't make sense. Treat this codebase like your own - it's your job to tell the user about any potential unexpected behaviour that the user might not have considered if their suggestion is obviously dumb. 
