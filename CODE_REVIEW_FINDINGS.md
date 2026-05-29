# Nano — Code Review Findings (10–20k CCU readiness)

**Scope:** Core library of `github.com/lonng/nano` (this fork). Excludes `examples/**`, generated `*.pb.go`, and tests except where they document intended semantics.
**Driving question:** Can this server reliably handle 10,000–20,000 concurrent connected users (CCU)?
**Method:** 6 parallel domain reviewers + direct line-level validation of every high-impact claim against the source.
**Verification:** all findings independently re-checked by an oracle pass (line-level); corrections and 4 additional issues folded in below.

---

## Verdict: NOT READY for 10–20k CCU

The blocking issues are correctness/safety, not raw capacity:

- **A single malformed packet from any unauthenticated client crashes the entire process** (and drops all connections) — there is no `recover()` in the connection read loop.
- Real **data races** on per-connection and cluster-membership state (fail under `go test -race`).
- A **self-deadlock** in the per-connection write goroutine that leaks goroutines/FDs.
- **Synchronous logging on the per-message hot path** that collapses throughput.
- Throughput is capped by a **single global scheduler goroutine** on the default dispatch path.

Runtime shape at 20k CCU: ~40k goroutines (read + write loop per conn), 20k `time.Ticker`, 20k decoders each with a growing `bytes.Buffer`, 20k×2 channels. Memory/goroutine counts are fine for Go; the defects below are what break it.

Severity legend: **CRITICAL** = crash / data race / unbounded / remotely-triggerable DoS · **HIGH** = correctness bug or hard scale ceiling · **MEDIUM** = latent bug / inefficiency · **LOW** = minor.

## Verification status (oracle, independent re-check)

An independent oracle pass re-opened every cited line. Outcome: the **NOT READY** conclusion is upheld; the headline crash/race/deadlock findings (C1–C4, C6, H1–H7, H9, H12) are **CONFIRMED**. Adjustments: **C5** downgraded CRITICAL→HIGH; **C6** impact corrected upward (whole-process crash); **H8/H10/H11/M5** refined with preconditions; **4 issues added** (H13–H15, L3) found during verification and re-confirmed directly against the code.

**Round 2 (cluster / HTTP / SSE / WS deep pass):** 4 reviewers + an oracle re-check added **26 issues** — 1 CRITICAL (C7), 12 HIGH (H16–H27), 12 MEDIUM (M7–M18), 1 LOW (L4). All 29 round-2 candidates had a real code path; the systemic theme is unsafe ownership/lifecycle across HTTP/SSE/WS and cluster RPC (over-trusted request/session state, incomplete cleanup, missing deadlines/recovery, unpropagated errors). The trust/authz gaps (M9/M11) are scored MEDIUM under a trusted inter-node-network assumption.

**Round 3 (component / serialize / service / internal / root):** 3 reviewers + an oracle re-check added **15 issues** — 2 HIGH (H28–H29), 10 MEDIUM (M19–M28), 3 LOW (L5–L7); no new CRITICAL. The two HIGH are remotely-triggerable Prometheus label-cardinality memory DoS (client-controlled `route` and per-`ip` series that are never deleted). Most others are config-time misconfiguration panics (nil/zero option values — downgraded to MEDIUM as fail-fast operator errors) and runtime-only global-state races.

**Round 4 (final convergence sweep):** an oracle full-core sweep added **3 issues** — 1 HIGH (H30), 1 MEDIUM (M29), 1 LOW (L8); the remaining surface only reproduced already-ledgered issues. **Running total: 74 findings (7 CRITICAL, 30 HIGH, 29 MEDIUM, 8 LOW).**

**Round 5 (convergence confirmation):** one more LOW (L9); everything else duplicated ledger IDs. **Running total: 75 findings (7 CRITICAL, 30 HIGH, 29 MEDIUM, 9 LOW).** New-issue yield per round is collapsing (26 → 15 → 3 → 1), indicating convergence; a final clean sweep is the stopping criterion.

**Round 6 (convergence confirmation):** 2 more — 1 HIGH (H31), 1 MEDIUM (M30) — in `node.go` startup/option wiring; all else duplicated. **Running total: 77 findings (7 CRITICAL, 31 HIGH, 30 MEDIUM, 9 LOW).** Remaining new findings are confined to cluster startup/config wiring; the loop continues until a sweep returns zero.

**Round 7 (convergence confirmation):** 3 more — 1 HIGH (H32), 2 MEDIUM (M31–M32) — in the HTTP/SSE lifecycle and option-validation seams. **Running total: 80 findings (7 CRITICAL, 32 HIGH, 32 MEDIUM, 9 LOW).** The residual tail is concentrated in HTTP/SSE lifecycle and `With*` option validation; an exhaustive pass over those seams follows.

**Round 8 (exhaustive seam sweep):** an exhaustive pass over `options.go` (every `With*` setter) + the full HTTP/SSE lifecycle added **10 issues** — 3 HIGH (H33–H35), 5 MEDIUM (M33–M37), 2 LOW (L10–L11) — and audited every setter/aspect (results in the agent artifact). **Running total: 90 findings (7 CRITICAL, 35 HIGH, 37 MEDIUM, 11 LOW).** The HTTP/SSE subsystem is the buggiest area; a final clean confirmation follows.

**Round 9 (final convergence confirmation): CONVERGED.** An independent full re-sweep of the core hot paths + remaining cluster helpers found **no new issues** — every observed problem maps to an existing ID. **Iteration stopped. Final tally: 90 findings — 7 CRITICAL · 35 HIGH · 37 MEDIUM · 11 LOW.** New-issue yield by round: 30 → 26 → 15 → 3 → 1 → 2 → 3 → 10 → 0.

## Fix status (automated run)

Automated TDD fix run (red→green per finding, owned-file scope per unit), followed by an **orchestrated enhancement run** (subagent fan-out + oracle-reviewed direction) that implemented the remaining items. All 90 findings were attempted.

| Status | Count |
| --- | --- |
| FIXED | 87 |
| PARTIAL | 3 |
| WONTFIX | 0 |
| FAILED | 0 |
| NOT-ATTEMPTED | 0 |

**Integration:** build **pass** · vet **fail** (2 PRE-EXISTING findings only: `interface.go:111` unbuffered signal channel, `examples/cluster/main.go:152` stringintconv) · test **partial** · race **pass** (specified package set, no data races).

- PARTIAL (3): **H34** (request-scoped `ResponseMid` landed + tested; full `session.Response()` decoupling needs a request-scoped network entity — broad signature change), **M35** (HTTP response + local `/api` now use the JSON codec; remote-routed `/api` JSON decode needs a `clusterpb` payload-encoding field + regen — works today in a homogeneous JSON cluster), **M11** (owner check needs a caller-gate field in `clusterpb` to bind the authenticated peer to a session).
- WONTFIX (0): none — **H1** (opt-in sharded scheduler), **H9** (gRPC auth interceptor + client token), **M27** (copy `[]byte` at enqueue), **H11** (global connection cap), **H35** (SSE bounded write deadline), **M30** (WS route gated by `IsWebsocket`) were all implemented in the orchestrated enhancement run with tests.
- **Remaining regressions:** none. The one regression introduced by the run (the `internal/log` `Debug` change made `go vet` treat `log.Debug` as a print-style func, breaking `go vet` + `go test ./internal/codec`) was fixed by routing `internal/codec/codec.go:57` through `log.Debugf`. The 2 vet findings and the `cluster` integration-suite hang (`TestNode`/`TestNodeHTTP`) are pre-existing baseline issues, not regressions.

---

## CRITICAL (P0)

### C1 — One malformed packet crashes the whole process (unauthenticated DoS) — STATUS: FIXED
- **Location:** `internal/message/message.go:201`; missing recovery in `cluster/handler.go:301-433`.
- **Evidence:** `code := binary.BigEndian.Uint16(data[offset:(offset + 2)])` is reached after only `if offset >= len(data)` (`message.go:194`). With the compress bit set and exactly one byte left after the message-id, `data[offset:offset+2]` is out of range → **panic**. The read loop (`handle`) and `processPacket` have **no `recover()`** (the only `recover` in `cluster/` is in `agent.send`). An unrecovered panic in the read goroutine terminates the process.
- **Impact @ CCU:** Any client can kill the gate and all connected users with one crafted frame.
- **Fix:** Add `if offset+2 > len(data) { return nil, ErrWrongMessage }` before the slice; **and** wrap the read loop body in a `defer func(){ recover() ... }()` so a single bad connection cannot crash the server.

### C2 — Concurrent `Close` double-closes `chDie` → panic — STATUS: FIXED
- **Location:** `cluster/agent.go:204-221`.
- **Evidence:** `if a.status() == statusClosed { return }` then `select { case <-a.chDie: default: close(a.chDie) }`. The check-then-close is **not atomic**. Two goroutines (e.g. app `Session.Close()` racing the write-loop defer `a.Close()` at `agent.go:282`) can both pass the status check and both take `default`, calling `close(a.chDie)` twice → panic → process crash.
- **Impact @ CCU:** Disconnect storms make concurrent close common; each collision can crash the process.
- **Fix:** Gate with `atomic.CompareAndSwapInt32(&a.state, prev, statusClosed)` (or `sync.Once`) and only the winner closes channels / emits metrics.

### C3 — Data race on `agent.lastAt` (every connection) — STATUS: FIXED
- **Location:** write: `cluster/handler.go:431` (`agent.lastAt = time.Now().Unix()`); reads: `cluster/agent.go:292,294` (`atomic.LoadInt64(&a.lastAt)`).
- **Evidence:** Plain assignment races atomic loads on the same field that drives heartbeat-timeout decisions.
- **Impact @ CCU:** Guaranteed data race (fails `-race`); undefined heartbeat-timeout behavior under load.
- **Fix:** `atomic.StoreInt64(&a.lastAt, time.Now().Unix())` everywhere it is written (centralize in `a.touch()`).

### C4 — Write goroutine self-deadlocks on full `chWrite` → goroutine + FD leak — STATUS: FIXED
- **Location:** `cluster/agent.go:274-366` (send at `:357`, receive at `:300`).
- **Evidence:** `agent.write` is both the sender (`chWrite <- p`) and the only receiver (`case data := <-chWrite`) of `chWrite` (cap 16). If `select` keeps choosing the `a.chSend` case until `chWrite` fills, the next `chWrite <- p` blocks forever — the goroutine can no longer drain `chWrite`, observe `chDie`, or close the conn.
- **Impact @ CCU:** Each occurrence permanently leaks one goroutine + one FD + one connection; accumulates toward FD exhaustion under bursty fan-out.
- **Fix:** Remove the intermediate `chWrite`; write directly to `a.conn.Write` in the `chSend` case. If buffering is needed, make the send a `select` with a `<-a.chDie` escape.

### C5 — Unconditional logging on the per-message hot path collapses throughput  ·  *severity corrected: CRITICAL → HIGH* — STATUS: FIXED
> **Verification (oracle):** code confirmed (incl. `cluster/http_agent.go:148,154,157,173,193,202,206`); impact depends on the log sink/rate, so reclassified as HIGH performance/DoS rather than CRITICAL.
- **Locations (not gated by `env.Debug`, run per packet/message):**
  - `cluster/agent.go:356` — `log.Println("push encoded t chWrite")` (every outbound packet; also a leftover typo).
  - `cluster/handler.go:633` — `log.Infof("Local process task completed: %v", msg.Route)` (every handled message).
  - `cluster/handler.go:684` / `:687` — `log.Infof("Schedule task: %v", task)` / `"Push task: %v"` (every message; also formats a `func` value via reflection).
  - `cluster/http_agent.go` — per-message info logs on `Push`/`RPC`/`ResponseMid` incl. payload bytes.
- **Impact @ CCU:** Synchronous I/O + formatting on every message saturates CPU/IO even with debug off. This is the **highest-impact, lowest-risk** performance fix.
- **Fix:** Delete leftover debug lines; gate the rest behind `if env.Debug` and never format payloads/func values unless the line will be emitted.

### C6 — Unrecovered panic in scheduler/timer kills all dispatch — STATUS: FIXED
- **Location:** `scheduler/scheduler.go:117` (`cron()` called directly); `scheduler/timer.go:122-124` (`t.condition.Check(now)` outside `safecall`).
- **Evidence:** `try` (`scheduler.go:56-63`) wraps only task handlers; `safecall` (`timer.go:94-98`) wraps only `t.fn`. A panic in `cron` or `condition.Check` escapes the single scheduler goroutine → it dies → no client is served anymore.
- **Impact @ CCU:** *(verification correction)* `Sched` runs as `go scheduler.Sched()` (`interface.go:110`), so an unrecovered panic in `cron`/`condition.Check` terminates the **entire Go process** — not just the dispatch loop. One bad condition-timer drops every connected client.
- **Fix:** Wrap `cron()` and each timer's condition evaluation in the same `recover()` boundary; close the faulty timer.

### C7 — Nil `MemberInfo` in cluster RPC handlers crashes the process *(round 2; verified directly)* — STATUS: FIXED
- **Location:** `cluster/cluster.go:175` (`req.MemberInfo.Label`); `cluster/handler.go:172` (`member.Services` via `addRemoteService`, called by `NewMember` `cluster/node.go:857-859`). gRPC server has no recovery interceptor (`cluster/node.go:590`, `grpc.NewServer()`).
- **Evidence:** Verified directly — `Heartbeat` does `log.Println("Receive Heartbeat from: ", req.MemberInfo.Label)` with no nil check while holding `c.mu`; `MemberInfo` is an optional proto message (pointer, nil if omitted). A `HeartbeatRequest{}` / `NewMemberRequest{}` with nil `MemberInfo` → nil-pointer panic in the gRPC handler goroutine → **process crash** (and `Heartbeat` also leaves `c.mu` locked).
- **Impact @ CCU:** One malformed membership RPC crashes the master/service node and drops every connected client. Reachable by anything that can reach the gRPC port (see H9).
- **Fix:** Validate `req != nil && req.MemberInfo != nil && req.MemberInfo.ServiceAddr != ""` at the top of each membership handler; add a gRPC `recover` interceptor so handler panics return `codes.Internal` instead of crashing.

---

## HIGH (P1)

### H1 — Single global scheduler goroutine is the throughput ceiling — STATUS: FIXED
- **Location:** `scheduler/scheduler.go:114-127` (one `for/select`), `:138-140` (`PushTask` blocks when `chTasks` (cap 256) is full); default dispatch at `cluster/handler.go:688` (`scheduler.PushTask(task)`).
- **Evidence:** All local handlers funnel through one goroutine; a CPU-bound or blocking handler stalls everyone; producers block once the 256-slot queue fills.
- **Mitigation already in code:** per-service `SchedName` → `scheduler.LocalScheduler` sharding (`handler.go:672-685`).
- **Fix:** Document and require per-room/per-service `SchedName` schedulers for high CCU; separate timer processing from handler dispatch; never block or do I/O on the scheduler goroutine.

### H2 — Session state data races — STATUS: FIXED
- **Locations:**
  - `session/session.go:464-468` — `State()` returns the **live** `s.data` map; callers iterate/marshal it after the lock is released (e.g. `json.Marshal(session.State())` at `cluster/handler.go:504` per remote request) while `Set/Remove/Clear` mutate it → possible concurrent-map panic.
  - `session/session.go:143-145` — `ClientUid()` reads `s.clientUid` without a lock, racing the locked `SetClientUid` (`:148-152`).
  - `session/session.go:484` — `Clear()` does plain `s.uid = 0`, racing atomic `UID()` (`:129`).
- **Fix:** Return a shallow copy from `State()`; use lock or atomic for `ClientUid`; `atomic.StoreInt64(&s.uid, 0)` in `Clear`.

### H3 — Group broadcast holds the lock across fan-out and swallows errors — STATUS: FIXED
- **Location:** `group.go:198-208` (`Broadcast`); same pattern in `Multicast` and `BroadcastWithFallbackClosedSession`; `Close` resets `c.sessions` without the lock (~`group.go:349`).
- **Evidence:** `c.mu.RLock(); defer c.mu.RUnlock(); for _, s := range c.sessions { s.Push(...) }` serializes the entire broadcast under the read lock (blocks `Add/Leave/Close`), and `return err` returns only the **last** member's error.
- **Fix:** Snapshot session pointers under `RLock`, release, then push; aggregate errors; clear `sessions` under `Lock()` and re-check closed state in mutators.

### H4 — Cluster member-slice races + routing reads a live slice — STATUS: FIXED
- **Location:** `cluster/cluster.go` (~`:76-123`: `Register`/`Unregister`/`Heartbeat`/`pingNodes`/`checkMemberHeartbeat` access `c.members` with inconsistent locking); `cluster/handler.go:435-438` (`findMembers` returns `h.remoteServices[service]` after releasing the lock) consumed at `:481` (`members[rand.Intn(len(members))]`).
- **Evidence:** `NewMember`/`DelMember` mutate the same slices under the write lock while routing reads them lock-free → race on the backing array and incorrect/incomplete routing.
- **Fix:** Guard all `members`/`remoteServices` access with one mutex; have `findMembers` return a copy.

### H5 — Remote RPC from the client goroutine has no deadline — STATUS: FIXED
- **Location:** `cluster/handler.go:523` (`client.HandleRequest(context.Background(), ...)`), Notify branch `:529+`; `cluster/acceptor.go:36`.
- **Evidence:** `context.Background()` never times out. A slow/partitioned backend pins the client's read goroutine indefinitely and head-of-line blocks all later messages from that client.
- **Impact @ CCU:** One dead backend can pin thousands of goroutines.
- **Fix:** `context.WithTimeout` and surface timeout errors.

### H6 — Synchronous all-node session-close fan-out on disconnect — STATUS: FIXED
- **Location:** `cluster/handler.go:262-288`.
- **Evidence:** On disconnect the defer iterates **all** members calling `client.SessionClosed(context.Background(), ...)` synchronously **before** `agent.Close()`. A mass disconnect of 20k clients × N nodes = 20k·N synchronous RPCs; one slow node blocks local cleanup and retains agent/write goroutines.
- **Fix:** Call `agent.Close()` first; fan out close notifications asynchronously with a timeout/batch.

### H7 — `send()` blocks due to TOCTOU on `chSend` — STATUS: FIXED
- **Location:** `cluster/agent.go:102-110` (`send` does blocking `a.chSend <- m`), checked by `Push` (`:125-127`) and `ResponseMid` (`:182-184`).
- **Evidence:** The `len(a.chSend) >= agentWriteBacklog` check is not synchronized with the send; concurrent producers can all see space, one fills the buffer, the rest block the calling (scheduler/handler) goroutine — amplified by C4.
- **Fix:** `select { case a.chSend <- m: return nil; default: return ErrBufferExceed }`.

### H8 — Route-compression dictionaries are unsynchronized global maps — STATUS: FIXED
- **Location:** `internal/message/message.go:62-64` (globals), reads at `:128` (`Encode`) and `:202` (`Decode`), writes at `:243-244` (`SetDictionary`), live map returned at `:252` (`GetDictionary`).
- **Evidence:** Runtime dictionary update racing active connections hits Go's fatal concurrent map read/write. Code carries a TODO warning (`:228`).
- **Fix:** Build a replacement map and publish atomically (`atomic.Value`) or guard with `RWMutex`; return a copy from `GetDictionary`.
- **Verification (oracle):** confirmed, with precondition — the race requires the dictionary to be **mutated/read live after startup** (or the live map from `GetDictionary` to be mutated). Startup-only configuration is safe.

### H9 — Cluster gRPC server is unauthenticated and binds `0.0.0.0` — STATUS: FIXED
- **Location:** `cluster/node.go:579-592`.
- **Evidence:** `grpc.NewServer()` with no credentials/interceptor; registers member/master services exposing `Register/NewMember/DelMember/SessionClosed/CloseSession`. Any peer reaching the port can register fake backends or close other users' sessions.
- **Fix:** mTLS or an auth interceptor (shared token) between nodes; bind to an internal interface; document the network-isolation requirement.

### H10 — Buffer aliasing for `IsRawArg` handlers dispatched async → data corruption — STATUS: FIXED
- **Location:** `internal/codec/codec.go:92` (`c.buf.Next(c.size)` returns an aliased slice; TODO "shared slice" at `:71`); `cluster/handler.go:302` reuses `buf := make([]byte, 2048)`; `:600-601` sets `data = payload` for raw args; async dispatch at `:685`/`:688`.
- **Evidence:** Non-raw handlers `Unmarshal` synchronously (`:604`) and are safe. Raw-arg handlers capture the aliased payload in a task run later on the scheduler. *(verification correction)* The alias is to the **decoder's internal `bytes.Buffer`** (returned by `Next`), **not** the read buffer — `bytes.Buffer.Write` copies `buf[:n]` (`handler.go:302-313`). Corruption still occurs when the decoder buffer is reused/compacted on a later `Decode` while the raw payload is read asynchronously.
- **Fix:** Copy `msg.Data` before dispatch when `IsRawArg` (or always copy in the read loop before scheduling).

### H11 — No deadlines / connection cap → slowloris and FD exhaustion — STATUS: FIXED
- **Location:** `cluster/agent.go:302` (no write deadline before `a.conn.Write`); `cluster/ws.go:45-52` (first WS frame read with no deadline). *(verification correction)* An **optional per-IP cap exists** (`LimitConnectPerIp`, `cluster/node.go:73-75,753-768`); there is still **no global connection cap**.
- **Evidence:** A client that opens a connection and then stalls (no read, or no first frame) pins a goroutine/FD outside the heartbeat path.
- **Fix:** Set/refresh read and write deadlines (tied to heartbeat), enforce a max-connections cap, close on timeout.

### H12 — `pipeline.Process` holds the read lock while running handlers — STATUS: FIXED
- **Location:** `pipeline/pipeline.go:66-72`.
- **Evidence:** `p.mu.RLock()` is held across the `for _, h := range p.handlers { h(s, msg) }` loop. A slow handler blocks `PushBack`; a handler that calls `PushBack` deadlocks (RLock→Lock).
- **Fix:** Copy the handler slice under the lock, release, then iterate the snapshot.

### H13 — Local sessions are never removed on disconnect → unbounded session leak *(found in verification)* — STATUS: FIXED
- **Location:** `cluster/handler.go:234` (`h.currentNode.storeSession(agent.session)` on connect); disconnect defer `cluster/handler.go:262-299` never deletes from `n.sessions`. The only deletes are remote RPC `SessionClosed` (`cluster/node.go:872-887`) and SSE unregister (`cluster/node.go:482-494`).
- **Evidence:** Verified directly — the read-loop defer does `decreaseConnection`, fan-out `SessionClosed`, `agent.Close()`, metrics; there is no `delete(n.sessions, …)`. Direct TCP/WS clients on a gate leak their `n.sessions` entry (and its network entity) on every disconnect.
- **Impact @ CCU:** `n.sessions` grows unbounded under connect/disconnect churn → memory leak + stale entities; at 20k CCU with reconnect churn this is a steady leak.
- **Fix:** Delete the local session from `n.sessions` in the disconnect defer (single owner, under `n.mu`).

### H14 — `sseClients` map read without lock → concurrent-map fatal *(found in verification)* — STATUS: FIXED
- **Location:** read `cluster/node.go:248` (`ch := n.sseClients[string(sid)]`, no `n.mu`); writes under lock at `registerSSEClient` (`:475-479`) and `unregisterSSEClient` (`:482-494`).
- **Evidence:** Verified directly — the SSE request handler indexes the map lock-free while register/unregister mutate it under `n.mu`. Concurrent HTTP/SSE requests trigger Go's fatal concurrent map read/write.
- **Impact @ CCU:** Remotely reachable process crash under concurrent SSE traffic.
- **Fix:** Read `n.sseClients` under `n.mu.RLock()` (or switch to `sync.Map`).

### H15 — `connectionCount` read before lock in `decreaseConnection` → map race *(found in verification)* — STATUS: FIXED
- **Location:** `cluster/node.go:777-779` reads `n.connectionCount[ipAddress]` **before** acquiring `n.mu.Lock()` at `:780`; `increaseConnection` mutates it under lock (`:756-761`).
- **Evidence:** Verified directly — the `if _, ok :=` / `if … > 0` checks run outside the lock, racing concurrent connects/disconnects when `LimitConnectPerIp > 0`.
- **Impact @ CCU:** Data race / possible fatal concurrent map access on connection-limited deployments.
- **Fix:** Acquire `n.mu.Lock()` before the existence/value checks.

### H16 — `fasthttp.RequestCtx` mutated from non-owner goroutines *(round 2)* — STATUS: FIXED
- **Location:** stored live at `cluster/http_agent.go:60-64` (`AttackHttpRequestCtx`); mutated at `cluster/http_agent.go:189-211` (`ResponseMid`: `SetContentType`/`SetBody`/`SetStatusCode`) from scheduler/RPC goroutines (e.g. `cluster/node.go:821-826`).
- **Evidence:** fasthttp documents `RequestCtx` as unsafe to touch from other goroutines, yet local handlers (via `scheduler.PushTask`) and remote gRPC responses write the stored ctx.
- **Impact @ CCU:** Data races / corrupted HTTP responses under concurrent `/api` load.
- **Fix:** Publish serialized bytes+status to a per-request channel; let the original `/api` handler goroutine write the ctx.

### H17 — `httpAgent.Close` races in-flight requests → nil-map/nil-deref panic *(round 2)* — STATUS: FIXED
- **Location:** `cluster/http_agent.go:98-104` nils `session`/`sseChan`/`messageIDMapToRequest` without `h.mu`, racing the locked attach (`:60-64`) and `ResponseMid`'s `h.session.ID()` (`:206`).
- **Evidence:** SSE teardown (`cluster/node.go:491`) calls `ne.Close()` while a concurrent `/api` request may assign into the now-nil map or deref the nil session.
- **Impact @ CCU:** Disconnect/request race → panic (no recover on these paths) under concurrent SSE+API traffic.
- **Fix:** Make `Close` idempotent + synchronized; mark closed, reject new attaches, keep request state alive until in-flight responses drain.

### H18 — Client-controlled SSE session IDs overwrite/fixate sessions *(round 2)* — STATUS: FIXED
- **Location:** `cluster/node.go:385-395` accepts `X-SSE-SessionID`/`sse_sessionID`/cookie; `:459-469` stores it; `storeSession` overwrites `n.sessions[s.ID()]` (`:691-694`). Default `MiddlewareHttp` is allow-all (`internal/env/env.go:62-63`).
- **Evidence:** A client supplying an existing numeric session ID replaces that session's network entity with an HTTP/SSE agent before any app auth.
- **Impact @ CCU:** Session fixation/hijack + routing corruption; magnitude depends on whether IDs leak via normal protocols.
- **Fix:** Only use server-issued/authenticated SSE IDs; reject collisions with non-SSE sessions under `n.mu`.

### H19 — Stale SSE unregister closes the replacement live stream *(round 2)* — STATUS: FIXED
- **Location:** `cluster/node.go:428-430` defers `unregisterSSEClient(sessionID)`; `:475-479` overwrites the channel for a reused ID; `:489-494` closes session + deletes map entry without checking it is the same stream.
- **Evidence:** Reconnect / second tab with the same cookie → the old stream's defer deletes the new channel and closes the live session.
- **Impact @ CCU:** Dropped pushes + `/api` auth failures for the live client on every reconnect.
- **Fix:** Pass the channel/token into unregister; delete/close only if the map still points to that exact stream.

### H20 — Local HTTP responses never complete + can block on an unconsumed channel *(round 2)* — STATUS: FIXED
- **Location:** local notify falls into the 10s observe loop instead of returning 200 (`cluster/node.go:332` → `:337-348`); the `/api` `responseChan` is created unbuffered and not read (`cluster/node.go:270-271`) while `localProcess` can send to it (`cluster/handler.go:648-659`).
- **Evidence:** Local `/api` notify holds a fasthttp goroutine for 10s then 408s; a local handler that returns a response blocks the scheduler/local-scheduler task on the send to the unconsumed channel.
- **Impact @ CCU:** Request-goroutine pinning (10s each) + scheduler-task blocking → throughput collapse / stalled dispatch under HTTP load.
- **Fix:** Return 200 immediately after dispatching a local notify; give `responseChan` a reader (or buffer) and complete on the request goroutine.

### H21 — `Shutdown` cannot stop the fasthttp client listener *(round 2)* — STATUS: FIXED
- **Location:** `listenAndServe` creates a local `server := &fasthttp.Server` (`cluster/node.go:157`) and `ListenAndServe(n.ClientAddr)` (`:175`); `Shutdown` checks the never-assigned `n.httpServer` (`:681-685`).
- **Evidence:** The client listener is owned by a local var, so `Shutdown` cannot stop it.
- **Impact @ CCU:** Restart leaves the public client port bound and still accepting; FD/goroutine leak.
- **Fix:** Store the fasthttp server on `Node`; call its `Shutdown`/`Close` and wait for the serve goroutine.

### H22 — `log.Fatalf` in the gRPC serve goroutine exits the process *(round 2; verified directly)* — STATUS: FIXED
- **Location:** `cluster/node.go:594-598` — `go func(){ if err := n.server.Serve(listener); err != nil { log.Fatalf(...) } }()`.
- **Evidence:** Any post-startup `Serve` error calls `log.Fatalf` (→ `os.Exit`) from a background goroutine, bypassing unregister/component shutdown.
- **Impact @ CCU:** A transient listener error takes down the whole node and all clients.
- **Fix:** Log/propagate the error to a coordinated shutdown; never `Fatal` from server goroutines.

### H23 — Lifecycle RPCs to master use uncancellable `context.Background()` *(round 2)* — STATUS: FIXED
- **Location:** unbounded retry `client.Register(context.Background(), …)` (`cluster/node.go:626-634`); `Unregister` (`:669`); `Heartbeat` (`:918`).
- **Evidence:** No deadline + no cancellation; a stalled master hangs startup or shutdown indefinitely.
- **Impact @ CCU:** Node cannot start/stop cleanly during a master partition; hung deploys/restarts.
- **Fix:** Deadlines tied to node lifecycle + bounded/backoff retries that abort on shutdown.

### H24 — Departing gate's acceptor sessions are never purged *(round 2)* — STATUS: FIXED
- **Location:** acceptor sessions store `gateAddr` (`cluster/node.go:717-734`); `DelMember` only updates routing (`:863-866`) and never scans `n.sessions` for that gate.
- **Evidence:** If a gate disappears before sending per-session `SessionClosed`, all its remote sessions stay in the service-node map forever.
- **Impact @ CCU:** Unbounded remote-session leak on gate crash/rollout.
- **Fix:** On member removal, delete sessions whose entity is an `*acceptor` with the departed `gateAddr` and run the lifetime-close path.

### H25 — Master register/unregister abort before mutating local state on peer-fanout failure *(round 2)* — STATUS: FIXED
- **Location:** `Register` returns on first peer `NewMember` error (`cluster/cluster.go:82-90`) before local add (`:95-99`); `Unregister` returns on first `DelMember` error (`:132-139`) before local removal (`:159-167`).
- **Evidence:** One stale/unreachable peer blocks all new joins; one unreachable peer blocks member removal → stale routable addrs + heartbeat retry loop; partial fanout leaves peers inconsistent with the master.
- **Impact @ CCU:** Cluster cannot add/shed capacity when any single peer is unhealthy — directly limits scale-out.
- **Fix:** Mutate master/local registry first (or via guaranteed defer); make peer fanout best-effort/aggregated with cleanup of failed peers.

### H26 — `HandshakeAck` bypasses `HandshakeValidator` *(round 2)* — STATUS: FIXED
- **Location:** `cluster/handler.go:403-404` sets `statusWorking` with no check that a prior `Handshake` passed the validator (`:387-391`); `Data` only checks working status (`:415-425`).
- **Evidence:** A client sending `HandshakeAck` as its first packet reaches `statusWorking` and can send Data without ever passing `env.HandshakeValidator`; pre-handshake heartbeats also refresh `lastAt` (`:431`).
- **Impact @ CCU:** Auth/handshake bypass; unauthenticated connections kept alive.
- **Fix:** Enforce Start→Handshake(validated)→HandshakeAck→Working transitions; reject out-of-state packets.

### H27 — Failed first WS read leaks the upgraded connection *(round 2)* — STATUS: FIXED
- **Location:** `cluster/handler.go:574-578` — `handleWS` returns when `newWSConn` fails without closing the upgraded `*websocket.Conn`; `newWSConn` can fail after `conn.NextReader()` (`cluster/ws.go:44-56`).
- **Evidence:** The upgrader hands FD ownership to the handler; on failure no agent defer runs → server-side FD leak.
- **Impact @ CCU:** Clients that upgrade then send a bad first frame leak FDs (slowloris-style).
- **Fix:** `_ = conn.Close()` on the error path (or close inside `newWSConn` after ownership).

### H28 — Unbounded Prometheus `route` label cardinality → memory DoS *(round 3)* — STATUS: FIXED
- **Location:** `metrics/metrics.go:35-42` (`RouteRequestDuration` label `route`); `msg.Route` decoded from client bytes (`internal/message/message.go:210-215`); observed at `cluster/handler.go:544-546,646`; clustered routing validates only the service prefix (`:447-455`).
- **Evidence:** A client varies the method suffix under a known service → each distinct route becomes a retained histogram series; no `DeleteLabelValues`/normalization.
- **Impact @ CCU:** Attacker-driven unbounded registry growth (memory) + scrape blowup — remotely triggerable DoS.
- **Fix:** Observe only bounded labels (registered route ID / service / status); reject or normalize unknown routes before `WithLabelValues`.

### H29 — Per-IP gauge labels never deleted → registry growth on IP churn *(round 3)* — STATUS: FIXED
- **Location:** `metrics/metrics.go:44-49` (`ConnectionsPerIP` label `ip`); `cluster/node.go:753-775` only `Inc`/`Dec` `WithLabelValues(ipAddress)`; IP from peer addr (`cluster/agent.go:239-250`).
- **Evidence:** GaugeVec children persist after returning to zero; no `DeleteLabelValues`. IP churn / botnet permanently grows series count.
- **Impact @ CCU:** Slow memory DoS via reconnect churn from many IPs.
- **Fix:** Drop the raw-IP label (or bucket it); delete the label when the per-IP active count hits zero.

### H30 — `OpenPrometheus` exposes `/metrics` on the client listener + global mux and can crash the process *(round 4; verified directly)* — STATUS: FIXED
- **Location:** `cluster/node.go:139-147` — `http.Handle("/metrics", promhttp.Handler())` + `http.ListenAndServe(":2112", nil)` + `log.Fatalf` on error; client fallback routes unmatched requests to `http.DefaultServeMux` (`cluster/node.go:193-196`).
- **Evidence:** Verified directly — `/metrics` is registered on the global `DefaultServeMux`, so it is also reachable via the public client listener (`fasthttpadaptor.NewFastHTTPHandler(http.DefaultServeMux)`). `http.Handle` panics on a duplicate pattern (second node in-process / restart), the `:2112` bind error calls `log.Fatalf` (→ process exit) in an unrecovered goroutine, and the metrics server is never tracked for shutdown.
- **Impact @ CCU:** Internal metrics leaked to all clients; a port-bind failure or duplicate registration crashes the whole node.
- **Fix:** Use a private `http.ServeMux` + a `Node`-owned `http.Server` on a configurable address; return bind errors instead of `Fatalf`; never route the public client fallback through `DefaultServeMux`.

### H31 — Client listener bind failure is swallowed; `Startup` reports success *(round 6; verified directly)* — STATUS: FIXED
- **Location:** `cluster/node.go:132-135` (`go n.listenAndServe()`); the bind happens inside the goroutine and only logs (`cluster/node.go:175-177`, `if err := server.ListenAndServe(n.ClientAddr); err != nil { log.Println(...) }`); callers only see `node.Startup()` errors (`interface.go:96-99`).
- **Evidence:** Verified directly — an invalid/in-use `ClientAddr` leaves the node initialized/registered with no client listener serving and no error returned. Distinct from H21 (shutdown can't stop the listener).
- **Impact @ CCU:** A node silently joins the cluster while accepting no client traffic — undetected outage / split brain.
- **Fix:** Bind the client listener synchronously in `Startup` and return the listen error; run the serving loop in a goroutine only after a successful bind; retain the server for shutdown (ties into H21).

### H32 — SSE `Push` silently drops events while returning success *(round 7)* — STATUS: FIXED
- **Location:** `cluster/http_agent.go:150-159` — `select { case h.sseChan <- data: …; default: <log drop> }` then `return nil`; SSE channel is a fixed 256-slot buffer (`cluster/node.go:413-414`). Compare `agent.Push` which returns `ErrBufferExceed` when full (`cluster/agent.go:119-140`); `Session.Push` propagates the entity error (`session/session.go:106-108`).
- **Evidence:** On a slow SSE client / bursty fan-out, the channel fills and the event is dropped, but the application sees `Push` succeed → silent data loss (inconsistent with TCP/WS semantics). Distinct from C5/H16/H17/H20.
- **Impact @ CCU:** Undetected message loss for SSE clients under load.
- **Fix:** Return an explicit error when `sseChan` is full/closed (align with `agent.Push`), or apply a defined blocking/close policy.

### H33 — HTTP message IDs / SSE session IDs use wall-clock nanoseconds → collisions *(round 8)* — STATUS: FIXED
- **Location:** `/api` `messageID := uint64(time.Now().UnixNano())` (`cluster/node.go:277`) keyed into `messageIDMapToRequest` (`cluster/http_agent.go:63-66`); new SSE id `fmt.Sprintf("%d", time.Now().UnixNano())` (`cluster/node.go:497-498`) keyed into `sseClients`/`n.sessions` (`:475-479,691-694`).
- **Evidence:** Wall-clock time is not a unique counter; concurrent calls observing the same timestamp (or a non-monotonic clock) collide. An `/api` collision overwrites another in-flight request's context (and a later defer deletes the wrong entry); an SSE collision overwrites a session/channel mapping.
- **Impact @ CCU:** Misrouted/dropped HTTP responses and clobbered SSE sessions under concurrency.
- **Fix:** Use a per-agent atomic counter for message IDs and the snowflake/crypto-random generator for SSE session IDs.

### H34 — Shared `httpAgent.lastMid` cross-wires concurrent `/api` responses *(round 8)* — STATUS: PARTIAL
- **Location:** `cluster/http_agent.go:184-185` (`Response` → `ResponseMid(h.lastMid, v)`); `h.lastMid` set per scheduled handler task (`cluster/handler.go:620-630`). Concurrent `/api` requests for one SSE session share one `httpAgent`.
- **Evidence:** A later request overwrites `lastMid` before an earlier handler calls `session.Response`, so the response goes to the wrong request / is dropped after timeout. Distinct from H20 (hits the framework `Session.Response` path, not just `responseChan`). The same shared-`lastMid` pattern exists in core `agent`/`acceptor` (`cluster/agent.go:166-172`, `cluster/acceptor.go:77-82`) but is serialized per-connection there.
- **Impact @ CCU:** Wrong-recipient / lost HTTP responses under concurrent requests per SSE session.
- **Fix:** Bind the message ID to a per-request responder; require `ResponseMID` with the request-scoped ID instead of mutable shared `lastMid`.

### H35 — Stalled SSE writers pin sessions; fasthttp server has no read/write timeouts *(round 8)* — STATUS: FIXED
- **Location:** SSE loop uses `case <-ctx.Done()` (`cluster/node.go:436-438`) + `w.Flush()` (`:442-449`); unregister only in the defer (`:427-431`). The fasthttp server sets only `Handler` (`cluster/node.go:157-173`); fasthttp `RequestCtx.Done()` closes only on server shutdown, and read/write timeouts default to unlimited.
- **Evidence:** Normal client disconnect isn't reliably signaled by `ctx.Done()`, and a stalled client blocks in `Flush()` with no write deadline → the unregister defer never runs → leaked SSE channel/session/goroutine. Different root cause from M31 (remote-close path).
- **Impact @ CCU:** Slow/stalled SSE clients pin goroutines + FDs (slowloris); complements H11 (TCP/WS deadlines).
- **Fix:** Set bounded `ReadTimeout`/`WriteTimeout`/`IdleTimeout` on the fasthttp server; detect write failure/cancellation reliably; run unregister independent of a blocked flush.

---

## MEDIUM

### M1 — Encode side lacks size validation — STATUS: FIXED
- **Location:** `internal/codec/codec.go:190` (`make([]byte, p.Length+HeadLength)` before any cap); `intToBytes` 3-byte length wraps for payloads `> 0xFFFFFF`; `internal/message/message.go:154` writes `byte(len(m.Route))` and appends the full route even when `len(route) > 255`.
- **Fix:** Check `len(data) <= MaxPacketSize` before `make` and return `ErrPacketSizeExcced`; reject routes longer than `msgRouteLengthMask`.

### M2 — Message-id varint is unbounded / unterminated — STATUS: FIXED
- **Location:** `internal/message/message.go:183-191`.
- **Evidence:** No 10-byte cap and no terminator check; all-continuation bytes leave `offset` at the id start and the remainder is parsed as route/data; over-wide ids wrap.
- **Fix:** Cap at 10 bytes, require a terminator, reject overflow; return `ErrWrongMessage`.

### M3 — Connection pools leak when members depart — STATUS: FIXED
- **Location:** `cluster/connpool.go` (~`:123`), `createConnPool` stores per-address pools in `c.pools`; `closePool` is never called on member removal.
- **Evidence:** Departed/recreated node addresses keep their gRPC `ClientConn`s and map entries for the process lifetime → FD/memory leak under cluster churn (e.g. k8s rollouts).
- **Fix:** Close and delete the pool when a member/address is removed.

### M4 — Timer races and condition-timer registration bug — STATUS: FIXED
- **Location:** `scheduler/timer.go:89` (`Stop` writes `t.counter = 0` unsynchronized vs `cron`); `:104` (`cron` reads `len(createdTimer)` before taking `muCreatedTimer`); `:213-214` (`NewCondTimer` sets `t.condition` *after* registration and uses a `MaxInt64` interval → `createAt + MaxInt64` overflow / early fire).
- **Fix:** Make stop/registration scheduler-owned or fully locked; set `condition` before registering; avoid the `MaxInt64` sentinel.

### M5 — Lifetime callback slice is unsynchronized — STATUS: FIXED
- **Location:** `session/lifetime.go:19` (`OnClosed` appends while `Close` ranges the slice without a lock).
- **Fix:** Guard with a mutex and snapshot before invoking, or enforce startup-only registration.
- **Verification (oracle):** confirmed as a race **only if callbacks are registered at runtime**; startup-only registration is safe but the code neither documents nor enforces that contract.

### M6 — Per-message allocation pressure — STATUS: FIXED
- **Location:** `internal/message/message.go:125` (`make([]byte, 0)` then grow + copy, plus a second copy in `codec.Encode`); `cluster/handler.go:504` (`json.Marshal(session.State())` per remote request).
- **Fix:** Preallocate `make([]byte, 0, headerLen+len(m.Data))`; avoid re-marshaling session state per request.

### M7 — Remote dispatch failures are not propagated *(round 2)* — STATUS: FIXED
- **Location:** `remoteProcess` silently returns on invalid route/no member/pool error (`cluster/handler.go:448-488`); `agent.RPC` (`cluster/agent.go:143-161`) and `acceptor.RPC` (`cluster/acceptor.go:40-53`) return nil regardless.
- **Impact @ CCU:** `/api` callers wait for the 10s timeout (H20); `RPC` callers see false success — silent failures hide outages.
- **Fix:** Return an error/handled flag from `remoteProcess` and propagate from `RPC`/HTTP paths.

### M8 — gRPC client pools not closed on `Shutdown` *(round 2)* — STATUS: FIXED
- **Location:** `closePool` (`cluster/connpool.go:134-144`) has no callers; `Node.Shutdown` (`cluster/node.go:677-688`) stops only the gRPC/HTTP servers.
- **Impact @ CCU:** Outbound `ClientConn`s + goroutines leak on in-process shutdown/restart (distinct from M3's per-member case).
- **Fix:** Call `rpcClient.closePool()` from `Shutdown` after unregister/keepalive stop.

### M9 — Service node trusts RPC-supplied `GateAddr` for dialing + session creation *(round 2; trusted-net caveat)* — STATUS: FIXED
- **Location:** `cluster/node.go:803` passes `req.GateAddr` to `findOrCreateSession`, which `getConnPool(gateAddr)` + stores an acceptor (`:713-734`).
- **Impact @ CCU:** A member RPC can make a node dial an arbitrary address + forge sessions (SSRF/resource exhaustion). MEDIUM under the trusted inter-node threat model.
- **Fix:** Validate `GateAddr` against registered members / the RPC peer before dialing/storing.

### M10 — `findOrCreateSession` can create duplicate sessions per sid *(round 2)* — STATUS: FIXED
- **Location:** `cluster/node.go:706-708` reads under `RLock`, builds the session after unlock, stores under `Lock` (`:713-734`) with no recheck.
- **Impact @ CCU:** Concurrent first-seen RPCs for one sid create two `Session` objects with split state/router/lifetime.
- **Fix:** Double-check under the write lock (or a per-sid singleflight/placeholder).

### M11 — Session-close RPCs lack owner authorization *(round 2; trusted-net caveat)* — STATUS: PARTIAL
- **Location:** `SessionClosed`/`CloseSession` delete/close by `req.SessionId` with no gate/member ownership check (`cluster/node.go:871-889`).
- **Impact @ CCU:** Any caller able to issue member RPCs can close arbitrary client sessions. MEDIUM under trusted inter-node network (distinct from H9 transport auth).
- **Fix:** Derive caller identity, compare with the stored session's owning gate, reject mismatches.

### M12 — Remote service routing entries duplicate/stale on rejoin *(round 2)* — STATUS: FIXED
- **Location:** `addRemoteService` always appends (`cluster/handler.go:168-175`); `cluster.addMember` dedupes only `c.members` (`cluster/cluster.go:309-324`); re-register doesn't purge `remoteServices` (`:61-73,96`).
- **Impact @ CCU:** Node restart/deploy leaves stale routes + grows duplicate routing entries (unbounded; mis-routes to dropped services).
- **Fix:** Replace entries by `ServiceAddr` (purge before add), or make `addRemoteService` idempotent.

### M13 — Master heartbeat-checker goroutine leaks on shutdown *(round 2)* — STATUS: FIXED
- **Location:** `checkMemberHeartbeat` ticker loop only exits on `!IsMaster` (`cluster/cluster.go:220-228`); `Node.Shutdown` doesn't flip/stop it (`cluster/node.go:677-688`).
- **Impact @ CCU:** Start/stop cycles leak a ticker goroutine that keeps mutating old cluster state.
- **Fix:** Add a stop channel/context selected in the loop and closed from `Shutdown`.

### M14 — Master accepts empty-`ServiceAddr` registrations that cannot be unregistered *(round 2)* — STATUS: FIXED
- **Location:** `Register` only nil-checks `MemberInfo` (`cluster/cluster.go:55-58`) then appends (`:95-99`); `Unregister` rejects empty `ServiceAddr` (`:103-107`).
- **Impact @ CCU:** A malformed registration poisons the registry/fanout with an unremovable entry.
- **Fix:** Validate non-empty `ServiceAddr` (and ≥1 service for non-master) before mutating state.

### M15 — Conn-pool caches unverified dials + holds the global lock while dialing *(round 2)* — STATUS: FIXED
- **Location:** `newConnArray` dials without `grpc.WithBlock` (`cluster/connpool.go:59-64`; opts = `WithInsecure` only, `internal/env/env.go:54`); `createConnPool` holds the global write lock during `newConnArray(10, addr)` (`:118-126`).
- **Impact @ CCU:** Bad addresses cached as healthy (failure deferred to RPCs); one slow address stalls all concurrent `getConnPool` (routing/heartbeat/session-close).
- **Fix:** Build the pool outside the lock (per-address singleflight) and verify connectivity before caching.

### M16 — Session-id frame is not a valid nano message *(round 2)* — STATUS: FIXED
- **Location:** `encodeSessionId` wraps raw JSON in `codec.Encode(packet.Data, …)` (`cluster/handler.go:100-110`); clients decode Data via `message.Decode`, which expects a message-flag byte and rejects invalid types (`internal/message/message.go:165-176`).
- **Impact @ CCU:** Clients cannot parse the sid push emitted on every HandshakeAck (correctness).
- **Fix:** Carry sid in the handshake response, or encode it as a proper Push/Response message before the packet frame.

### M17 — IPv6 addresses parsed incorrectly for per-IP accounting *(round 2)* — STATUS: FIXED
- **Location:** `RemoteAddrWithoutPortStr` uses `strings.Split(addr, ":")[0]` (`cluster/agent.go:239-250`); drives per-IP limit + `ConnectionsPerIP` (`cluster/handler.go:236-242`, `cluster/node.go:753-755`).
- **Impact @ CCU:** `[2001:db8::1]:1234` → `[2001`; IPv6 clients grouped together → wrong limiting/metrics.
- **Fix:** Use `net.SplitHostPort` and normalize the host.

### M18 — Per-IP-limit rejection returns before connection cleanup *(round 2)* — STATUS: FIXED
- **Location:** `handle` stores the session (`cluster/handler.go:234`) then `increaseConnection` (`:242`); on rejection it returns before the cleanup defer/`agent.Close` (`:261-299`).
- **Impact @ CCU:** A rejected over-limit connection leaks the upgraded connection path + the H13-style session entry.
- **Fix:** Register the cleanup defer before the limit check, or close/clean on the rejection path.

### M19 — `Shutdown` is not idempotent (closes one-shot `env.Die`) *(round 3)* — STATUS: FIXED
- **Location:** `interface.go:130-131` (`close(env.Die)`); `env.Die` created once (`internal/env/env.go:57-58`).
- **Impact @ CCU:** A second `Shutdown` panics (close-of-closed); a later in-process `Listen` immediately selects the closed channel and tears down the new node.
- **Fix:** Guard the close with `sync.Once` / a per-run context; recreate the channel before a new `Listen`.

### M20 — Invalid heartbeat option crashes ticker goroutines *(round 3; config-time)* — STATUS: FIXED
- **Location:** `options.go:77-80` stores any `d` into `env.Heartbeat`; `time.NewTicker(env.Heartbeat)` at `cluster/agent.go:274-275`, `cluster/cluster.go:219-220`, `cluster/node.go:928-929`.
- **Impact @ CCU:** `WithHeartbeatInterval(0)`/negative → `time.NewTicker` panics in connection/heartbeat goroutines (no recover). Operator misconfiguration.
- **Fix:** Reject `d <= 0` (or treat 0 as default) at option application.

### M21 — Nil serializer option panics on first non-raw request *(round 3; config-time)* — STATUS: FIXED
- **Location:** `options.go:124-127` (`env.Serializer = serializer`, no nil check); used at `cluster/handler.go:598-604`.
- **Impact @ CCU:** `WithSerializer(nil)` starts fine, then the first non-raw handler nil-derefs and drops all clients.
- **Fix:** Reject nil or keep the default serializer.

### M22 — Nil handshake validator option panics on handshake *(round 3; config-time)* — STATUS: FIXED
- **Location:** `options.go:165-169` (`env.HandshakeValidator = fn`); called unconditionally at `cluster/handler.go:385-391`.
- **Impact @ CCU:** `WithHandshakeValidator(nil)` → nil func call in the read goroutine on first handshake.
- **Fix:** Reject nil or restore the default no-op validator.

### M23 — Nil CheckOrigin option panics on WS upgrade *(round 3; config-time)* — STATUS: FIXED
- **Location:** `options.go:83-87` (`env.CheckOrigin = fn`); called at `cluster/node.go:503-520` (`ok := env.CheckOrigin(r)`).
- **Impact @ CCU:** `WithCheckOriginFunc(nil)` → nil func call on first WebSocket upgrade.
- **Fix:** Reject nil or restore the default origin checker.

### M24 — `runtime.CurrentNode` published unsynchronized and after listener start *(round 3)* — STATUS: FIXED
- **Location:** plain global (`internal/runtime/runtime.go:23-25`), written at `interface.go:100,124`, read by `PingNodes` (`interface.go:135-140`); `Startup` starts the listener before publication (`cluster/node.go:132-135`).
- **Impact @ CCU:** `PingNodes` races startup/shutdown writes (data race); early reads see nil ("Node is not initialized").
- **Fix:** Publish before starting listeners; store in `atomic.Pointer[cluster.Node]` (or mutex) for all access.

### M25 — `service.ResetNodeId` races global `Connections` readers *(round 3; runtime-only)* — STATUS: FIXED
- **Location:** `service/connection.go:36-42` replaces the global `Connections` interface; read at `session/session.go:68-80` (`service.Connections.SessionID()`).
- **Impact @ CCU:** A runtime `ResetNodeId` races session creation on the multiword interface value and resets the ID generator → possible duplicate session IDs. (`WithNodeId` at option time is safe.)
- **Fix:** Guard behind a mutex/`atomic.Value`, or make node-id immutable after startup.

### M26 — `Components.List()` exposes the live backing slice *(round 3)* — STATUS: FIXED
- **Location:** `component/hub.go:32-39` (`Register` appends; `List` returns `cs.comps`); iterated at startup/shutdown (`cluster/node.go:111-128,642-652`).
- **Impact @ CCU:** Post-start `Register` or mutation of a returned slice races shutdown iteration → skipped/orphaned lifecycle hooks.
- **Fix:** Return a copy from `List`; freeze/reject `Register` after startup (or guard with a mutex).

### M27 — Outbound `[]byte` payloads alias caller buffers until the writer serializes *(round 3)* — STATUS: FIXED
- **Location:** `internal/message/util.go:25-27` (`Serialize` returns the caller `[]byte` unchanged); `Push`/`Response` enqueue `payload: v` (`cluster/agent.go:118-140,172-197`); serialized later on the write goroutine (`:307-308`).
- **Impact @ CCU:** A handler that reuses/pools a buffer after `Push`/`Response` returns races the writer → corrupted outbound payload. (Distinct from H10 inbound raw-arg aliasing.)
- **Fix:** Copy `[]byte` at the Push/Response enqueue boundary (or serialize before enqueueing).

### M28 — Per-IP `connectionCount` map retains zero-count keys *(round 3; found in verification)* — STATUS: FIXED
- **Location:** `cluster/node.go:753-761` creates `n.connectionCount[ipAddress]`; disconnect only decrements (`:777-785`), never deletes zero-count keys (when `LimitConnectPerIp > 0`).
- **Impact @ CCU:** IP churn grows the map unboundedly (distinct from M18's rejected-connection cleanup).
- **Fix:** `delete(n.connectionCount, ip)` when the count reaches zero (under `n.mu`).

### M29 — `Group.Add` can succeed after `Close` and repopulate a closed group *(round 4)* — STATUS: FIXED
- **Location:** `group.go:254-257` checks `c.isClosed()` *before* taking `c.mu`; `:263-274` inserts under `c.mu` with no closed recheck; `Close` clears `c.sessions` + sets closed (`:341-349`).
- **Evidence:** An `Add` that passes the pre-lock `isClosed()` check, then blocks on `c.mu` while `Close` runs, resumes and inserts into the now-closed/cleared group, returning nil instead of `ErrClosedGroup`.
- **Impact @ CCU:** A closed group can retain live sessions during shutdown/reset churn (leak + contract violation). Distinct from H3 (broadcast lock / unlocked Close).
- **Fix:** Guard status + `sessions` with one mutex; re-check closed state inside `Add` after acquiring `c.mu`.

### M30 — TLS / WebSocket mode options are dead or bypassed *(round 6)* — STATUS: FIXED
- **Location:** `options.go:143-147` (`WithIsWebsocket` sets `opt.IsWebsocket`), `options.go:150-155` (`WithTSLConfig` sets cert/key); `Startup` always starts plain `go n.listenAndServe()` (`cluster/node.go:132-135`); `handleFastHTTP` routes `env.WSPath` unconditionally (`cluster/node.go:183-192`); `listenAndServeWSTLS` (`cluster/node.go:538-557`) has no caller.
- **Evidence:** TLS cert/key are silently ignored (WSS never served by the TLS handler); `IsWebsocket` does not gate WS routing — both public options don't do what their comments advertise.
- **Impact @ CCU:** Operators believe TLS/WSS is configured when it is not (security/availability surprise).
- **Fix:** Wire startup mode explicitly (start the TLS path when cert/key are set; honor `IsWebsocket`), or remove the dead options.

### M31 — Remote `CloseSession` leaves the SSE stream/channel registered *(round 7)* — STATUS: FIXED
- **Location:** `cluster/node.go:883-890` (`CloseSession` deletes `n.sessions[sid]` + `s.Close()`); `httpAgent.Close` only nils fields (`cluster/http_agent.go:98-105`); SSE unregister only happens in the stream-writer defer (`cluster/node.go:427-431,482-494`).
- **Evidence:** A backend-initiated close removes the session but leaves the live SSE response goroutine and a stale `sseClients[sid]` entry. Later `/api` calls pass the `sseClients` check (`:248-252`) yet find no session (`:264-272`) → inconsistent HTTP-agent state. Distinct from M11 (authz) and H24 (acceptor purge).
- **Impact @ CCU:** SSE goroutine/entry leak + inconsistent state on backend-initiated closes.
- **Fix:** Centralize HTTP/SSE teardown so `CloseSession` and `unregisterSSEClient` both delete the session + SSE entry under one lock and close/signal the stream idempotently.

### M32 — Negative `WithAdvertiseAddr` retry interval disables registration backoff *(round 7)* — STATUS: FIXED
- **Location:** `options.go:37-42` (`WithAdvertiseAddr` stores `retryInterval[0]` unvalidated); `Listen` only replaces it when exactly zero (`interface.go:87-90`); the register loop sleeps `n.RetryInterval` after each failure (`cluster/node.go:626-634`).
- **Evidence:** A negative interval survives option processing, so the failed-register loop has effectively no delay → tight retry/log storm during master outage or misconfiguration. Distinct from M20 (heartbeat ticker panic).
- **Impact @ CCU:** CPU/log storm hammering the master on registration failure.
- **Fix:** Validate `retryInterval > 0` (or normalize `<= 0` to default) before `initNode`.

### M33 — Nil `Option` panics during option application *(round 8; config-time)* — STATUS: FIXED
- **Location:** `interface.go:77-78` applies every option `option(&opt)` with no nil guard (`Option` = `func(*cluster.Options)`, `options.go:20`).
- **Impact:** A nil `Option` passed to `Listen` panics at startup before validation.
- **Fix:** Skip/reject nil options before invocation.

### M34 — `WithGrpcOptions(nil)` stores a nil dial option that panics on dial *(round 8; config-time)* — STATUS: FIXED
- **Location:** `options.go:63-66` appends variadic `grpc.DialOption` into `env.GrpcOptions` unvalidated; passed to `grpc.DialContext` (`cluster/connpool.go:60-63`), which calls `opt.apply(...)` per element.
- **Impact:** `WithGrpcOptions(nil)` is retained globally and panics on the next cluster dial.
- **Fix:** Reject nil elements before appending.

### M35 — HTTP/SSE JSON framing is mixed with the global binary serializer *(round 8)* — STATUS: PARTIAL
- **Location:** `/api` parses a JSON envelope (`cluster/node.go:225-234`) and forwards `request.Data.MarshalJSON()` (`:304-309`); local handlers `Unmarshal` via `env.Serializer` (default protobuf, `internal/env/env.go:64`, `cluster/handler.go:603-607`); responses `message.Serialize(v)` but force `application/json` (`cluster/http_agent.go:194,207`).
- **Impact @ CCU:** With the default protobuf serializer, JSON HTTP clients send bytes protobuf handlers can't unmarshal, and protobuf responses are mislabeled JSON — HTTP mode is effectively broken unless the JSON serializer is selected.
- **Fix:** Make HTTP payload encoding explicit (require/validate the JSON serializer for HTTP, or use an HTTP-specific JSON codec) and set content type from the actual body.

### M36 — `convertFastHTTPToHTTP` drops body + content metadata before middleware *(round 8)* — STATUS: FIXED
- **Location:** `cluster/node.go:200-214` builds an `*http.Request` with only Method/URL/Proto/Header/Host/RemoteAddr — no `Body`/`ContentLength`/`RequestURI`/TLS; repeated headers overwritten (`:202-203`). Passed to `WithMiddlewareHttp` (`:284-285`, `:462-464`).
- **Impact @ CCU:** Body/size/repeated-header-based middleware (auth, validation) sees a non-equivalent request → can fail open/closed.
- **Fix:** Populate `Body` from the fasthttp body, set `ContentLength`/`RequestURI`/scheme, append repeated headers.

### M37 — `/api` waits for responses by 1 ms busy-polling under the agent mutex *(round 8; perf)* — STATUS: FIXED
- **Location:** `cluster/node.go:340-365` — timer + `default` select branch that locks `agent.mu`, reads `observeStatus`, unlocks, `time.Sleep(1ms)` until response/timeout.
- **Impact @ CCU:** Every in-flight `/api` request wakes ~every ms and contends on the agent mutex → scheduler/CPU pressure scaling with active waits, not useful work.
- **Fix:** Replace polling with a per-message response channel/condition selected together with the timeout.

---

## LOW

### L1 — `logger.Debugf` passes `v` instead of `v...` — STATUS: FIXED
- **Location:** `internal/log/logger.go:127-132`.
- **Evidence:** `logger.Infof("[DEBUG] "+format, v)` supplies one `[]interface{}` to multiple verbs → malformed output (`%!s(...)`).
- **Fix:** `logger.Infof("[DEBUG] "+format, v...)`.

### L2 — `scheduler.PushTask` debug uses `log.Println` with format verbs — STATUS: FIXED
- **Location:** `scheduler/scheduler.go:142`.
- **Evidence:** `log.Println("Scheduler push task channel size %d", len(chTasks))` prints the literal `%d`.
- **Fix:** Use `log.Printf`/`Infof`.

### L3 — `log.Debug` shows literal format verbs *(found in verification)* — STATUS: FIXED
- **Location:** `internal/log/logger.go:119-124` always formats `"[DEBUG] %v"` over the whole arg slice; callers pass format strings, e.g. `internal/codec/codec.go:57`, `cluster/cluster.go:261`.
- **Evidence:** `log.Debug("forward header: %v, type=%v …", …)` renders the literal `%v` rather than interpolating — debug diagnostics are unreadable.
- **Fix:** Route `Debug` through a formatting path (and fix L1) so debug calls interpolate their arguments.

### L4 — WebSocket close sends no close control frame *(round 2)* — STATUS: FIXED
- **Location:** `cluster/ws.go:103-106` — `wsConn.Close` calls `c.conn.Close()` directly.
- **Impact:** Clients/proxies see abnormal closure on every server-side close (noisy reconnects), not a crash/leak.
- **Fix:** Send `websocket.CloseMessage` via `WriteControl` (under the write mutex) before closing.

### L5 — Nil/typed-nil component registration panics during extraction *(round 3)* — STATUS: FIXED
- **Location:** `component/service.go:60-64` does `reflect.Indirect(s.Receiver).Type().Name()` before nil/typed-nil checks (`:98-104`).
- **Impact:** Registering a nil component crashes at startup instead of returning an `ExtractHandler` error. Config-time.
- **Fix:** Check nil/invalid `reflect.Value`/typed-nil before `reflect.Indirect`.

### L6 — `WithNameFunc` duplicate route names silently overwrite handlers *(round 3)* — STATUS: FIXED
- **Location:** `component/service.go:82-86` (`methods[mn] = ...` with no duplicate check after rename).
- **Impact:** Two methods renamed to the same route collapse to one → a route becomes unreachable / wrong dispatch, no startup error.
- **Fix:** Detect an existing `methods[mn]` and return an extraction error on collision.

### L7 — `WithComponents(nil)` causes a startup nil panic *(round 3; found in verification)* — STATUS: FIXED
- **Location:** `options.go:70-73` overwrites the safe default with the supplied (possibly nil) `*component.Components`; `n.Components.List()` is dereferenced at startup (`cluster/node.go:111`, `component/hub.go:37-39`).
- **Impact:** Passing nil compiles and then nil-panics during `Startup`. Config-time.
- **Fix:** Ignore nil (keep the default) or reject it at option application.

### L8 — Decoder keeps invalid frame state after a `forward` error *(round 4)* — STATUS: FIXED
- **Location:** `internal/codec/codec.go:54-66` — `forward` consumes the 4-byte header (`c.buf.Next(HeadLength)`), returns on invalid type before setting a new size, and leaves an oversized `c.size` on `> MaxPacketSize`; `Decode` (`:85-103`) doesn't reset `buf`/`typ`/`size` on error.
- **Evidence:** Reusing the same `Decoder` after a decode error desynchronizes the stream (header already consumed; oversized `c.size` persists → later calls mis-frame). The cluster read loop closes the connection on decode error (`cluster/handler.go:313-324`), so it is not an active crash there, but the stateful decoder API invariant is broken.
- **Impact @ CCU:** Latent mis-framing/stream corruption for any caller that reuses a decoder after an error.
- **Fix:** Make decode errors terminal (require a fresh decoder) or reset `buf`/`typ`/`size` on error; validate oversize length into a local before committing to `c.size`.

### L9 — `ServiceAddr` parsing panics on malformed/IPv6 address in cluster mode *(round 5)* — STATUS: FIXED
- **Location:** `cluster/node.go:571` — `port := strings.Split(n.ServiceAddr, ":")[1]` (reached when `IsMaster` or `AdvertiseAddr != ""`); only emptiness is checked earlier (`:102-105`).
- **Evidence:** A non-empty malformed `ServiceAddr` (e.g. `localhost`) makes `[1]` index out of range → startup panic; a valid IPv6 hostport `[::1]:9000` is misparsed (`[1] == ""`) and rejected before `net.Listen`.
- **Impact:** Config-time startup crash / IPv6 unusable for the service address. Distinct from M14 (empty registration) and M17 (per-IP IPv6 metric parsing).
- **Fix:** Use `net.SplitHostPort`; return a normal startup error on parse failure / empty port; keep the original address for `ForceHostname`/IPv6 listen.

### L10 — `WithWSPath` accepts reserved paths that shadow the WS route *(round 8)* — STATUS: FIXED
- **Location:** `options.go:104-107` stores any path in `env.WSPath`; `handleFastHTTP` switches `/api`,`/sse`,`/health` before the WS case (`cluster/node.go:183-190`).
- **Impact:** `WithWSPath("/api"|"/sse"|"/health")` is accepted but the WS handler is unreachable there (silently ignored).
- **Fix:** Reject reserved paths / validate the WS path at option application.

### L11 — Server-issued SSE cookie is `Secure` on the plain listener *(round 8)* — STATUS: FIXED
- **Location:** `cluster/node.go:402-409` sets `sse_sessionID` with `HTTPOnly`+`Secure`; the started listener is plain `fasthttp` (`:157-175`; TLS path dead per M30). `/api` requires that cookie or `X-SSE-SessionID` (`:238-244`).
- **Impact:** On plain-HTTP deployments browsers won't send the `Secure` cookie back and `HTTPOnly` blocks JS fallback → the advertised cookie handshake is unusable unless the client parses the SSE event and sets the header.
- **Fix:** Set `Secure` from the actual external scheme/TLS-proxy config, or make the header/token flow explicit for non-TLS SSE.

---

## Recommended fix order

1. **Stop the crashes:** C1 (bounds check + `recover` in read loop), C6 (recover `cron`/condition), C7 (nil-check membership RPCs + add a gRPC `recover` interceptor), C2 (CAS/Once in `Close`), H22/H30 (no `Fatalf` in serve/metrics goroutines).
2. **Kill hot-path logging:** C5 — remove leftovers, gate the rest behind `env.Debug`.
3. **Fix races (gate on `-race`):** C3 (`lastAt`), H2 (session), H4 (members), H8 (dictionary, runtime-mutation only), H14 (`sseClients`), H15 (`connectionCount`).
4. **Backpressure/leaks:** C4 (drop `chWrite`), H7 (non-blocking `send`), H11 (deadlines + cap), H10 (copy raw payload).
5. **Cluster resilience / leaks:** H5/H6 (timeouts + async fan-out), H9 (auth), H13 (delete local session on disconnect), M3 (close pools).
6. **Scale-out:** H1 — require/document per-room `SchedName` schedulers; split timers from dispatch.
7. **Remainder:** M1/M2/M4–M6, L1/L2.

## How to verify (commands available in this repo)

- `go test -race ./...` — catches C3, H2, H4, H8, H14, H15, H17, M4, M5, M10, M24, M25.
- `go vet ./...` — catches L1, L2 (`Printf`-family checks).
- Metrics DoS check (H28/H29): fuzz distinct routes / source IPs, then confirm the Prometheus registry series count stays bounded.
- New regression tests to add:
  - Compressed-route frame with one trailing byte → expect error, not panic (C1).
  - Two goroutines calling `agent.Close()` concurrently → no panic (C2).
  - `Heartbeat`/`NewMember` RPC with nil `MemberInfo` → expect error, not panic (C7).
  - `HandshakeAck` as the first packet → connection must NOT reach working state (H26).
  - Concurrent SSE close + `/api` request on the same session id → no panic (H17/H19).
  - `Broadcast` to a large group while `Leave` runs concurrently (H3).
  - Load test via `benchmark/io` under `-race` after fixes.

---

*Generated from a 6-reviewer parallel audit; every CRITICAL and HIGH finding was validated against the cited source lines, then independently re-verified by an oracle pass (C5 downgraded, C6/H10/H11/M5 corrected, H13–H15 + L3 added).*
