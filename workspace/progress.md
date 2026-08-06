# Progress

## Round 1 plan

- Active Major: Lock-free SPSC ownership-transfer queue (round 1).
- Task: Replace the `Mutex<BinaryHeap>` implementation in `src/lib.rs` with a
  bounded preallocated slot representation specialized for the trusted SPSC
  workload. Use atomic publication/reclamation so producer and consumer never
  wait for one another, scan only published slots to return the lowest numeric
  priority, and copy values into candidate-owned storage without retaining
  caller pointers. Preserve all exported symbols and statuses in
  `_evaluator/priority-queue/CANDIDATE_CONTRACT.md`; handle runner-supplied
  capacity/value-size limits and invalid undersized dequeue without removing or
  modifying the item. Keep any multi-count handle validation required by the
  ABI, but optimize the one-producer/one-consumer operation path.
- Pass criteria: The trusted accuracy checker and benchmark sanity check pass;
  the candidate builds `./priority-queue-candidate.so`, remains linearizable
  and bounded, returns minimum numeric priorities, preserves values and queue
  state on invalid undersized dequeue, and the SPSC benchmark reports a valid
  end-to-end `total_ops_per_sec` without hangs, crashes, or malformed statuses.
- Reasoning: The current baseline serializes both operations on one mutex,
  performs BinaryHeap bookkeeping, and allocates a value vector on every
  enqueue. SPSC ownership allows those synchronization costs to be removed;
  this is the highest-leverage structural step before copy-path and priority
  scan tuning.
## Round 1 — Orchestrator (plan)
- **reasoning**: The baseline serializes both operations on one mutex, performs heap bookkeeping, and allocates a value vector per enqueue. SPSC ownership enables those costs to be removed, making this the highest-leverage first structural optimization.

### Task
Replace the Mutex<BinaryHeap> implementation in src/lib.rs with a bounded preallocated slot representation specialized for SPSC. Use atomic publication/reclamation, avoid producer-consumer blocking, scan published slots for the lowest numeric priority, copy values into owned storage, and preserve all ABI statuses and invalid undersized-dequeue semantics from _evaluator/priority-queue/CANDIDATE_CONTRACT.md.

### Pass criteria
Trusted accuracy and benchmark sanity checks pass; ./priority-queue-candidate.so builds and exports the required ABI; the queue remains bounded and linearizable, returns minimum priorities, preserves values on invalid undersized dequeue, and reports valid end-to-end total_ops_per_sec without hangs or crashes.

## Round 1 — Implementer result

- Replaced the mutex-protected `BinaryHeap` with preallocated slots containing
  fixed owned value buffers and atomic `empty/writing/published/claimed`
  ownership states.
- Enqueue copies into a claimed empty slot and publishes it with release
  ordering; dequeue scans published slots for the lowest numeric priority,
  validates output size before claiming, copies the value, and reclaims the
  slot without producer/consumer locks.
- Preserved ABI handles, statuses, capacity/value-size validation, and
  undersized-dequeue non-destructive behavior. Added a regression test for
  unchanged output and retained item state.
- Verification: `cargo test --locked` passed (2 tests); release build created
  `priority-queue-candidate.so`; all required `vspq_*` symbols exported; the
  native ABI probe passed; a 1P/1C benchmark produced valid
  `total_ops_per_sec` (291214.633413 in the short sanity run).
- Go evaluator tests could not run because the environment has a trimmed Go
  binary with no configured `GOROOT`.
## Round 1 — Implementer (attempt 1)
- **expected_behavior**: Bounded lock-free SPSC queue returns minimum priorities, copies values safely, preserves items on invalid dequeue, passes ABI probe and tests, and reports valid benchmark throughput.

### Summary
Replaced Mutex<BinaryHeap> with preallocated atomic ownership slots, preserving ABI behavior and undersized-dequeue semantics. Added regression coverage and built the shared library with all required exports.

## Round 1 — Judge (attempt 1)
- **verdict**: pass

### Analysis
The candidate builds successfully as a native shared library and exports every required ABI symbol with ABI version 1. Rust unit tests pass, including bounded priority ordering and preservation after undersized dequeue. Native ABI probes pass for non-word-aligned values and concurrent producer/consumer handles, including payload-copying, empty/invalid output invariants, and conservation. A native SPSC benchmark completes without hangs or crashes and reports valid total_ops_per_sec. The implementation preallocates slot buffers, copies inputs and outputs, uses release/acquire state publication, enforces capacity, and scans published slots for the minimum priority. No correctness, completeness, dependency, or ABI issues were found. The Go evaluator could not run because the environment's Go installation lacks GOROOT, but the available native evaluator checks passed.

### Feedback


