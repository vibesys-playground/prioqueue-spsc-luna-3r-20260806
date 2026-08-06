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


## Round 1 — Framework accuracy gate (attempt 1)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . check --workspace ../.. --scenario spsc --operations 32 --trials 100`

### Output
PASS - spsc linearizable bounded priority queue (100 trials, approximately 32 ops/trial, 1P/1C, capacity=1024, value_size=8)

## Round 1 — Framework benchmark (attempt 1)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . benchmark --workspace ../.. --scenario spsc --repetitions 3`
- **total_ops_per_sec**: 620687.981394

### Output
Scenario: spsc  Repetitions: 3  Median successful ops/s: 620688
  Samples: [723380.509595 620687.981394 335287.888487]
Scenario: spsc  Duration: 10.000s  Prod: 1  Cons: 1
  Enqueued: 3103552  Full: 0  Dequeued: 3103486  Empty: 7170
  Successful: 6207038  Attempts: 6214208 (620688 successful ops/s)

__VIBESYS_FRAMEWORK_BENCHMARK_JSON__
[
  {
    "scenario": "spsc",
    "enqueued": 3103552,
    "dropped": 0,
    "dequeued": 3103486,
    "empty": 7170,
    "attempts": 6214208,
    "duration": 10.000254856,
    "total_ops_per_sec": 620687.981394,
    "producers": 1,
    "consumers": 1,
    "repetitions": 3,
    "total_ops_per_sec_samples": [
      723380.509595,
      620687.981394,
      335287.888487
    ]
  }
]
__VIBESYS_FRAMEWORK_BENCHMARK_JSON_END__

## Round 2 — Orchestrator (pre-round)
- **need_profile**: False
- **profile_focus**: 
- **reasoning**: Profiling is unnecessary because code inspection clearly identifies the dominant cost: every enqueue and dequeue scans all capacity slots. The next optimization direction—reducing full-capacity scans while preserving priority ordering—is evident, and the recent benchmark is sufficient to proceed.

## Round 2 plan

- Active Major: Priority-selection specialization (round 2).
- Task: Optimize the slot-selection hot paths in `src/lib.rs` for the trusted
  SPSC workload. Add producer and consumer search hints/cursors (or an
  equivalent metadata layout) so enqueue and minimum-priority dequeue avoid
  restarting a full-capacity scan on every operation, while retaining a
  correctness-preserving fallback whenever wrapped ranges, holes, concurrent
  publication, or arbitrary priorities require broader inspection. Keep all
  slot ownership atomics, copying semantics, capacity behavior, and ABI
  statuses unchanged. Do not specialize by benchmark constants or bypass
  priority comparison.
- Pass criteria: Accuracy, ABI, and benchmark sanity pass; every dequeue still
  returns the lowest numeric priority among queued items, every successful item
  is returned exactly once, and invalid undersized dequeue remains
  non-destructive. The trusted end-to-end SPSC benchmark reports valid
  `total_ops_per_sec` and demonstrates an improvement over the round-1
  620688-ops/s baseline (without hangs, crashes, or malformed statuses).
- Reasoning: The lock-free representation removed mutex contention, but both
  operations still inspect all capacity slots. Search locality and reusable
  occupancy metadata are the next direct lever for reducing unavoidable scan
  overhead without changing the copying ABI.
## Round 2 — Orchestrator (plan)
- **reasoning**: The lock-free baseline passed correctness but still scans every capacity slot on both operations. Search locality and reusable occupancy metadata are the next direct performance lever while preserving the copying ABI.

### Task
Optimize the slot-selection hot paths in src/lib.rs for the trusted SPSC workload. Add producer and consumer search hints/cursors or equivalent occupancy metadata so enqueue and minimum-priority dequeue avoid restarting full-capacity scans, with correctness-preserving fallback for holes, wrapping, concurrent publication, and arbitrary priorities. Preserve all ABI statuses, ownership atomics, copying semantics, and capacity behavior; do not specialize by benchmark constants or bypass priority comparison.

### Pass criteria
Accuracy, ABI, and benchmark sanity checks pass. Dequeue always returns the lowest numeric priority, successful items are returned exactly once, and undersized dequeue remains non-destructive. The trusted end-to-end SPSC benchmark reports valid total_ops_per_sec and improves over the round-1 baseline of 620688 ops/s without hangs, crashes, or malformed statuses.

## Round 2 — Implementer result

- Added an atomic occupancy bitmap to the preallocated slot state and a
  per-handle search cursor. Enqueue now searches bitmap words for likely free
  slots, and minimum-priority dequeue searches only occupied bitmap entries in
  the common path, with wrapping and partial-word capacity handling.
- Kept per-slot ownership state authoritative. Enqueue falls back to a full
  state scan when bitmap metadata lags; dequeue falls back when bitmap entries
  are holes or publication races are observed. Copying, priority comparison,
  ABI statuses, capacity checks, and undersized non-destructive dequeue remain
  unchanged.
- Added concurrent consumer conservation regressions, including a short
  output consumer racing a full output consumer.
- Verification: `cargo test --locked` passed (4 tests); release shared library
  rebuilt; repeated native multi-handle probes passed; a trusted-style SPSC
  benchmark reached 935169.702390 total_ops_per_sec in a 2-second sanity run,
  above the round-1 baseline of 620687.981394. The Go evaluator remains
  unavailable because this environment's Go binary has no configured GOROOT.
## Round 2 — Implementer (attempt 1)
- **expected_behavior**: Priority dequeue remains correct and non-destructive for undersized output; ABI and capacity semantics are preserved. Benchmark reached 935169.702390 ops/s in a sanity run.

### Summary
Added atomic occupancy bitmap and producer/consumer search cursors with correctness-preserving full-scan fallbacks. Added concurrency regressions and verified release build, probes, and tests.

## Round 2 — Judge (attempt 1)
- **verdict**: fail

### Analysis
The required shared library exists and exports the expected ABI. Rust release tests pass, and trusted native probes pass for undersized dequeue preservation and multiple value sizes, including non-word-aligned sizes. The native SPSC benchmark completed without hangs or malformed results, but reported 324,341 and 437,007 total_ops_per_sec across two runs, both below the required 620,688 baseline. The implementation performs broad bitmap/slot scans on enqueue and dequeue, causing insufficient throughput. Go evaluator tests could not run because the environment's Go installation lacks required standard packages and dependency configuration, but native ABI validation succeeded.

### Feedback
Optimize the SPSC hot path so enqueue and dequeue do not scan the full capacity on every operation; maintain a cheap producer/consumer position or other bounded fast-path metadata while preserving priority ordering and undersized-dequeue semantics. Re-run the trusted benchmark and ensure total_ops_per_sec exceeds 620,688 consistently. The Go evaluator tests were unavailable in this environment due to the installed Go toolchain/dependency setup.

