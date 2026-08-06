#![deny(unsafe_op_in_unsafe_fn)]

mod ffi;

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

const ABI_VERSION: u32 = 1;
const STATUS_OK: u32 = 0;
const STATUS_FULL: u32 = 1;
const STATUS_EMPTY: u32 = 2;
const STATUS_INVALID: u32 = 3;
const STATUS_INTERNAL_ERROR: u32 = 4;

const SLOT_EMPTY: u8 = 0;
const SLOT_WRITING: u8 = 1;
const SLOT_PUBLISHED: u8 = 2;
const SLOT_CLAIMED: u8 = 3;
const WORD_BITS: usize = usize::BITS as usize;

struct Slot {
    state: AtomicU8,
    priority: AtomicU64,
    length: AtomicUsize,
    value: UnsafeCell<Vec<u8>>,
}

// A slot's Vec is accessed by only the owner of its current state. The state
// release/acquire transitions publish all value-buffer accesses between the
// producer and consumer.
unsafe impl Sync for Slot {}

impl Slot {
    fn new(max_value_size: usize) -> Result<Self, u32> {
        let mut value = Vec::new();
        value
            .try_reserve_exact(max_value_size)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        // The buffer is deliberately allocated once. Bytes beyond length are
        // never read, so they need not be initialized.
        unsafe { value.set_len(max_value_size) };
        Ok(Self {
            state: AtomicU8::new(SLOT_EMPTY),
            priority: AtomicU64::new(0),
            length: AtomicUsize::new(0),
            value: UnsafeCell::new(value),
        })
    }
}

struct QueueState {
    max_value_size: usize,
    slots: Vec<Slot>,
    // Search-only occupancy metadata. Slot state remains authoritative.
    occupied: Vec<AtomicUsize>,
}

pub struct Queue {
    producer_count: u32,
    consumer_count: u32,
    state: Arc<QueueState>,
}

impl Queue {
    fn new(
        capacity: u64,
        max_value_size: u64,
        producer_count: u32,
        consumer_count: u32,
    ) -> Result<Self, u32> {
        if capacity == 0 || max_value_size == 0 || producer_count == 0 || consumer_count == 0 {
            return Err(STATUS_INVALID);
        }
        let capacity = usize::try_from(capacity).map_err(|_| STATUS_INVALID)?;
        let max_value_size = usize::try_from(max_value_size).map_err(|_| STATUS_INVALID)?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for _ in 0..capacity {
            slots.push(Slot::new(max_value_size)?);
        }
        let word_count = capacity.div_ceil(WORD_BITS);
        let mut occupied = Vec::new();
        occupied
            .try_reserve_exact(word_count)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for _ in 0..word_count {
            occupied.push(AtomicUsize::new(0));
        }
        Ok(Self {
            producer_count,
            consumer_count,
            state: Arc::new(QueueState {
                max_value_size,
                slots,
                occupied,
            }),
        })
    }

    fn producer(&self, id: u32) -> Result<Producer, u32> {
        (id < self.producer_count)
            .then(|| Producer {
                state: Arc::clone(&self.state),
                search_hint: AtomicUsize::new(0),
            })
            .ok_or(STATUS_INVALID)
    }

    fn consumer(&self, id: u32) -> Result<Consumer, u32> {
        (id < self.consumer_count)
            .then(|| Consumer {
                state: Arc::clone(&self.state),
                search_hint: AtomicUsize::new(0),
            })
            .ok_or(STATUS_INVALID)
    }
}

pub struct Producer {
    state: Arc<QueueState>,
    search_hint: AtomicUsize,
}

impl Producer {
    fn try_enqueue(&self, priority: u64, data: &[u8]) -> u32 {
        if data.len() > self.state.max_value_size {
            return STATUS_INVALID;
        }

        let capacity = self.state.slots.len();
        let start = self.search_hint.load(Ordering::Relaxed) % capacity;
        let start_word = start / WORD_BITS;

        // In SPSC, the next free slot is normally near the previous one.
        // Search bitmap words first, wrapping at the end of the ring.
        for word_offset in 0..self.state.occupied.len() {
            let word_index = (start_word + word_offset) % self.state.occupied.len();
            let valid = valid_word_mask(word_index, capacity);
            let mut free = !self.state.occupied[word_index].load(Ordering::Acquire) & valid;
            while free != 0 {
                let bit = free.trailing_zeros() as usize;
                let index = word_index * WORD_BITS + bit;
                free &= free - 1;
                if self.claim_and_publish(index, priority, data) {
                    self.search_hint.store((index + 1) % capacity, Ordering::Relaxed);
                    return STATUS_OK;
                }
            }
        }

        // Metadata is only an index and may lag a concurrent reclaim. The
        // authoritative fallback preserves exact FULL behavior.
        for index in 0..capacity {
            if self.claim_and_publish(index, priority, data) {
                self.search_hint.store((index + 1) % capacity, Ordering::Relaxed);
                return STATUS_OK;
            }
        }
        STATUS_FULL
    }

    #[inline]
    fn claim_and_publish(&self, index: usize, priority: u64, data: &[u8]) -> bool {
        let slot = &self.state.slots[index];
        if slot
            .state
            .compare_exchange(SLOT_EMPTY, SLOT_WRITING, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        unsafe {
            (&mut *slot.value.get())[..data.len()].copy_from_slice(data);
        }
        slot.priority.store(priority, Ordering::Relaxed);
        slot.length.store(data.len(), Ordering::Relaxed);
        // Publish the search bit before the state release. A consumer that
        // observes the bit but not publication simply falls back/retries.
        self.state.occupied[index / WORD_BITS].fetch_or(
            1usize << (index % WORD_BITS),
            Ordering::Release,
        );
        slot.state.store(SLOT_PUBLISHED, Ordering::Release);
        true
    }
}

pub struct Consumer {
    state: Arc<QueueState>,
    search_hint: AtomicUsize,
}

impl Consumer {
    fn try_dequeue(&self, output: &mut [u8]) -> Result<usize, u32> {
        loop {
            let mut best: Option<&Slot> = None;
            let mut best_priority = u64::MAX;
            let mut best_index = 0;

            let capacity = self.state.slots.len();
            let start = self.search_hint.load(Ordering::Relaxed) % capacity;
            let start_word = start / WORD_BITS;
            for word_offset in 0..self.state.occupied.len() {
                let word_index = (start_word + word_offset) % self.state.occupied.len();
                let valid = valid_word_mask(word_index, capacity);
                let mut bits = self.state.occupied[word_index].load(Ordering::Acquire) & valid;
                while bits != 0 {
                    let bit = bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    let slot = &self.state.slots[word_index * WORD_BITS + bit];
                    if slot.state.load(Ordering::Acquire) == SLOT_PUBLISHED {
                        let priority = slot.priority.load(Ordering::Relaxed);
                        if best.is_none() || priority < best_priority {
                            best = Some(slot);
                            best_priority = priority;
                            best_index = word_index * WORD_BITS + bit;
                        }
                    }
                }
            }

            // A publication can race the bitmap read, and a reclaimed bit
            // can remain set briefly. Scan authoritative states when the
            // indexed view found no published item (or only found holes).
            if best.is_none() {
                for slot in &self.state.slots {
                    if slot.state.load(Ordering::Acquire) == SLOT_PUBLISHED {
                        let priority = slot.priority.load(Ordering::Relaxed);
                        if best.is_none() || priority < best_priority {
                            best = Some(slot);
                            best_priority = priority;
                            best_index = self
                                .state
                                .slots
                                .iter()
                                .position(|candidate| std::ptr::eq(candidate, slot))
                                .expect("selected slot belongs to queue");
                        }
                    }
                }
            }
            let Some(slot) = best else {
                return Err(STATUS_EMPTY);
            };
            let length = slot.length.load(Ordering::Relaxed);
            // Do not claim an undersized item: this preserves both the item
            // and all caller-owned output state exactly as required by ABI.
            if length > output.len() {
                return Err(STATUS_INVALID);
            }
            if slot
                .state
                .compare_exchange(
                    SLOT_PUBLISHED,
                    SLOT_CLAIMED,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                // This is only relevant if extra consumer handles are used;
                // retrying is finite, non-blocking progress for the caller.
                continue;
            }
            unsafe {
                output[..length].copy_from_slice(&(&*slot.value.get())[..length]);
            }
            let index = best_index;
            self.state.occupied[index / WORD_BITS].fetch_and(
                !(1usize << (index % WORD_BITS)),
                Ordering::AcqRel,
            );
            slot.state.store(SLOT_EMPTY, Ordering::Release);
            self.search_hint.store((index + 1) % capacity, Ordering::Relaxed);
            return Ok(length);
        }
    }
}

#[inline]
fn valid_word_mask(word_index: usize, capacity: usize) -> usize {
    let remaining = capacity.saturating_sub(word_index * WORD_BITS);
    if remaining >= WORD_BITS {
        usize::MAX
    } else if remaining == 0 {
        0
    } else {
        (1usize << remaining) - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_core_is_bounded_priority_queue() {
        let queue = Queue::new(3, 8, 1, 1).unwrap();
        let producer = queue.producer(0).unwrap();
        let consumer = queue.consumer(0).unwrap();
        assert_eq!(producer.try_enqueue(7, b"later"), STATUS_OK);
        assert_eq!(producer.try_enqueue(2, b"first"), STATUS_OK);
        assert_eq!(producer.try_enqueue(2, b"second"), STATUS_OK);
        assert_eq!(producer.try_enqueue(0, b"full"), STATUS_FULL);

        let mut output = [0_u8; 8];
        for expected in [b"first".as_slice(), b"second", b"later"] {
            assert_eq!(consumer.try_dequeue(&mut output), Ok(expected.len()));
            assert_eq!(&output[..expected.len()], expected);
        }
        assert_eq!(consumer.try_dequeue(&mut output), Err(STATUS_EMPTY));
    }

    #[test]
    fn undersized_dequeue_keeps_item_and_output_unchanged() {
        let queue = Queue::new(1, 8, 1, 1).unwrap();
        let producer = queue.producer(0).unwrap();
        let consumer = queue.consumer(0).unwrap();
        assert_eq!(producer.try_enqueue(1, b"payload"), STATUS_OK);

        let mut output = [0xA5_u8; 3];
        assert_eq!(consumer.try_dequeue(&mut output), Err(STATUS_INVALID));
        assert_eq!(output, [0xA5; 3]);

        let mut full_output = [0_u8; 8];
        assert_eq!(consumer.try_dequeue(&mut full_output), Ok(7));
        assert_eq!(&full_output[..7], b"payload");
    }

    #[test]
    fn concurrent_consumers_conserve_items() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        use std::sync::Arc;
        use std::thread;

        let queue = Queue::new(8, 64, 2, 2).unwrap();
        let producer = queue.producer(0).unwrap();
        let first = queue.consumer(0).unwrap();
        let second = queue.consumer(1).unwrap();
        let consumed = Arc::new(AtomicUsize::new(0));
        thread::scope(|scope| {
            scope.spawn(move || {
                for sequence in 0..2_000u64 {
                    let value = sequence.to_le_bytes();
                    while producer.try_enqueue(sequence, &value) == STATUS_FULL {
                        thread::yield_now();
                    }
                }
            });
            for consumer in [first, second] {
                let consumed = Arc::clone(&consumed);
                scope.spawn(move || {
                    let mut output = [0u8; 8];
                    while consumed.load(AtomicOrdering::Acquire) < 2_000 {
                        if consumer.try_dequeue(&mut output).is_ok() {
                            consumed.fetch_add(1, AtomicOrdering::Release);
                        } else {
                            thread::yield_now();
                        }
                    }
                });
            }
        });
        assert_eq!(consumed.load(AtomicOrdering::Acquire), 2_000);
    }

    #[test]
    fn concurrent_undersized_consumer_does_not_block_other_consumer() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        use std::sync::Arc;
        use std::thread;

        let queue = Queue::new(8, 64, 1, 2).unwrap();
        let producer = queue.producer(0).unwrap();
        let short = queue.consumer(0).unwrap();
        let full = queue.consumer(1).unwrap();
        let consumed = Arc::new(AtomicUsize::new(0));
        thread::scope(|scope| {
            scope.spawn(move || {
                for sequence in 0..2_000u64 {
                    let length = if sequence % 2 == 0 { 16 } else { 64 };
                    let value = vec![sequence as u8; length];
                    while producer.try_enqueue(sequence, &value) == STATUS_FULL {
                        thread::yield_now();
                    }
                }
            });
            let consumed_short = Arc::clone(&consumed);
            scope.spawn(move || {
                let mut output = [0xa7u8; 32];
                while consumed_short.load(AtomicOrdering::Acquire) < 2_000 {
                    if short.try_dequeue(&mut output).is_ok() {
                        consumed_short.fetch_add(1, AtomicOrdering::Release);
                    } else {
                        thread::yield_now();
                    }
                }
            });
            let consumed_full = Arc::clone(&consumed);
            scope.spawn(move || {
                let mut output = [0u8; 64];
                while consumed_full.load(AtomicOrdering::Acquire) < 2_000 {
                    if full.try_dequeue(&mut output).is_ok() {
                        consumed_full.fetch_add(1, AtomicOrdering::Release);
                    } else {
                        thread::yield_now();
                    }
                }
            });
        });
        assert_eq!(consumed.load(AtomicOrdering::Acquire), 2_000);
    }
}
