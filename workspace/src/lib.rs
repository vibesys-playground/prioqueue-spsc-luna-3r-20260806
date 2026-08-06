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
        Ok(Self {
            producer_count,
            consumer_count,
            state: Arc::new(QueueState {
                max_value_size,
                slots,
            }),
        })
    }

    fn producer(&self, id: u32) -> Result<Producer, u32> {
        (id < self.producer_count)
            .then(|| Producer {
                state: Arc::clone(&self.state),
            })
            .ok_or(STATUS_INVALID)
    }

    fn consumer(&self, id: u32) -> Result<Consumer, u32> {
        (id < self.consumer_count)
            .then(|| Consumer {
                state: Arc::clone(&self.state),
            })
            .ok_or(STATUS_INVALID)
    }
}

pub struct Producer {
    state: Arc<QueueState>,
}

impl Producer {
    fn try_enqueue(&self, priority: u64, data: &[u8]) -> u32 {
        if data.len() > self.state.max_value_size {
            return STATUS_INVALID;
        }

        // In the SPSC workload the producer owns the search and claims one
        // free slot without ever waiting for the consumer.
        for slot in &self.state.slots {
            if slot
                .state
                .compare_exchange(
                    SLOT_EMPTY,
                    SLOT_WRITING,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                unsafe {
                    (&mut *slot.value.get())[..data.len()].copy_from_slice(data);
                }
                slot.priority.store(priority, Ordering::Relaxed);
                slot.length.store(data.len(), Ordering::Relaxed);
                slot.state.store(SLOT_PUBLISHED, Ordering::Release);
                return STATUS_OK;
            }
        }
        STATUS_FULL
    }
}

pub struct Consumer {
    state: Arc<QueueState>,
}

impl Consumer {
    fn try_dequeue(&self, output: &mut [u8]) -> Result<usize, u32> {
        loop {
            let mut best: Option<&Slot> = None;
            let mut best_priority = u64::MAX;
            for slot in &self.state.slots {
                if slot.state.load(Ordering::Acquire) == SLOT_PUBLISHED {
                    let priority = slot.priority.load(Ordering::Relaxed);
                    if best.is_none() || priority < best_priority {
                        best = Some(slot);
                        best_priority = priority;
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
            slot.state.store(SLOT_EMPTY, Ordering::Release);
            return Ok(length);
        }
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
}
