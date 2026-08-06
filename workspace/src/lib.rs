#![deny(unsafe_op_in_unsafe_fn)]

mod ffi;

use std::alloc::{alloc, dealloc, Layout};
use std::cell::UnsafeCell;
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::ptr::{self, NonNull};
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
    // The buffer is allocated once with exactly the runner-supplied bound.
    // Keeping only the pointer and bound avoids Vec metadata and permits a
    // direct bounded non-overlapping copy in the operation path.
    value: NonNull<u8>,
    value_layout: Layout,
}

// A slot's raw buffer is accessed by only the owner of its current state. The
// state release/acquire transitions publish all value-buffer accesses between
// the producer and consumer.
unsafe impl Send for Slot {}
unsafe impl Sync for Slot {}

impl Slot {
    fn new(max_value_size: usize) -> Result<Self, u32> {
        let value_layout = Layout::array::<u8>(max_value_size).map_err(|_| STATUS_INVALID)?;
        let value = NonNull::new(unsafe { alloc(value_layout) }).ok_or(STATUS_INTERNAL_ERROR)?;
        Ok(Self {
            state: AtomicU8::new(SLOT_EMPTY),
            priority: AtomicU64::new(0),
            length: AtomicUsize::new(0),
            value,
            value_layout,
        })
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        unsafe { dealloc(self.value.as_ptr(), self.value_layout) };
    }
}

struct QueueState {
    max_value_size: usize,
    slots: Vec<Slot>,
    // Search-only occupancy metadata. Slot state remains authoritative.
    occupied: Vec<AtomicUsize>,
    // In the exact one-producer/one-consumer configuration these positions
    // describe a monotonic ring window.  They are only a search hint: slot
    // state remains authoritative and the bitmap path is retained as the
    // recovery path for holes and wrapped reuse.
    ring_mode: AtomicU8,
    producer_position: AtomicUsize,
    consumer_position: AtomicUsize,
    published_indices: Vec<AtomicUsize>,
    published_write: AtomicUsize,
    published_read: AtomicUsize,
    free_indices: Vec<AtomicUsize>,
    free_write: AtomicUsize,
    free_read: AtomicUsize,
}

#[derive(Eq, PartialEq)]
struct HeapItem {
    priority: u64,
    index: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.index.cmp(&self.index))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
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
        let mut published_indices = Vec::new();
        published_indices
            .try_reserve_exact(capacity)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for _ in 0..capacity {
            published_indices.push(AtomicUsize::new(0));
        }
        let mut free_indices = Vec::new();
        free_indices
            .try_reserve_exact(capacity)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for index in 0..capacity {
            free_indices.push(AtomicUsize::new(index));
        }
        Ok(Self {
            producer_count,
            consumer_count,
            state: Arc::new(QueueState {
                max_value_size,
                slots,
                occupied,
                ring_mode: AtomicU8::new(if producer_count == 1 && consumer_count == 1 {
                    2
                } else {
                    0
                }),
                producer_position: AtomicUsize::new(0),
                consumer_position: AtomicUsize::new(0),
                published_indices,
                published_write: AtomicUsize::new(0),
                published_read: AtomicUsize::new(0),
                free_indices,
                free_write: AtomicUsize::new(capacity),
                free_read: AtomicUsize::new(0),
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
                heap: UnsafeCell::new(BinaryHeap::new()),
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

        if self.state.ring_mode.load(Ordering::Acquire) == 2 {
            if let Some(status) = self.try_enqueue_exchange(priority, data) {
                return status;
            }
        }

        if self.state.ring_mode.load(Ordering::Acquire) == 1 {
            if let Some(status) = self.try_enqueue_ring(priority, data) {
                return status;
            }
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
    fn try_enqueue_exchange(&self, priority: u64, data: &[u8]) -> Option<u32> {
        let read = self.state.free_read.load(Ordering::Acquire);
        let write = self.state.free_write.load(Ordering::Acquire);
        if read == write {
            return Some(STATUS_FULL);
        }
        let free_slot = &self.state.free_indices[read % self.state.slots.len()];
        let index = free_slot.load(Ordering::Relaxed);
        let slot = &self.state.slots[index];
        if slot
            .state
            .compare_exchange(SLOT_EMPTY, SLOT_WRITING, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            self.state.ring_mode.store(0, Ordering::Release);
            return None;
        }
        unsafe { ptr::copy_nonoverlapping(data.as_ptr(), slot.value.as_ptr(), data.len()) };
        slot.priority.store(priority, Ordering::Relaxed);
        slot.length.store(data.len(), Ordering::Relaxed);
        self.state.occupied[index / WORD_BITS].fetch_or(
            1usize << (index % WORD_BITS),
            Ordering::Release,
        );
        slot.state.store(SLOT_PUBLISHED, Ordering::Release);
        let published_write = self.state.published_write.load(Ordering::Relaxed);
        self.state.published_indices[published_write % self.state.slots.len()]
            .store(index, Ordering::Relaxed);
        self.state
            .published_write
            .store(published_write.wrapping_add(1), Ordering::Release);
        self.state
            .free_read
            .store(read.wrapping_add(1), Ordering::Release);
        Some(STATUS_OK)
    }

    #[inline]
    fn try_enqueue_ring(&self, priority: u64, data: &[u8]) -> Option<u32> {
        let position = self.state.producer_position.load(Ordering::Relaxed);
        let index = position % self.state.slots.len();
        let slot = &self.state.slots[index];
        if slot
            .state
            .compare_exchange(SLOT_EMPTY, SLOT_WRITING, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            // The next ring cell is still live.  Reusing an older hole would
            // invalidate the compact ring window, so switch to the general
            // path before publishing anything outside it.
            self.state.ring_mode.store(0, Ordering::Release);
            return None;
        }
        unsafe { ptr::copy_nonoverlapping(data.as_ptr(), slot.value.as_ptr(), data.len()) };
        slot.priority.store(priority, Ordering::Relaxed);
        slot.length.store(data.len(), Ordering::Relaxed);
        self.state.occupied[index / WORD_BITS].fetch_or(
            1usize << (index % WORD_BITS),
            Ordering::Release,
        );
        slot.state.store(SLOT_PUBLISHED, Ordering::Release);
        self.state
            .producer_position
            .store(position.wrapping_add(1), Ordering::Release);
        Some(STATUS_OK)
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
        unsafe { ptr::copy_nonoverlapping(data.as_ptr(), slot.value.as_ptr(), data.len()) };
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
    heap: UnsafeCell<BinaryHeap<HeapItem>>,
}

unsafe impl Sync for Consumer {}

impl Consumer {
    fn try_dequeue(&self, output: &mut [u8]) -> Result<usize, u32> {
        if self.state.ring_mode.load(Ordering::Acquire) == 2 {
            if let Some(result) = self.try_dequeue_exchange(output) {
                return result;
            }
        }
        if self.state.ring_mode.load(Ordering::Acquire) != 0 {
            if let Some(result) = self.try_dequeue_ring(output) {
                return result;
            }
        }
        self.try_dequeue_general(output)
    }

    fn try_dequeue_exchange(&self, output: &mut [u8]) -> Option<Result<usize, u32>> {
        let capacity = self.state.slots.len();
        let mut read = self.state.published_read.load(Ordering::Relaxed);
        let write = self.state.published_write.load(Ordering::Acquire);
        let heap = unsafe { &mut *self.heap.get() };
        while read != write {
            let index = self.state.published_indices[read % capacity].load(Ordering::Relaxed);
            let slot = &self.state.slots[index];
            if slot.state.load(Ordering::Acquire) != SLOT_PUBLISHED {
                self.state.ring_mode.store(0, Ordering::Release);
                return None;
            }
            heap.push(HeapItem {
                priority: slot.priority.load(Ordering::Relaxed),
                index,
            });
            read = read.wrapping_add(1);
        }
        self.state.published_read.store(read, Ordering::Release);

        let Some(item) = heap.peek() else {
            return Some(Err(STATUS_EMPTY));
        };
        let slot = &self.state.slots[item.index];
        let length = slot.length.load(Ordering::Relaxed);
        if length > output.len() {
            return Some(Err(STATUS_INVALID));
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
            self.state.ring_mode.store(0, Ordering::Release);
            return None;
        }
        let item = heap.pop().expect("heap item was peeked");
        unsafe { ptr::copy_nonoverlapping(slot.value.as_ptr(), output.as_mut_ptr(), length) };
        self.state.occupied[item.index / WORD_BITS].fetch_and(
            !(1usize << (item.index % WORD_BITS)),
            Ordering::AcqRel,
        );
        slot.state.store(SLOT_EMPTY, Ordering::Release);
        let free_write = self.state.free_write.load(Ordering::Relaxed);
        self.state.free_indices[free_write % capacity].store(item.index, Ordering::Relaxed);
        self.state
            .free_write
            .store(free_write.wrapping_add(1), Ordering::Release);
        Some(Ok(length))
    }

    // The common SPSC path scans the current logical window, rather than all
    // capacity slots.  A minimum still requires comparing every live item;
    // this is the necessary priority-ordering work, but the window is usually
    // much smaller than capacity in the try-style benchmark.
    fn try_dequeue_ring(&self, output: &mut [u8]) -> Option<Result<usize, u32>> {
        let capacity = self.state.slots.len();
        let mut head = self.state.consumer_position.load(Ordering::Relaxed);
        let tail = self.state.producer_position.load(Ordering::Acquire);
        let span = tail.wrapping_sub(head);
        if span > capacity {
            self.state.ring_mode.store(0, Ordering::Release);
            return None;
        }

        let mut best_index = None;
        let mut best_priority = u64::MAX;
        for offset in 0..span {
            let index = head.wrapping_add(offset) % capacity;
            let slot = &self.state.slots[index];
            if slot.state.load(Ordering::Acquire) == SLOT_PUBLISHED {
                let priority = slot.priority.load(Ordering::Relaxed);
                if best_index.is_none() || priority < best_priority {
                    best_priority = priority;
                    best_index = Some(index);
                }
            }
        }

        let Some(index) = best_index else {
            // If the producer has switched to the general path, inspect its
            // bitmap before reporting empty; otherwise this is a real empty
            // ring (or a window containing only reclaimed holes).
            if self.state.ring_mode.load(Ordering::Acquire) == 0 {
                return None;
            }
            while head != tail {
                let index = head % capacity;
                if self.state.slots[index].state.load(Ordering::Acquire) != SLOT_EMPTY {
                    break;
                }
                head = head.wrapping_add(1);
            }
            self.state.consumer_position.store(head, Ordering::Release);
            return Some(Err(STATUS_EMPTY));
        };

        let slot = &self.state.slots[index];
        let length = slot.length.load(Ordering::Relaxed);
        if length > output.len() {
            return Some(Err(STATUS_INVALID));
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
            return Some(Err(STATUS_EMPTY));
        }
        unsafe { ptr::copy_nonoverlapping(slot.value.as_ptr(), output.as_mut_ptr(), length) };
        self.state.occupied[index / WORD_BITS].fetch_and(
            !(1usize << (index % WORD_BITS)),
            Ordering::AcqRel,
        );
        slot.state.store(SLOT_EMPTY, Ordering::Release);
        head = self.state.consumer_position.load(Ordering::Relaxed);
        while head != tail {
            let head_index = head % capacity;
            if self.state.slots[head_index].state.load(Ordering::Acquire) != SLOT_EMPTY {
                break;
            }
            head = head.wrapping_add(1);
        }
        self.state.consumer_position.store(head, Ordering::Release);
        Some(Ok(length))
    }

    fn try_dequeue_general(&self, output: &mut [u8]) -> Result<usize, u32> {
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
            unsafe { ptr::copy_nonoverlapping(slot.value.as_ptr(), output.as_mut_ptr(), length) };
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
