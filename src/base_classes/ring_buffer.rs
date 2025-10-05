#![allow(dead_code)]

use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicUsize, Ordering};

// Simple padding to reduce false sharing on hot atomics
#[repr(align(128))]
struct Pad<T>(T);

// Single-producer single-consumer lock-free ring buffer with power-of-two capacity.
// Provides non-blocking try_push/try_pop designed for ultra-low latency pipelines.
pub struct Producer<T, const N: usize> {
    inner: std::sync::Arc<Inner<T, N>>,
}

pub struct Consumer<T, const N: usize> {
    inner: std::sync::Arc<Inner<T, N>>,
}

struct Inner<T, const N: usize> {
    buf: Box<[MaybeUninit<T>]>,
    mask: usize,
    head: Pad<AtomicUsize>, // next write index
    tail: Pad<AtomicUsize>, // next read index
}

// Safety: SPSC algorithm ensures exclusive slot ownership between producer and consumer
// through head/tail indices with acquire/release ordering. It's sound to share Inner across
// threads when T: Send.
unsafe impl<T: Send, const N: usize> Sync for Inner<T, N> {}
unsafe impl<T: Send, const N: usize> Send for Inner<T, N> {}

impl<T, const N: usize> Inner<T, N> {
    #[inline(always)]
    fn new() -> Self {
        assert!(
            N.is_power_of_two(),
            "SPSC ring capacity must be a power of two"
        );
        // Allocate uninitialized buffer on heap to avoid large stack frames
        // SAFETY: we only ever read entries after they were written, and drop drains initialized items.
        let buf: Box<[MaybeUninit<T>]> = unsafe { Box::new_uninit_slice(N).assume_init() };
        Self {
            buf,
            mask: N - 1,
            head: Pad(AtomicUsize::new(0)),
            tail: Pad(AtomicUsize::new(0)),
        }
    }
}

impl<T, const N: usize> Producer<T, N> {
    #[inline(always)]
    pub fn new_pair() -> (Self, Consumer<T, N>) {
        let inner = std::sync::Arc::new(Inner::<T, N>::new());
        (
            Self {
                inner: inner.clone(),
            },
            Consumer { inner },
        )
    }

    // Returns Ok(()) if pushed, Err(item) if full.
    #[inline(always)]
    pub fn try_push(&self, item: T) -> Result<(), T> {
        let head = self.inner.head.0.load(Ordering::Relaxed);
        let next = (head + 1) & self.inner.mask;
        if next == self.inner.tail.0.load(Ordering::Acquire) {
            return Err(item); // full
        }
        // SAFETY: slot is exclusively owned by producer until tail moves past it.
        let idx = head & self.inner.mask;
        let slot = unsafe {
            self.inner.buf.get_unchecked(idx) as *const MaybeUninit<T> as *mut MaybeUninit<T>
        };
        unsafe {
            (*slot).write(item);
        }
        self.inner.head.0.store(next, Ordering::Release);
        Ok(())
    }

    // Spin until space available. For ultra-low latency producer threads.
    #[inline(always)]
    pub fn push_spin(&self, mut item: T) {
        loop {
            match self.try_push(item) {
                Ok(()) => return,
                Err(it) => {
                    item = it;
                    core::hint::spin_loop();
                }
            }
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let head = self.inner.head.0.load(Ordering::Acquire);
        let tail = self.inner.tail.0.load(Ordering::Acquire);
        head.wrapping_sub(tail) & self.inner.mask
    }
}

impl<T, const N: usize> Consumer<T, N> {
    // Returns Ok(item) if present, Err(()) if empty.
    #[inline(always)]
    pub fn try_pop(&self) -> Result<T, ()> {
        let tail = self.inner.tail.0.load(Ordering::Relaxed);
        if tail == self.inner.head.0.load(Ordering::Acquire) {
            return Err(()); // empty
        }
        // SAFETY: slot was previously written by producer and not yet consumed.
        let idx = tail & self.inner.mask;
        let slot = unsafe { self.inner.buf.get_unchecked(idx) as *const MaybeUninit<T> };
        let item = unsafe { (*slot).assume_init_read() };
        self.inner
            .tail
            .0
            .store((tail + 1) & self.inner.mask, Ordering::Release);
        Ok(item)
    }

    // Spin until data available.
    #[inline(always)]
    pub fn pop_spin(&self) -> T {
        loop {
            if let Ok(v) = self.try_pop() {
                return v;
            }
            core::hint::spin_loop();
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let head = self.inner.head.0.load(Ordering::Acquire);
        let tail = self.inner.tail.0.load(Ordering::Acquire);
        head.wrapping_sub(tail) & self.inner.mask
    }
}

impl<T, const N: usize> Drop for Consumer<T, N> {
    fn drop(&mut self) {
        // Drain remaining items to drop them properly.
        while self.try_pop().is_ok() {}
    }
}
