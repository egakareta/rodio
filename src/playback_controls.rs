//! Non-blocking state shared by player handles and their sample generators.

use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use crate::Float;

#[cfg(not(feature = "64bit"))]
type VolumeBits = AtomicU32;
#[cfg(feature = "64bit")]
type VolumeBits = AtomicU64;

pub(crate) struct AtomicVolume(VolumeBits);

impl AtomicVolume {
    pub(crate) fn new(value: Float) -> Self {
        Self(VolumeBits::new(value.to_bits()))
    }

    pub(crate) fn load(&self) -> Float {
        Float::from_bits(self.0.load(Ordering::Relaxed))
    }

    pub(crate) fn store(&self, value: Float) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
}

struct PositionSlot {
    seconds: AtomicU64,
    nanos: AtomicU32,
}

impl PositionSlot {
    fn new(value: Duration) -> Self {
        Self {
            seconds: AtomicU64::new(value.as_secs()),
            nanos: AtomicU32::new(value.subsec_nanos()),
        }
    }
}

/// A full-precision duration snapshot with exactly one writer: the sample generator.
///
/// The writer updates the inactive slot before publishing it. Readers never wait for a
/// writer to finish a critical section, including when an AudioWorklet is suspended.
/// A reader retries only if a newer snapshot was published during its read. All fields
/// use sequential consistency so a reused slot cannot be mistaken for the old snapshot.
pub(crate) struct AtomicPosition {
    published: AtomicU64,
    slots: [PositionSlot; 2],
}

impl AtomicPosition {
    pub(crate) fn new(value: Duration) -> Self {
        Self {
            published: AtomicU64::new(0),
            slots: [PositionSlot::new(value), PositionSlot::new(value)],
        }
    }

    /// Only the owning sample generator may publish positions.
    pub(crate) fn store(&self, value: Duration) {
        let next = self.published.load(Ordering::Relaxed).wrapping_add(1);
        let slot = &self.slots[(next & 1) as usize];
        slot.seconds.store(value.as_secs(), Ordering::SeqCst);
        slot.nanos.store(value.subsec_nanos(), Ordering::SeqCst);
        self.published.store(next, Ordering::SeqCst);
    }

    pub(crate) fn load(&self) -> Duration {
        loop {
            let published = self.published.load(Ordering::SeqCst);
            let slot = &self.slots[(published & 1) as usize];
            let seconds = slot.seconds.load(Ordering::SeqCst);
            let nanos = slot.nanos.load(Ordering::SeqCst);
            if self.published.load(Ordering::SeqCst) == published {
                return Duration::new(seconds, nanos);
            }
        }
    }
}

/// Transfers ownership of a pending command without borrowing shared mutable data.
/// Replacing a command cancels the previous one, matching the player's last-seek-wins policy.
pub(crate) struct AtomicOption<T> {
    pointer: AtomicPtr<T>,
    owned: PhantomData<T>,
}

// SAFETY: values are accessed only after an atomic swap transfers exclusive ownership
// of their Box. No references into the stored value are exposed. T need only be Send,
// like the value protected by a Mutex, not Sync.
unsafe impl<T: Send> Sync for AtomicOption<T> {}

impl<T> AtomicOption<T> {
    pub(crate) fn new() -> Self {
        Self {
            pointer: AtomicPtr::new(ptr::null_mut()),
            owned: PhantomData,
        }
    }

    pub(crate) fn replace(&self, value: T) {
        let previous = self
            .pointer
            .swap(Box::into_raw(Box::new(value)), Ordering::AcqRel);
        if !previous.is_null() {
            // SAFETY: this swap exclusively removed the Box allocated by replace.
            drop(unsafe { Box::from_raw(previous) });
        }
    }

    pub(crate) fn take(&self) -> Option<T> {
        let previous = self.pointer.swap(ptr::null_mut(), Ordering::AcqRel);
        if previous.is_null() {
            None
        } else {
            // SAFETY: this swap exclusively removed the Box allocated by replace.
            Some(*unsafe { Box::from_raw(previous) })
        }
    }
}

impl<T> Drop for AtomicOption<T> {
    fn drop(&mut self) {
        let pointer = *self.pointer.get_mut();
        if !pointer.is_null() {
            // SAFETY: exclusive access to self means no swaps can still access this Box.
            drop(unsafe { Box::from_raw(pointer) });
        }
    }
}
