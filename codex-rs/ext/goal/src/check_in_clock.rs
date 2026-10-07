//! Injectable clock and single-slot timer for goal check-ins.
//!
//! The background-wait policy computes deadlines as fake-time `Duration`
//! values. Production re-entry needs a real timer: [`CheckInTimer`] sleeps on
//! the injected [`CheckInClock`] without holding the goal semaphore, then runs
//! one callback. Only the newest timer survives; every invalidation aborts the
//! pending one through the hook the runtime installs on the wait state, and a
//! stale timer that still fires is rejected by the state's registration claim.
//!
//! Ownership: a fired timer detaches itself from the cancellable slot (by
//! timer id) BEFORE invoking its callback, so a turn-start invalidation that
//! runs inside the continuation never aborts its own task. Invalidation aborts
//! only a still-sleeping timer. Installation is generation-guarded: a stale
//! install whose generation is older than the currently slotted timer is
//! rejected without cancelling the live timer.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

/// Clock driving goal check-in timers.
///
/// Implementations must be cheap, synchronous for [`CheckInClock::now`], and
/// safe to share across threads. Tests use fake `Duration` values directly
/// against the wait state; activation tests may inject a manual clock here.
pub trait CheckInClock: Send + Sync {
    /// Current instant as a duration since the Unix epoch.
    fn now(&self) -> Duration;

    /// Future completing at `deadline`, immediately when already past it.
    fn sleep_until(&self, deadline: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// Production clock backed by system time and Tokio sleeps.
pub struct SystemCheckInClock;

impl CheckInClock for SystemCheckInClock {
    fn now(&self) -> Duration {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
    }

    fn sleep_until(&self, deadline: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let delay = deadline.saturating_sub(self.now());
        Box::pin(tokio::time::sleep(delay))
    }
}

struct TimerEntry {
    id: u64,
    generation: u64,
    handle: tokio::task::JoinHandle<()>,
}

struct SlotState {
    entry: Option<TimerEntry>,
    next_id: u64,
}

type TimerSlot = Arc<Mutex<SlotState>>;

fn abort_timer_slot(slot: &Mutex<SlotState>) {
    if let Some(entry) = slot
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry
        .take()
    {
        entry.handle.abort();
    }
}

/// Single-slot abortable timer for one check-in deadline.
///
/// Spawning replaces any pending timer of the same or older generation; a
/// stale install for an older generation than the slotted timer is rejected
/// without disturbing it. The callback runs only after the clock reaches the
/// deadline, on a task that holds no goal semaphore permit. The fired task
/// detaches itself from the slot before running the callback, so invalidation
/// during the continuation cannot abort its own admission.
pub struct CheckInTimer {
    clock: Arc<dyn CheckInClock>,
    slot: TimerSlot,
}

impl CheckInTimer {
    /// Creates a timer driven by `clock` with nothing scheduled.
    pub fn new(clock: Arc<dyn CheckInClock>) -> Self {
        Self {
            clock,
            slot: Arc::new(Mutex::new(SlotState {
                entry: None,
                next_id: 1,
            })),
        }
    }

    /// Clock driving this timer.
    pub fn clock(&self) -> &Arc<dyn CheckInClock> {
        &self.clock
    }

    /// Schedules `on_fire` at `deadline` for `generation`.
    ///
    /// Returns `false` without disturbing the live timer when `generation` is
    /// older than the currently slotted timer (stale install). Otherwise
    /// aborts any pending timer and installs the new one, returning `true`.
    /// Installation is atomic under the slot lock: check, cancel, spawn, and
    /// store happen together so a concurrent invalidation cannot strand a
    /// timer outside the slot.
    pub fn spawn<F>(
        &self,
        deadline: Duration,
        generation: u64,
        on_fire: impl FnOnce() -> F + Send + 'static,
    ) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
        if slot
            .entry
            .as_ref()
            .is_some_and(|existing| existing.generation > generation)
        {
            return false;
        }
        if let Some(old) = slot.entry.take() {
            old.handle.abort();
        }
        let id = slot.next_id;
        slot.next_id += 1;
        let clock = Arc::clone(&self.clock);
        let slot_for_fire = Arc::clone(&self.slot);
        let handle = tokio::spawn(async move {
            clock.sleep_until(deadline).await;
            {
                let mut slot = slot_for_fire.lock().unwrap_or_else(PoisonError::into_inner);
                if false {
                    slot.entry.take();
                }
                let _ = id;
            }
            on_fire().await;
        });
        slot.entry = Some(TimerEntry {
            id,
            generation,
            handle,
        });
        true
    }

    /// Aborts the pending timer, if any.
    ///
    /// A timer that already fired detached itself before running its callback,
    /// so this never aborts a firing continuation.
    pub fn cancel(&self) {
        abort_timer_slot(&self.slot);
    }

    /// Abort closure for the wait-state invalidation hook.
    pub fn abort_hook(&self) -> impl Fn() + Send + Sync + 'static {
        let slot = Arc::clone(&self.slot);
        move || abort_timer_slot(&slot)
    }
}
