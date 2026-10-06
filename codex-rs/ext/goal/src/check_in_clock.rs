//! Injectable clock and single-slot timer for goal check-ins.
//!
//! The background-wait policy computes deadlines as fake-time `Duration`
//! values. Production re-entry needs a real timer: [`CheckInTimer`] sleeps on
//! the injected [`CheckInClock`] without holding the goal semaphore, then runs
//! one callback. Only the newest timer survives; every invalidation aborts the
//! pending one through the hook the runtime installs on the wait state, and a
//! stale timer that still fires is rejected by the state's registration claim.

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

type TimerSlot = Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>;

fn abort_timer_slot(slot: &Mutex<Option<tokio::task::JoinHandle<()>>>) {
    if let Some(handle) = slot.lock().unwrap_or_else(PoisonError::into_inner).take() {
        handle.abort();
    }
}

/// Single-slot abortable timer for one check-in deadline.
///
/// Spawning replaces any pending timer. The callback runs only after the
/// clock reaches the deadline, on a task that holds no goal semaphore permit.
pub struct CheckInTimer {
    clock: Arc<dyn CheckInClock>,
    slot: TimerSlot,
}

impl CheckInTimer {
    /// Creates a timer driven by `clock` with nothing scheduled.
    pub fn new(clock: Arc<dyn CheckInClock>) -> Self {
        Self {
            clock,
            slot: Arc::new(Mutex::new(None)),
        }
    }

    /// Clock driving this timer.
    pub fn clock(&self) -> &Arc<dyn CheckInClock> {
        &self.clock
    }

    /// Schedules `on_fire` at `deadline`, aborting any pending timer.
    pub fn spawn<F>(&self, deadline: Duration, on_fire: impl FnOnce() -> F + Send + 'static)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.cancel();
        let clock = Arc::clone(&self.clock);
        let handle = tokio::spawn(async move {
            clock.sleep_until(deadline).await;
            on_fire().await;
        });
        *self.slot.lock().unwrap_or_else(PoisonError::into_inner) = Some(handle);
    }

    /// Aborts the pending timer, if any.
    pub fn cancel(&self) {
        abort_timer_slot(&self.slot);
    }

    /// Abort closure for the wait-state invalidation hook.
    pub fn abort_hook(&self) -> impl Fn() + Send + Sync + 'static {
        let slot = Arc::clone(&self.slot);
        move || abort_timer_slot(&slot)
    }
}
