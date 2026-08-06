use std::{
    any::type_name,
    cell::UnsafeCell,
    env,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll, Waker},
    time::Instant,
};

use super::{
    raw::{self, Vtable},
    state::State,
    utils::UnsafeCellExt,
    Schedule,
};

const LONG_POLL_THRESHOLD_UNINITIALIZED: u64 = u64::MAX;
static LONG_POLL_THRESHOLD_US: AtomicU64 = AtomicU64::new(LONG_POLL_THRESHOLD_UNINITIALIZED);
#[cfg(test)]
static LONG_POLL_REPORTS: AtomicU64 = AtomicU64::new(0);

fn parse_long_poll_threshold(value: Option<&str>) -> u64 {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|threshold| *threshold > 0)
        .unwrap_or(0)
}

fn long_poll_threshold_us() -> u64 {
    let threshold = LONG_POLL_THRESHOLD_US.load(Ordering::Relaxed);
    if threshold != LONG_POLL_THRESHOLD_UNINITIALIZED {
        return threshold;
    }

    let configured = env::var("LCF_MONOIO_HOLD_US").ok();
    let configured = parse_long_poll_threshold(configured.as_deref());
    let _ = LONG_POLL_THRESHOLD_US.compare_exchange(
        LONG_POLL_THRESHOLD_UNINITIALIZED,
        configured,
        Ordering::Relaxed,
        Ordering::Relaxed,
    );
    LONG_POLL_THRESHOLD_US.load(Ordering::Relaxed)
}

fn report_long_poll<T>(elapsed_us: u128) {
    eprintln!("MONOIO_LONG_POLL us={elapsed_us} task={}", type_name::<T>());
    #[cfg(test)]
    LONG_POLL_REPORTS.fetch_add(1, Ordering::Relaxed);
}

#[repr(C)]
pub(crate) struct Cell<T: Future, S> {
    pub(crate) header: Header,
    pub(crate) core: Core<T, S>,
    pub(crate) trailer: Trailer,
}

pub(crate) struct Core<T: Future, S> {
    /// Scheduler used to drive this future
    pub(crate) scheduler: S,
    /// Either the future or the output
    pub(crate) stage: CoreStage<T>,
}
pub(crate) struct CoreStage<T: Future> {
    stage: UnsafeCell<Stage<T>>,
}

pub(crate) enum Stage<T: Future> {
    Running(T),
    Finished(T::Output),
    Consumed,
}

#[repr(C)]
pub(crate) struct Header {
    /// State
    pub(crate) state: State,
    /// Table of function pointers for executing actions on the task.
    pub(crate) vtable: &'static Vtable,
    /// Thread ID(sync: used for wake task on its thread; sync disabled: do checking)
    pub(crate) owner_id: usize,
}

pub(crate) struct Trailer {
    /// Consumer task waiting on completion of this task.
    pub(crate) waker: UnsafeCell<Option<Waker>>,
}

impl<T: Future, S: Schedule> Cell<T, S> {
    /// Allocates a new task cell, containing the header, trailer, and core
    /// structures.
    pub(crate) fn new(owner_id: usize, future: T, scheduler: S) -> Box<Cell<T, S>> {
        Box::new(Cell {
            header: Header {
                state: State::new(),
                vtable: raw::vtable::<T, S>(),
                owner_id,
            },
            core: Core {
                scheduler,
                stage: CoreStage {
                    stage: UnsafeCell::new(Stage::Running(future)),
                },
            },
            trailer: Trailer {
                waker: UnsafeCell::new(None),
            },
        })
    }
}

impl<T: Future> CoreStage<T> {
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut Stage<T>) -> R) -> R {
        self.stage.with_mut(f)
    }

    pub(crate) fn poll(&self, mut cx: Context<'_>) -> Poll<T::Output> {
        let long_poll_threshold_us = long_poll_threshold_us();
        let poll_started = (long_poll_threshold_us != 0).then(Instant::now);
        let res = {
            self.with_mut(|ptr| {
                // Safety: The caller ensures mutual exclusion to the field.
                let future = match unsafe { &mut *ptr } {
                    Stage::Running(future) => future,
                    _ => unreachable!("unexpected stage"),
                };

                // Safety: The caller ensures the future is pinned.
                let future = unsafe { Pin::new_unchecked(future) };

                future.poll(&mut cx)
            })
        };

        if let Some(poll_started) = poll_started {
            let elapsed_us = poll_started.elapsed().as_micros();
            if elapsed_us >= u128::from(long_poll_threshold_us) {
                report_long_poll::<T>(elapsed_us);
            }
        }

        if res.is_ready() {
            self.drop_future_or_output();
        }

        res
    }

    /// Drop the future
    ///
    /// # Safety
    ///
    /// The caller must ensure it is safe to mutate the `stage` field.
    pub(crate) fn drop_future_or_output(&self) {
        // Safety: the caller ensures mutual exclusion to the field.
        unsafe {
            self.set_stage(Stage::Consumed);
        }
    }

    /// Store the task output
    ///
    /// # Safety
    ///
    /// The caller must ensure it is safe to mutate the `stage` field.
    pub(crate) fn store_output(&self, output: T::Output) {
        // Safety: the caller ensures mutual exclusion to the field.
        unsafe {
            self.set_stage(Stage::Finished(output));
        }
    }

    /// Take the task output
    ///
    /// # Safety
    ///
    /// The caller must ensure it is safe to mutate the `stage` field.
    pub(crate) fn take_output(&self) -> T::Output {
        use std::mem;

        self.with_mut(|ptr| {
            // Safety:: the caller ensures mutual exclusion to the field.
            match mem::replace(unsafe { &mut *ptr }, Stage::Consumed) {
                Stage::Finished(output) => output,
                _ => panic!("JoinHandle polled after completion"),
            }
        })
    }

    unsafe fn set_stage(&self, stage: Stage<T>) {
        self.with_mut(|ptr| *ptr = stage)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::UnsafeCell,
        future::Future,
        pin::Pin,
        sync::atomic::Ordering,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use super::{
        parse_long_poll_threshold, CoreStage, Stage, LONG_POLL_REPORTS, LONG_POLL_THRESHOLD_US,
    };

    struct SlowReady;

    struct FastReady;

    impl Future for FastReady {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(())
        }
    }

    impl Future for SlowReady {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            std::thread::sleep(Duration::from_millis(2));
            Poll::Ready(())
        }
    }

    #[test]
    fn long_poll_threshold_is_disabled_for_missing_or_invalid_values() {
        assert_eq!(parse_long_poll_threshold(None), 0);
        assert_eq!(parse_long_poll_threshold(Some("")), 0);
        assert_eq!(parse_long_poll_threshold(Some("invalid")), 0);
        assert_eq!(parse_long_poll_threshold(Some("0")), 0);
    }

    #[test]
    fn long_poll_threshold_uses_positive_microseconds() {
        assert_eq!(parse_long_poll_threshold(Some("200")), 200);
    }

    #[test]
    fn long_poll_diagnostic_filters_real_core_stage_polls() {
        let prior_threshold = LONG_POLL_THRESHOLD_US.swap(u64::MAX - 1, Ordering::Relaxed);
        let reports_before = LONG_POLL_REPORTS.load(Ordering::Relaxed);
        let fast_stage = CoreStage {
            stage: UnsafeCell::new(Stage::Running(FastReady)),
        };
        let waker = Waker::noop();

        assert!(fast_stage.poll(Context::from_waker(waker)).is_ready());
        assert_eq!(
            LONG_POLL_REPORTS.load(Ordering::Relaxed),
            reports_before,
            "a poll below the configured threshold must not be reported"
        );

        LONG_POLL_THRESHOLD_US.store(1, Ordering::Relaxed);
        let stage = CoreStage {
            stage: UnsafeCell::new(Stage::Running(SlowReady)),
        };

        assert!(stage.poll(Context::from_waker(waker)).is_ready());
        assert_eq!(
            LONG_POLL_REPORTS.load(Ordering::Relaxed),
            reports_before + 1,
            "the production CoreStage::poll path must emit one diagnostic"
        );
        LONG_POLL_THRESHOLD_US.store(prior_threshold, Ordering::Relaxed);
    }
}

impl Header {
    #[allow(unused)]
    pub(crate) fn get_owner_id(&self) -> usize {
        // safety: If there are concurrent writes, then that write has violated
        // the safety requirements on `set_owner_id`.
        self.owner_id
    }
}

impl Trailer {
    pub(crate) unsafe fn set_waker(&self, waker: Option<Waker>) {
        self.waker.with_mut(|ptr| {
            *ptr = waker;
        });
    }

    pub(crate) unsafe fn will_wake(&self, waker: &Waker) -> bool {
        self.waker
            .with(|ptr| (*ptr).as_ref().unwrap().will_wake(waker))
    }

    pub(crate) fn wake_join(&self) {
        self.waker.with(|ptr| match unsafe { &*ptr } {
            Some(waker) => waker.wake_by_ref(),
            None => panic!("waker missing"),
        });
    }
}
