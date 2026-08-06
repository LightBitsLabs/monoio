use std::{
    future::poll_fn,
    task::Poll,
    time::{Duration, Instant},
};

use futures::{channel::mpsc, StreamExt};

/// Timers must advance even while runnable tasks keep the runtime away from
/// `Driver::park()`. The watchdog keeps the run queue non-empty, forcing the
/// runtime through `Driver::submit()` until either the timer fires or the
/// regression bound expires.
#[monoio::test_all(timer_enabled = true)]
async fn timer_progresses_while_runtime_only_submits() {
    let (tx, mut rx) = mpsc::unbounded::<bool>();

    let timer_tx = tx.clone();
    monoio::spawn(async move {
        monoio::time::sleep(Duration::from_millis(20)).await;
        let _ = timer_tx.unbounded_send(true);
    });

    monoio::spawn(async move {
        let started = Instant::now();
        poll_fn(move |cx| {
            if started.elapsed() >= Duration::from_millis(250) {
                let _ = tx.unbounded_send(false);
                return Poll::Ready(());
            }

            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await;
    });

    assert_eq!(
        rx.next().await,
        Some(true),
        "timer did not progress while the runtime stayed on the submit path"
    );
}
