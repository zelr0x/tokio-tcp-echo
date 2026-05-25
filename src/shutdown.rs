use tokio::signal;
use tokio::{
    select,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
use tokio::signal::unix;

pub fn register(cancel: CancellationToken) {
    let mut last_signal: Option<Instant> = None;

    #[cfg(unix)]
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to bind SIGTERM");

    tokio::task::spawn(async move {
        loop {
            let cancel = cancel.clone();

            #[cfg(not(unix))]
            select! {
                _ = signal::ctrl_c() => {
                    handle_signal(cancel, &mut last_signal);
                }
            }
            #[cfg(unix)]
            select! {
                _ = signal::ctrl_c() => {
                    handle_signal(cancel, &mut last_signal);
                }

                _ = sigterm.recv() => {
                    handle_signal(cancel, &mut last_signal);
                }
            }
        }
    });
}

fn handle_signal(cancel: CancellationToken, last_signal: &mut Option<Instant>) {
    let now = Instant::now();
    if let Some(last) = *last_signal {
        // Throttle.
        if now.duration_since(last) < Duration::from_secs(1) {
            return;
        }
        eprintln!("Force quit");
        std::process::exit(1);
    }
    cancel.cancel();
    eprintln!("Shutting down...");
    last_signal.replace(now);
}
