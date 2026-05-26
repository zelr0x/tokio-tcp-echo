use tokio::signal;
use tokio::{
    select,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
use tokio::signal::unix;

pub struct ShutdownManager {
    cancel: CancellationToken,
    force_cancel: CancellationToken,
}

impl ShutdownManager {
    pub fn new(force_timeout: Duration) -> Self {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let force_cancel = CancellationToken::new();
        let force_cancel_clone = force_cancel.clone();

        tokio::task::spawn(async move {
            let mut first_signal: Option<Instant> = None;

            #[cfg(unix)]
            let mut sigterm = signal(SignalKind::terminate()).expect("Failed to bind SIGTERM");
            loop {
                let cancel = cancel_clone.clone();
                let force_cancel = force_cancel_clone.clone();

                #[cfg(not(unix))]
                select! {
                    _ = signal::ctrl_c() => {
                        Self::handle_signal(cancel, force_cancel, force_timeout, &mut first_signal);
                    }
                }
                #[cfg(unix)]
                select! {
                    _ = signal::ctrl_c() => {
                        Self::handle_signal(cancel, force_cancel, force_timeout, &mut first_signal);
                    }

                    _ = sigterm.recv() => {
                        Self::handle_signal(cancel, force_cancel, force_timeout, &mut first_signal);
                    }
                }
            }
        });
        Self {
            cancel,
            force_cancel,
        }
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub fn force_cancel_token(&self) -> CancellationToken {
        self.force_cancel.clone()
    }

    fn handle_signal(
        cancel: CancellationToken,
        force_cancel: CancellationToken,
        force_timeout: Duration,
        first_signal: &mut Option<Instant>,
    ) {
        let now = Instant::now();
        match *first_signal {
            Some(first) => {
                // Throttle.
                if now.duration_since(first) < Duration::from_secs(1) {
                    return;
                }
                eprintln!("Force quit");
                std::process::exit(1)
            }
            None => {
                first_signal.replace(now);
                cancel.cancel();
                eprintln!("Shutting down...");
                tokio::task::spawn(async move {
                    tokio::time::sleep(force_timeout).await;
                    force_cancel.cancel();
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    std::process::exit(1);
                });
            }
        }
    }
}
