This repository contains a TCP echo server using Tokio. What makes it less boring is:
- Idle, read, and write timeouts
- Cancellation with `select!` and `CancellationToken`
- Tiered graceful shutdown

Two consecutive termination signals (either `SIGINT` or `SIGTERM` on Unix) result in a process exit, with throttle to protect from accidental double sends.

The shutdown process is the following:
- after first signal, cancel token is cancelled and accept loop starts awaiting connections to be closed cleanly
- after a certain (configurable) duration elapses after cancel signal was issued, the force cancel is issued and tasks are aborted
- after another 2 seconds the process exits if it still exists
