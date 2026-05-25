This repository contains a TCP echo server using Tokio. What makes it less boring is:
- Idle, read, and write timeouts
- Cancellation with `select!` and `CancellationToken`
- Tiered graceful shutdown, with two consecutive termination signals (either `SIGINT` or `SIGTERM` on Unix) resulting in a forceful shutdown, and with throttle to protect from accidental double sends
