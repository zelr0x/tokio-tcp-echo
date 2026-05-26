use std::{net::SocketAddr, time::Duration};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    net::{TcpListener, TcpStream},
    select,
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ServerTimeouts {
    pub read: Duration,
    pub write: Duration,
    pub idle: Duration,
}

impl Default for ServerTimeouts {
    fn default() -> Self {
        Self::same(Duration::from_secs(5))
    }
}

impl ServerTimeouts {
    #[allow(dead_code)]
    fn new() -> Self {
        ServerTimeouts::default()
    }

    fn same(timeout: Duration) -> Self {
        ServerTimeouts {
            idle: timeout,
            read: timeout,
            write: timeout,
        }
    }
}

#[derive(Debug)]
pub struct TcpEchoServer {
    cancel: CancellationToken,
    addr: SocketAddr,
    timeouts: ServerTimeouts,
    accept_join_handle: JoinHandle<()>,
}

impl TcpEchoServer {
    pub async fn start(
        cancel: CancellationToken,
        force_cancel: CancellationToken,
        addr: &str,
        port: u16,
        max_connections: usize,
        timeouts: ServerTimeouts,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind((addr, port)).await?;
        let local_addr = listener.local_addr()?;

        let cancel_clone = cancel.clone();
        let mut connections = JoinSet::new();

        let accept_join_handle = tokio::task::spawn(async move {
            loop {
                select! {
                    _ = cancel_clone.cancelled() => {
                        break;
                    }
                    Ok((sock, client_addr)) = listener.accept() => {
                        // Warning: no per-IP quotas.
                        if max_connections > 0 && connections.len() >= max_connections {
                            drop(sock);
                            continue
                        }
                        let cancel = cancel_clone.clone();
                        connections.spawn(async move {
                            Self::process(cancel, sock, client_addr, timeouts).await;
                        });
                    }
                }
            }
            drop(listener);
            select! {
                _ = force_cancel.cancelled() => {
                    connections.shutdown().await;
                }
                _ = async {
                    while connections.join_next().await.is_some() {}
                } => {}
            }
        });
        Ok(Self {
            cancel,
            addr: local_addr,
            timeouts,
            accept_join_handle,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[allow(dead_code)]
    pub fn timeouts(&self) -> ServerTimeouts {
        self.timeouts
    }

    #[allow(dead_code)]
    pub async fn shutdown(self) {
        self.cancel.cancel();
        self.block().await;
    }

    pub async fn block(self) {
        let _ = self.accept_join_handle.await;
    }

    async fn process(
        cancel: CancellationToken,
        mut sock: TcpStream,
        _client_addr: SocketAddr,
        timeouts: ServerTimeouts,
    ) {
        let mut buf = [0u8; 1024];
        let idle = tokio::time::sleep(timeouts.idle);
        tokio::pin!(idle);
        loop {
            let n = select! {
                _ = cancel.cancelled() => {
                    _ = sock.shutdown().await;
                    return
                }
                _ = &mut idle => {
                    drop(sock);
                    #[cfg(debug_assertions)]
                    println!("Idle timeout elapsed, closing connection");
                    return
                }
                read_res = timeout(timeouts.read, sock.read(&mut buf)) => {
                    match read_res {
                        Ok(Ok(0)) => {  // Connection closed.
                            return
                        }
                        Ok(Ok(n)) => n,
                        Ok(Err(e)) => {
                            drop(sock);
                            #[cfg(debug_assertions)]
                            println!("Error reading from socket: {}", e);
                            return
                        }
                        Err(e) => {
                            drop(sock);
                            #[cfg(debug_assertions)]
                            println!("Socket read timed out: {}", e);
                            return
                        },
                    }
                }
            };
            select! {
                _ = cancel.cancelled() => {
                    _ = sock.shutdown().await;
                    return
                }
                write_res = timeout(timeouts.write, sock.write_all(&buf[..n])) => {
                    match write_res {
                        Ok(Err(e)) => {
                            #[cfg(debug_assertions)]
                            println!("Error writing to socket: {}", e);
                            return
                        },
                        Err(e) => {
                            #[cfg(debug_assertions)]
                            println!("Socket write timed out: {}", e);
                            return
                        },
                        _ => {
                            // Consider only successful RTT as activity.
                            idle.as_mut().reset(Instant::now() + timeouts.idle);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Builder;

    #[test]
    fn serve_works() {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let cancel = CancellationToken::new();
            let force_cancel = CancellationToken::new();
            let server = TcpEchoServer::start(
                cancel,
                force_cancel,
                "localhost",
                0,
                0,
                ServerTimeouts::default(),
            )
            .await
            .unwrap();
            let addr = server.addr();
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(b"hello").await.unwrap();
            let mut buf = vec![0; 5];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello");
            server.shutdown().await;
        });
    }
}
