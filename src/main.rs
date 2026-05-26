mod server;
mod shutdown;

use std::time::Duration;

use clap::Parser;

use crate::{
    server::{ServerTimeouts, TcpEchoServer},
    shutdown::ShutdownManager,
};

#[derive(Debug, Parser)]
pub struct Args {
    #[arg(short, long, default_value = "127.0.0.1")]
    addr: String,
    #[arg(short, long, default_value_t = 0u16)]
    port: u16,
    #[arg(long, default_value_t = 100_000)]
    max_connections: usize,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let sm = ShutdownManager::new(Duration::from_secs(30));
    let timeouts = ServerTimeouts::default();
    let server = TcpEchoServer::start(
        sm.cancel_token(),
        sm.force_cancel_token(),
        &args.addr,
        args.port,
        args.max_connections,
        timeouts,
    )
    .await?;
    let addr = server.addr();
    println!("Listening on {}:{}", addr.ip(), addr.port());
    server.block().await;
    Ok(())
}
