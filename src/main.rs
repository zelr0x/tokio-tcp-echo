mod shutdown;
mod server;

use clap::Parser;
use tokio_util::sync::CancellationToken;

use crate::server::{ServerTimeouts, TcpEchoServer};

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
    let cancel = CancellationToken::new();
    let timeouts = ServerTimeouts::default();
    let server = TcpEchoServer::start(
        cancel.clone(),
        &args.addr,
        args.port,
        args.max_connections,
        timeouts
    ).await?;
    let addr = server.addr();
    println!("Listening on {}:{}", addr.ip(), addr.port());
    shutdown::register(cancel.clone());
    server.block().await;
    Ok(())
}
