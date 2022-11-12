mod replication;
mod types;
use anyhow::Result;
use replication::Transaction;
use tokio::task;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "logicaldecoding=info")
    }
    tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let (tx, _rx) = tokio::sync::broadcast::channel::<Transaction>(100);

    let streaming_handle =
        task::spawn(async { replication::start_streaming_changes("postgres", ready_tx, tx).await });

    // block waiting for replication
    ready_rx.await.unwrap();

    streaming_handle.await.unwrap().unwrap();

    Ok(())
}
