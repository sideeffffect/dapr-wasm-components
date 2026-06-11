use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context as _;
use clap::{Parser, ValueEnum};
use dapr_sdk::dapr::proto::runtime::v1::app_callback_server::AppCallbackServer;
use tokio::sync::Mutex;

use dapr_wasm_host::backend::{memory::MemoryBackend, sidecar::SidecarBackend, DaprBackend};
use dapr_wasm_host::callback::GuestCallbackService;
use dapr_wasm_host::runner::GuestRunner;

/// Run a `dapr:client` WebAssembly app component against a Dapr sidecar.
///
/// The component's imports (state, pubsub, secrets, bindings, invocation,
/// configuration) are bridged to the sidecar via the Dapr Rust SDK. If the
/// component subscribes to pub/sub topics, an app-callback gRPC server is
/// started for the sidecar to deliver messages to.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to the WebAssembly component (wasm32-wasip2, world dapr:client/app).
    component: PathBuf,

    /// Which Dapr backend to bridge the component to.
    #[arg(long, value_enum, default_value_t = Backend::Sidecar)]
    backend: Backend,

    /// Listen address for the app-callback gRPC server (only started when
    /// the component subscribes to at least one topic). The Dapr sidecar
    /// must be configured with this app port and --app-protocol grpc.
    #[arg(long, default_value = "127.0.0.1:50051")]
    app_listen: String,
}

#[derive(Clone, Copy, ValueEnum)]
enum Backend {
    /// Bridge to a real Dapr sidecar over gRPC.
    Sidecar,
    /// In-memory stub backend (for trying out components without Dapr).
    Memory,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let backend: Box<dyn DaprBackend> = match args.backend {
        Backend::Sidecar => Box::new(
            SidecarBackend::connect()
                .await
                .context("failed to connect to the Dapr sidecar")?,
        ),
        Backend::Memory => Box::new(MemoryBackend::new()),
    };

    let mut runner = GuestRunner::load(&args.component, backend).await?;

    let subscriptions = runner.list_topic_subscriptions().await?;

    match runner.run().await? {
        Ok(summary) => println!("component finished: {summary}"),
        Err(error) => anyhow::bail!("component returned an error: {error}"),
    }

    if subscriptions.is_empty() {
        return Ok(());
    }

    for s in &subscriptions {
        println!(
            "subscribing to topic {} on pubsub {}",
            s.topic, s.pubsub_name
        );
    }

    let addr = args
        .app_listen
        .parse()
        .with_context(|| format!("invalid listen address {}", args.app_listen))?;
    let service = GuestCallbackService::new(Arc::new(Mutex::new(runner)), subscriptions);

    println!("app-callback gRPC server listening on {addr} (ctrl-c to stop)");
    tonic::transport::Server::builder()
        .add_service(AppCallbackServer::new(service))
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}
