mod bunny;
mod config;
mod error;
mod lock;
mod s3;

use axum::{routing::any, Router};
use clap::Parser;
use tokio::net::{TcpListener, UnixListener};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use config::Config;
use s3::{handle_s3_request, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse CLI arguments
    let config = Config::parse();

    // Initialize logging
    let log_level = if config.verbose { "debug" } else { "info" };
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("bunny_s3_proxy={},tower_http=debug", log_level).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!(
        "Starting bunny-s3-proxy v{}",
        env!("CARGO_PKG_VERSION")
    );
    tracing::info!("Storage zone: {}", config.storage_zone);
    tracing::info!("Region: {}", config.region);

    // Create application state
    let state = AppState::new(config.clone());

    // Build router
    let app = Router::new()
        .route("/", any(handle_s3_request))
        .route("/{*path}", any(handle_s3_request))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    // Start server based on configuration
    if let Some(socket_path) = &config.socket_path {
        // Unix socket mode
        tracing::info!("Listening on Unix socket: {}", socket_path.display());

        // Remove existing socket file if it exists
        if socket_path.exists() {
            std::fs::remove_file(socket_path)?;
        }

        let listener = UnixListener::bind(socket_path)?;

        // Set permissions to allow connections
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o777))?;
        }

        serve_unix(listener, app).await?;
    } else {
        // TCP mode
        tracing::info!("Listening on http://{}", config.listen_addr);
        tracing::info!(
            "S3 endpoint: http://{}",
            config.listen_addr
        );
        tracing::info!(
            "Access Key ID: {}",
            config.s3_access_key_id
        );

        let listener = TcpListener::bind(config.listen_addr).await?;
        axum::serve(listener, app).await?;
    }

    Ok(())
}

async fn serve_unix(listener: UnixListener, app: Router) -> anyhow::Result<()> {
    use hyper::server::conn::http1;
    use hyper_util::rt::TokioIo;
    use tower::ServiceExt;

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let app = app.clone();

        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| {
                let app = app.clone();
                async move {
                    app.oneshot(req).await
                }
            });

            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                tracing::error!("Error serving connection: {}", err);
            }
        });
    }
}
