//background hub with a tray icon on windows and macos
#![windows_subsystem = "windows"]

#[cfg(any(windows, target_os = "macos"))]
mod tray;

use std::path::PathBuf;

use anyhow::Result;
#[cfg(any(windows, target_os = "macos"))]
use sampler_hub::cli_path;
use sampler_hub::{config, hub};
use tokio::sync::oneshot;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let data_dir = match (args.next().as_deref(), args.next()) {
        (Some("--data-dir"), Some(dir)) => PathBuf::from(dir),
        _ => config::default_data_dir()?,
    };
    hub::init_logging(&data_dir, true)?;

    //installers only copy the binaries
    #[cfg(any(windows, target_os = "macos"))]
    if let Err(e) = cli_path::add_self() {
        tracing::warn!("cannot add sampler-hub to the path: {e:#}");
    }

    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    //the tray opens pairing codes on it too
    let runtime = std::sync::Arc::new(tokio::runtime::Runtime::new()?);
    let hub_thread = std::thread::spawn({
        let data_dir = data_dir.clone();
        let runtime = runtime.clone();
        move || {
            let result = runtime.block_on(hub::run(&data_dir, async {
                let _ = stop_rx.await;
            }));
            if let Err(e) = result {
                tracing::error!("hub stopped: {e:#}");
                //no tray left to quit from
                std::process::exit(1);
            }
        }
    });

    //both oses want the tray on the main thread
    #[cfg(any(windows, target_os = "macos"))]
    return tray::run(data_dir, runtime.handle().clone(), stop_tx, hub_thread);

    //no tray here so run until killed
    #[cfg(not(any(windows, target_os = "macos")))]
    {
        let _keep_running = (stop_tx, runtime);
        let _ = hub_thread.join();
        Ok(())
    }
}
