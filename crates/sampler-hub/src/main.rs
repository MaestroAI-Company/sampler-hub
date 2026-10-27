use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use iroh_tickets::endpoint::EndpointTicket;
use sampler_hub::cli_path;
use sampler_hub::db::Db;
use sampler_hub::service::{self, Launch, Status};
use sampler_hub::store::{self, Store};
use sampler_hub::{config, hub, invite};
use sampler_hub_proto::{ProfileInfo, ProfileRole};

#[derive(Parser)]
#[command(
    name = "sampler-hub",
    version,
    about = "Sampler-Hub, the sync server for Opera devices"
)]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "Hub data directory, defaults to the OS data directory"
    )]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Run the hub in the foreground")]
    Run,
    #[command(about = "Show a one-time QR code and code to pair a device")]
    Pair,
    #[command(about = "List or remove paired devices")]
    Devices {
        #[command(subcommand)]
        action: Option<DevicesAction>,
    },
    #[command(about = "List, remove or promote profiles")]
    Profiles {
        #[command(subcommand)]
        action: Option<ProfilesAction>,
    },
    #[command(about = "Print the hub ticket and its QR code, for ping")]
    Ticket,
    #[command(about = "Check that a hub is reachable")]
    Ping {
        ticket: EndpointTicket,
        #[arg(short, long, default_value_t = 5)]
        count: u32,
    },
    #[command(about = "Manage the hub background service")]
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
}

#[derive(Subcommand)]
enum DevicesAction {
    List,
    #[command(about = "Unpair a device by id or id prefix")]
    Remove {
        id: String,
    },
}

#[derive(Subcommand)]
enum ProfilesAction {
    List,
    #[command(about = "Remove a profile, its data and its devices")]
    Remove {
        id: String,
    },
    #[command(about = "Let a profile manage the hub")]
    Promote {
        id: String,
    },
}

#[derive(Subcommand)]
enum ServiceAction {
    Install,
    Uninstall,
    Start,
    Stop,
    Status,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let data_dir = match cli.data_dir {
        Some(dir) => dir,
        None => config::default_data_dir()?,
    };

    match cli.command {
        Command::Run => {
            hub::init_logging(&data_dir, false)?;
            runtime()?.block_on(hub::run(&data_dir, shutdown_signal()))
        }
        Command::Pair => {
            hub::init_logging(&data_dir, false)?;
            runtime()?.block_on(async {
                let invite = invite::open(&data_dir).await?;
                let link = invite.link.to_string();
                qr2term::print_qr(&link)?;
                println!("{link}");
                println!("code: {}", invite.code);
                println!(
                    "scan the QR code or type the code in Opera within {} minutes, the hub must be running",
                    store::PAIRING_TTL.as_secs() / 60
                );
                //the code is answered from here
                tokio::select! {
                    used = invite.done => {
                        if used.unwrap_or(false) {
                            println!("code used, the device finishes pairing");
                        } else {
                            println!("the code expired");
                        }
                    }
                    _ = shutdown_signal() => {}
                }
                Ok(())
            })
        }
        Command::Devices { action } => devices(&data_dir, action.unwrap_or(DevicesAction::List)),
        Command::Profiles { action } => profiles(&data_dir, action.unwrap_or(ProfilesAction::List)),
        Command::Ticket => {
            let ticket = hub::ticket(&data_dir)?.to_string();
            qr2term::print_qr(&ticket)?;
            println!("{ticket}");
            Ok(())
        }
        Command::Ping { ticket, count } => {
            hub::init_logging(&data_dir, false)?;
            runtime()?.block_on(hub::ping(&data_dir, ticket, count))
        }
        Command::Service { action } => match action {
            ServiceAction::Install => {
                service::install(&launch(&data_dir)?)?;
                println!("installed, the hub starts with your session and runs now");
                match cli_path::add_self() {
                    Ok(()) => println!("open a new terminal to type sampler-hub directly"),
                    Err(e) => eprintln!("cannot add sampler-hub to your PATH: {e:#}"),
                }
                if cfg!(target_os = "linux") {
                    println!(
                        "to start it at boot without a login session, run: loginctl enable-linger $USER"
                    );
                }
                Ok(())
            }
            ServiceAction::Uninstall => {
                service::uninstall()?;
                //cleanup failure is not worth an error
                let _ = cli_path::remove_self();
                println!("uninstalled");
                Ok(())
            }
            ServiceAction::Start => service::start(&launch(&data_dir)?),
            ServiceAction::Stop => service::stop(),
            ServiceAction::Status => {
                let status = match service::status()? {
                    Status::NotInstalled => "not installed",
                    Status::Running => "running",
                    Status::Stopped => "stopped",
                };
                println!("{status}");
                Ok(())
            }
        },
    }
}

fn devices(data_dir: &Path, action: DevicesAction) -> Result<()> {
    let store = Store::new(data_dir);
    match action {
        DevicesAction::List => {
            let devices = store.devices()?;
            if devices.is_empty() {
                println!("no paired device, run: sampler-hub pair");
            }
            for device in devices {
                println!(
                    "{}  {} ({}) {}  profile {}",
                    device.id,
                    device.name,
                    device.kind,
                    device.scopes.join(","),
                    device.profile.as_deref().unwrap_or("none")
                );
            }
        }
        DevicesAction::Remove { id } => {
            let matches: Vec<_> = store
                .devices()?
                .into_iter()
                .filter(|d| d.id.starts_with(&id))
                .collect();
            match matches.as_slice() {
                [device] => {
                    store.remove_device(&device.id)?;
                    println!("removed {}", device.name);
                }
                [] => bail!("no paired device matches {id}"),
                _ => bail!("{id} matches several devices, use a longer prefix"),
            }
        }
    }
    Ok(())
}

fn profiles(data_dir: &Path, action: ProfilesAction) -> Result<()> {
    let store = Store::new(data_dir);
    //ids are long so prefixes work
    let find = |id: &str| -> Result<ProfileInfo> {
        let matches: Vec<_> = store
            .profiles()?
            .into_iter()
            .filter(|p| p.id.starts_with(id))
            .collect();
        match <[ProfileInfo; 1]>::try_from(matches) {
            Ok([profile]) => Ok(profile),
            Err(matches) if matches.is_empty() => bail!("no profile matches {id}"),
            Err(_) => bail!("{id} matches several profiles, use a longer prefix"),
        }
    };
    match action {
        ProfilesAction::List => {
            let profiles = store.profiles()?;
            if profiles.is_empty() {
                println!("no profile yet, a paired device creates the first one");
            }
            let devices = store.devices()?;
            for profile in profiles {
                let count = devices
                    .iter()
                    .filter(|d| d.profile.as_deref() == Some(profile.id.as_str()))
                    .count();
                let role = match profile.role {
                    ProfileRole::Admin => "admin",
                    ProfileRole::Member => "member",
                };
                println!(
                    "{}  {} ({role}) {count} device(s)",
                    profile.id, profile.name
                );
            }
        }
        ProfilesAction::Remove { id } => {
            let profile = find(&id)?;
            //data first so nothing stays orphaned
            Db::open(data_dir)?.remove_profile(&profile.id)?;
            store.remove_profile(&profile.id)?;
            println!("removed {} with its data and devices", profile.name);
        }
        ProfilesAction::Promote { id } => {
            let profile = find(&id)?;
            store.set_role(&profile.id, ProfileRole::Admin)?;
            println!("{} now manages the hub", profile.name);
        }
    }
    Ok(())
}

//the binaries next to this one
fn launch(data_dir: &Path) -> Result<Launch> {
    Launch::next_to(&std::env::current_exe()?, data_dir)
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Runtime::new()?)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        //systemd and launchd stop with sigterm
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("sigterm handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
