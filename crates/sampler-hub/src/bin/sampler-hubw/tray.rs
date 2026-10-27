use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use sampler_hub::config::Config;
use sampler_hub::invite;
use sampler_hub::secrets::http_client;
use sampler_hub::store::Store;
use sampler_hub::update;
use tao::dpi::{LogicalSize, PhysicalPosition};
use tao::event::{ElementState, Event, StartCause, WindowEvent};
use tao::event_loop::{ControlFlow, EventLoopBuilder, EventLoopWindowTarget};
use tao::keyboard::Key;
use tao::window::{Window, WindowBuilder};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
use tray_icon::{Icon, TrayIcon, TrayIconBuilder};

const REFRESH: Duration = Duration::from_secs(5);
//github rate limits anonymous checks
const UPDATE_CHECK: Duration = Duration::from_secs(6 * 60 * 60);
const OPERA_RED: [u8; 3] = [0xFF, 0x1A, 0x1A];
const QR_PANEL_SIZE: f64 = 280.0;
//room for the typed code under the qr
const CODE_STRIP: f64 = 64.0;
//one glyph pixel per this share of the width
const CODE_COLUMNS: usize = 60;
//space between the tray icon and the panel
const QR_PANEL_GAP: f64 = 8.0;
//white border scanners need around the code
const QR_QUIET_MODULES: usize = 4;

struct Labels {
    update: &'static str,
    update_title: &'static str,
    update_message: &'static str,
    install: &'static str,
    later: &'static str,
    update_failed: &'static str,
    pair: &'static str,
    scan: &'static str,
    quit: &'static str,
    none: &'static str,
    one: &'static str,
    many: &'static str,
}

fn labels() -> Labels {
    let french = sys_locale::get_locale().is_some_and(|l| l.starts_with("fr"));
    if french {
        Labels {
            update: "Installer Sampler-Hub {version}",
            update_title: "Mise à jour d'Sampler-Hub",
            update_message: "Sampler-Hub {version} est disponible (vous avez la {current}). L'installer maintenant ? Le hub redémarre en quelques secondes et vos appareils se reconnectent tout seuls.",
            install: "Installer",
            later: "Plus tard",
            update_failed: "La mise à jour n'a pas pu être téléchargée :",
            pair: "Appairer un appareil",
            scan: "Scannez ou tapez le code dans Opera · 10 min",
            quit: "Quitter",
            none: "Aucun appareil appairé",
            one: "1 appareil appairé",
            many: "appareils appairés",
        }
    } else {
        Labels {
            update: "Install Sampler-Hub {version}",
            update_title: "Sampler-Hub update",
            update_message: "Sampler-Hub {version} is available (you have {current}). Install it now? The hub restarts in a few seconds and your devices reconnect on their own.",
            install: "Install",
            later: "Later",
            update_failed: "The update could not be downloaded:",
            pair: "Pair a device",
            scan: "Scan or type the code in Opera · 10 min",
            quit: "Quit",
            none: "No paired device",
            one: "1 paired device",
            many: "paired devices",
        }
    }
}

fn devices_label(labels: &Labels, data_dir: &Path) -> String {
    match Store::new(data_dir).devices().map(|d| d.len()).unwrap_or(0) {
        0 => labels.none.into(),
        1 => labels.one.into(),
        n => format!("{n} {}", labels.many),
    }
}

//template on macos follows the menu bar theme
fn icon() -> Result<Icon> {
    let decoder = png::Decoder::new(std::io::Cursor::new(include_bytes!(
        "../../../assets/tray.png"
    )));
    let mut reader = decoder.read_info()?;
    let mut rgba = vec![0; reader.output_buffer_size().context("icon too large")?];
    let info = reader.next_frame(&mut rgba)?;
    rgba.truncate(info.buffer_size());
    //black disappears on a dark windows taskbar
    if cfg!(windows) {
        for pixel in rgba.as_chunks_mut::<4>().0 {
            pixel[..3].copy_from_slice(&OPERA_RED);
        }
    }
    Ok(Icon::from_rgba(rgba, info.width, info.height)?)
}

struct PairingWindow {
    window: Rc<Window>,
    surface: softbuffer::Surface<Rc<Window>, Rc<Window>>,
    dark: Vec<bool>,
    modules: usize,
    code: String,
}

//5x7 glyphs, the panel loads no font
const FONT: [(char, [u8; 7]); 33] = [
    (
        '0',
        [
            0b01110, 0b10001, 0b10011, 0b10101, 0b11001, 0b10001, 0b01110,
        ],
    ),
    (
        '1',
        [
            0b00100, 0b01100, 0b00100, 0b00100, 0b00100, 0b00100, 0b01110,
        ],
    ),
    (
        '2',
        [
            0b01110, 0b10001, 0b00001, 0b00010, 0b00100, 0b01000, 0b11111,
        ],
    ),
    (
        '3',
        [
            0b11111, 0b00010, 0b00100, 0b00010, 0b00001, 0b10001, 0b01110,
        ],
    ),
    (
        '4',
        [
            0b00010, 0b00110, 0b01010, 0b10010, 0b11111, 0b00010, 0b00010,
        ],
    ),
    (
        '5',
        [
            0b11111, 0b10000, 0b11110, 0b00001, 0b00001, 0b10001, 0b01110,
        ],
    ),
    (
        '6',
        [
            0b00110, 0b01000, 0b10000, 0b11110, 0b10001, 0b10001, 0b01110,
        ],
    ),
    (
        '7',
        [
            0b11111, 0b00001, 0b00010, 0b00100, 0b01000, 0b01000, 0b01000,
        ],
    ),
    (
        '8',
        [
            0b01110, 0b10001, 0b10001, 0b01110, 0b10001, 0b10001, 0b01110,
        ],
    ),
    (
        '9',
        [
            0b01110, 0b10001, 0b10001, 0b01111, 0b00001, 0b00010, 0b01100,
        ],
    ),
    (
        'A',
        [
            0b01110, 0b10001, 0b10001, 0b11111, 0b10001, 0b10001, 0b10001,
        ],
    ),
    (
        'B',
        [
            0b11110, 0b10001, 0b10001, 0b11110, 0b10001, 0b10001, 0b11110,
        ],
    ),
    (
        'C',
        [
            0b01110, 0b10001, 0b10000, 0b10000, 0b10000, 0b10001, 0b01110,
        ],
    ),
    (
        'D',
        [
            0b11100, 0b10010, 0b10001, 0b10001, 0b10001, 0b10010, 0b11100,
        ],
    ),
    (
        'E',
        [
            0b11111, 0b10000, 0b10000, 0b11110, 0b10000, 0b10000, 0b11111,
        ],
    ),
    (
        'F',
        [
            0b11111, 0b10000, 0b10000, 0b11110, 0b10000, 0b10000, 0b10000,
        ],
    ),
    (
        'G',
        [
            0b01110, 0b10001, 0b10000, 0b10111, 0b10001, 0b10001, 0b01111,
        ],
    ),
    (
        'H',
        [
            0b10001, 0b10001, 0b10001, 0b11111, 0b10001, 0b10001, 0b10001,
        ],
    ),
    (
        'J',
        [
            0b00111, 0b00010, 0b00010, 0b00010, 0b00010, 0b10010, 0b01100,
        ],
    ),
    (
        'K',
        [
            0b10001, 0b10010, 0b10100, 0b11000, 0b10100, 0b10010, 0b10001,
        ],
    ),
    (
        'M',
        [
            0b10001, 0b11011, 0b10101, 0b10101, 0b10001, 0b10001, 0b10001,
        ],
    ),
    (
        'N',
        [
            0b10001, 0b10001, 0b11001, 0b10101, 0b10011, 0b10001, 0b10001,
        ],
    ),
    (
        'P',
        [
            0b11110, 0b10001, 0b10001, 0b11110, 0b10000, 0b10000, 0b10000,
        ],
    ),
    (
        'Q',
        [
            0b01110, 0b10001, 0b10001, 0b10001, 0b10101, 0b10010, 0b01101,
        ],
    ),
    (
        'R',
        [
            0b11110, 0b10001, 0b10001, 0b11110, 0b10100, 0b10010, 0b10001,
        ],
    ),
    (
        'S',
        [
            0b01111, 0b10000, 0b10000, 0b01110, 0b00001, 0b00001, 0b11110,
        ],
    ),
    (
        'T',
        [
            0b11111, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100,
        ],
    ),
    (
        'V',
        [
            0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01010, 0b00100,
        ],
    ),
    (
        'W',
        [
            0b10001, 0b10001, 0b10001, 0b10101, 0b10101, 0b10101, 0b01010,
        ],
    ),
    (
        'X',
        [
            0b10001, 0b10001, 0b01010, 0b00100, 0b01010, 0b10001, 0b10001,
        ],
    ),
    (
        'Y',
        [
            0b10001, 0b10001, 0b01010, 0b00100, 0b00100, 0b00100, 0b00100,
        ],
    ),
    (
        'Z',
        [
            0b11111, 0b00001, 0b00010, 0b00100, 0b01000, 0b10000, 0b11111,
        ],
    ),
    (
        '-',
        [
            0b00000, 0b00000, 0b00000, 0b11111, 0b00000, 0b00000, 0b00000,
        ],
    ),
];

fn glyph(c: char) -> Option<&'static [u8; 7]> {
    FONT.iter().find(|(g, _)| *g == c).map(|(_, rows)| rows)
}

//each click makes a new one time code
fn open_pairing(
    target: &EventLoopWindowTarget<UserEvent>,
    runtime: &Handle,
    data_dir: &Path,
    labels: &Labels,
    anchor: Option<tray_icon::Rect>,
) -> Result<PairingWindow> {
    let invite = runtime.block_on(invite::open(data_dir))?;
    let link = invite.link.to_string();
    let code = qrcode::QrCode::new(link.as_bytes())?;
    let dark = code
        .to_colors()
        .iter()
        .map(|c| *c == qrcode::Color::Dark)
        .collect();
    let builder = WindowBuilder::new()
        .with_title(labels.scan)
        .with_inner_size(LogicalSize::new(QR_PANEL_SIZE, QR_PANEL_SIZE + CODE_STRIP))
        .with_resizable(false)
        .with_decorations(false)
        .with_always_on_top(true)
        .with_visible(false);
    #[cfg(windows)]
    let builder = {
        use tao::platform::windows::WindowBuilderExtWindows;
        builder.with_skip_taskbar(true)
    };
    let window = Rc::new(builder.build(target)?);
    if let Some(anchor) = anchor {
        place_next_to(&window, anchor);
    }
    window.set_visible(true);
    let context = softbuffer::Context::new(window.clone()).map_err(|e| anyhow::anyhow!("{e}"))?;
    let surface =
        softbuffer::Surface::new(&context, window.clone()).map_err(|e| anyhow::anyhow!("{e}"))?;
    window.set_focus();
    window.request_redraw();
    Ok(PairingWindow {
        window,
        surface,
        dark,
        modules: code.width(),
        code: invite.code,
    })
}

//above a bottom taskbar, below a top menu bar
fn place_next_to(window: &Window, anchor: tray_icon::Rect) {
    let (ax, ay) = (anchor.position.x, anchor.position.y);
    let (aw, ah) = (anchor.size.width as f64, anchor.size.height as f64);
    let Some(monitor) = window
        .available_monitors()
        .find(|m| {
            let (p, s) = (m.position(), m.size());
            ax >= p.x as f64
                && ax < p.x as f64 + s.width as f64
                && ay >= p.y as f64
                && ay < p.y as f64 + s.height as f64
        })
        .or_else(|| window.current_monitor())
    else {
        return;
    };
    let (screen, screen_size) = (monitor.position(), monitor.size());
    let size = window.outer_size();
    let (w, h) = (size.width as f64, size.height as f64);
    let gap = QR_PANEL_GAP * window.scale_factor();
    let left = screen.x as f64;
    let right = (left + screen_size.width as f64 - w).max(left);
    let x = (ax + aw / 2.0 - w / 2.0).clamp(left, right);
    let middle = screen.y as f64 + screen_size.height as f64 / 2.0;
    let y = if ay < middle {
        ay + ah + gap
    } else {
        ay - h - gap
    };
    window.set_outer_position(PhysicalPosition::new(x, y));
}

fn draw_pairing(pairing: &mut PairingWindow) -> Result<()> {
    let size = pairing.window.inner_size();
    let (Some(width), Some(height)) = (NonZeroU32::new(size.width), NonZeroU32::new(size.height))
    else {
        return Ok(());
    };
    pairing
        .surface
        .resize(width, height)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut buffer = pairing
        .surface
        .buffer_mut()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let (width, height) = (size.width as usize, size.height as usize);
    //the qr fills the square, the code the strip
    let square = width.min(height);
    let total = pairing.modules + QR_QUIET_MODULES * 2;
    let scale = (square / total).max(1);
    let left = (width.saturating_sub(total * scale)) / 2;
    let top = (square.saturating_sub(total * scale)) / 2;
    for y in 0..height {
        for x in 0..width {
            let module = |pixel: usize, start: usize| {
                (pixel.checked_sub(start)? / scale).checked_sub(QR_QUIET_MODULES)
            };
            let dark = match (module(x, left), module(y, top)) {
                (Some(mx), Some(my)) if mx < pairing.modules && my < pairing.modules => {
                    pairing.dark[my * pairing.modules + mx]
                }
                _ => false,
            };
            buffer[y * width + x] = if dark { 0x000000 } else { 0xFFFFFF };
        }
    }

    let pixel = (width / CODE_COLUMNS).max(1);
    let advance = pixel * 6;
    let text_width = pairing.code.chars().count() * advance - pixel;
    let text_left = width.saturating_sub(text_width) / 2;
    let text_top = square + height.saturating_sub(square).saturating_sub(pixel * 7) / 2;
    for (i, c) in pairing.code.chars().enumerate() {
        let Some(rows) = glyph(c) else { continue };
        for (row, bits) in rows.iter().enumerate() {
            for col in 0..5 {
                if bits & (0b10000 >> col) == 0 {
                    continue;
                }
                let x0 = text_left + i * advance + col * pixel;
                let y0 = text_top + row * pixel;
                for y in y0..(y0 + pixel).min(height) {
                    for x in x0..(x0 + pixel).min(width) {
                        buffer[y * width + x] = 0x000000;
                    }
                }
            }
        }
    }
    buffer.present().map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

enum UserEvent {
    Menu(MenuEvent),
    //a newer release, none when up to date
    Update(Option<String>),
}

fn check_updates(runtime: &Handle, proxy: tao::event_loop::EventLoopProxy<UserEvent>) {
    runtime.spawn(async move {
        let found = match http_client() {
            Ok(http) => update::newer_release(&http).await,
            Err(e) => Err(e),
        };
        match found {
            Ok(found) => {
                let _ = proxy.send_event(UserEvent::Update(found));
            }
            Err(e) => tracing::warn!("update check failed: {e:#}"),
        }
    });
}

//true when the user wants it now
fn ask_update(labels: &Labels, version: &str) -> bool {
    let message = labels
        .update_message
        .replace("{version}", version)
        .replace("{current}", update::VERSION);
    rfd::MessageDialog::new()
        .set_title(labels.update_title)
        .set_description(message)
        .set_buttons(rfd::MessageButtons::OkCancelCustom(
            labels.install.into(),
            labels.later.into(),
        ))
        .show()
        == rfd::MessageDialogResult::Custom(labels.install.into())
}

//the installer replaces and restarts the hub
fn install_update(runtime: &Handle, labels: &Labels, version: &str) {
    let result = runtime.block_on(async {
        let http = http_client()?;
        #[cfg(windows)]
        {
            let installer = update::download_installer(&http, version).await?;
            std::process::Command::new(installer).spawn()?;
            anyhow::Ok(())
        }
        #[cfg(not(windows))]
        {
            update::apply(&http, version).await?;
            update::restart()
        }
    });
    if let Err(e) = result {
        tracing::error!("update failed: {e:#}");
        rfd::MessageDialog::new()
            .set_title(labels.update_title)
            .set_description(format!("{} {e:#}", labels.update_failed))
            .set_level(rfd::MessageLevel::Error)
            .show();
    }
}

pub fn run(
    data_dir: PathBuf,
    runtime: Handle,
    stop: oneshot::Sender<()>,
    hub_thread: JoinHandle<()>,
) -> Result<()> {
    let labels = labels();
    let name = Config::load(&data_dir)?.hub_name();

    #[allow(unused_mut)]
    let mut event_loop = EventLoopBuilder::<UserEvent>::with_user_event().build();
    //menu bar only, no dock icon
    #[cfg(target_os = "macos")]
    {
        use tao::platform::macos::{ActivationPolicy, EventLoopExtMacOS};
        event_loop.set_activation_policy(ActivationPolicy::Accessory);
    }
    let proxy = event_loop.create_proxy();
    MenuEvent::set_event_handler(Some(move |event| {
        let _ = proxy.send_event(UserEvent::Menu(event));
    }));

    let update_proxy = event_loop.create_proxy();
    let mut next_check = Instant::now();
    //asked once per version
    let mut offered: Option<String> = None;
    let install = MenuItem::new("", true, None);
    let title = MenuItem::new(format!("Sampler-Hub · {name}"), false, None);
    let devices = MenuItem::new(devices_label(&labels, &data_dir), false, None);
    let pair = MenuItem::new(labels.pair, true, None);
    let quit = MenuItem::new(labels.quit, true, None);
    let menu = Menu::new();
    menu.append_items(&[
        &title,
        &devices,
        &PredefinedMenuItem::separator(),
        &pair,
        &quit,
    ])?;
    let icon = icon()?;

    let mut tray: Option<TrayIcon> = None;
    let mut pairing: Option<PairingWindow> = None;
    let mut stop = Some(stop);
    let mut hub_thread = Some(hub_thread);
    event_loop.run(move |event, target, control_flow| {
        *control_flow = ControlFlow::WaitUntil(Instant::now() + REFRESH);
        match event {
            //macos wants the icon once the loop runs
            Event::NewEvents(StartCause::Init) => {
                let builder = TrayIconBuilder::new()
                    .with_menu(Box::new(menu.clone()))
                    .with_tooltip("Sampler-Hub");
                #[cfg(target_os = "macos")]
                let builder = builder.with_icon_templated(icon.clone());
                #[cfg(windows)]
                let builder = builder.with_icon(icon.clone());
                match builder.build() {
                    Ok(icon) => tray = Some(icon),
                    Err(e) => tracing::error!("cannot show the tray icon: {e}"),
                }
            }
            Event::NewEvents(StartCause::ResumeTimeReached { .. }) => {
                devices.set_text(devices_label(&labels, &data_dir));
                if Instant::now() >= next_check {
                    next_check = Instant::now() + UPDATE_CHECK;
                    check_updates(&runtime, update_proxy.clone());
                }
            }
            Event::UserEvent(UserEvent::Update(found)) => {
                let Some(version) = found else {
                    let _ = menu.remove(&install);
                    return;
                };
                install.set_text(labels.update.replace("{version}", &version));
                if offered.is_none() {
                    let _ = menu.insert(&install, 3);
                }
                if offered.as_deref() != Some(version.as_str()) {
                    offered = Some(version.clone());
                    if ask_update(&labels, &version) {
                        install_update(&runtime, &labels, &version);
                    }
                }
            }
            Event::UserEvent(UserEvent::Menu(event)) if event.id == *install.id() => {
                if let Some(version) = offered.clone() {
                    install_update(&runtime, &labels, &version);
                }
            }
            Event::UserEvent(UserEvent::Menu(event)) if event.id == *pair.id() => {
                let anchor = tray.as_ref().and_then(|t| t.rect());
                match open_pairing(target, &runtime, &data_dir, &labels, anchor) {
                    Ok(window) => pairing = Some(window),
                    Err(e) => tracing::error!("cannot show the pairing code: {e:#}"),
                }
            }
            Event::RedrawRequested(id) => {
                if let Some(window) = pairing.as_mut().filter(|p| p.window.id() == id)
                    && let Err(e) = draw_pairing(window)
                {
                    tracing::error!("cannot draw the pairing code: {e:#}");
                }
            }
            //a click elsewhere or escape closes the panel
            Event::WindowEvent {
                event: WindowEvent::Focused(false) | WindowEvent::CloseRequested,
                ..
            } => pairing = None,
            Event::WindowEvent {
                event: WindowEvent::KeyboardInput { event, .. },
                ..
            } if event.logical_key == Key::Escape && event.state == ElementState::Pressed => {
                pairing = None
            }
            Event::UserEvent(UserEvent::Menu(event)) if event.id == *quit.id() => {
                tray.take();
                if let Some(stop) = stop.take() {
                    let _ = stop.send(());
                }
                //lets the hub close its connections
                if let Some(thread) = hub_thread.take() {
                    let _ = thread.join();
                }
                *control_flow = ControlFlow::Exit;
            }
            _ => {}
        }
    })
}
