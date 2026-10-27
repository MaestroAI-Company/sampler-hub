# Sampler-Hub

Sampler-Hub is a small self-hosted server that keeps your [Opera](https://github.com/MaestroAI-Company/opera) devices in sync. Install it on a computer you own, pair your phone, desktop and browser, and the hub handles the rest:

- **Sync**: conversations, settings and AI providers move between your devices.
- **Profiles**: one hub for a home or a flat share. Each person has a profile, sealed end to end: the hub stores it but cannot read it, its owner included.
- **Shared AI**: Ollama, LM Studio and llama.cpp running on the hub's computer can be used from every paired device, even outside your home network.
- **Local models**: the hub can download and run [llama.cpp](https://github.com/ggml-org/llama.cpp) models itself. It picks the GPU backend for you, and you manage it from the Opera app.

Your data stays on your hardware. There is no account and no Opera server in the middle.

## Install

### Windows

Download `sampler-hub-setup-windows-x86_64.exe` from the [latest release](https://github.com/MaestroAI-Company/sampler-hub/releases/latest) and run it. It installs for your user only, in `%LOCALAPPDATA%\MaestroAI-Company\Sampler-Hub`, and starts the hub with your session. A tray icon shows the pairing code and offers updates.

You can also host the hub from the Opera desktop app: open **Settings → Sampler-Hub → Host the hub here**.

### macOS (Apple silicon)

Download `sampler-hub-macos-aarch64.dmg` from the latest release. To install from a terminal instead:

```sh
curl -fsSL https://github.com/MaestroAI-Company/sampler-hub/releases/latest/download/install.sh | sh
```

### Linux (x86_64 and arm64)

```sh
curl -fsSL https://github.com/MaestroAI-Company/sampler-hub/releases/latest/download/install.sh | sh
```

The script checks every file against the release `SHA256SUMS`. It installs `sampler-hub` in `~/.local/bin` (change it with `SAMPLER_HUB_DIR`) and registers a systemd user service, or a launchd agent on macOS, so the hub starts with your session.

On every system the installer puts `sampler-hub` on your `PATH`, so open a new terminal and type the commands below directly.

## Pair a device

On the hub's computer:

```sh
sampler-hub pair
```

The command prints a QR code and an 8-character code, both valid for a single pairing:

- **phone**: scan the QR code from **Settings → Sampler-Hub → Pair a hub**;
- **desktop or browser**: type the code in the same place.

On the same network, Opera also finds the hub by itself and lists it under **Nearby hubs**. You still have to enter the code; being on the network grants nothing.

On Windows and macOS, the tray icon shows the same code.

## Profiles

A paired device then picks its profile:

- **new person**: create a profile with a name and a password. The first profile of a hub manages it, the next ones are members;
- **existing profile**: pick it and type its password, for example after reinstalling Opera;
- **another device of yours**: on a device that already has the profile, choose **Add one of my devices**. It shows a code; type it on the new device and compare the six digits on both screens. The key goes from one device to the other, no password needed.

The profile key is created on the device and never reaches the hub in clear. The hub keeps a copy locked by the password, and five wrong passwords lock the profile for a growing wait. A forgotten password with no device left loses the profile: nobody can reset it.

Each profile has its own conversations, settings, API keys and hosted MCP servers. Members never see each other's data, and only admins manage the hub. Requests to the hub's models and MCP tools are processed in clear on the hub machine while they answer.

## Commands

| Command | What it does |
|---|---|
| `sampler-hub run` | Run the hub in the foreground |
| `sampler-hub pair` | Show a one-time QR code and code to pair a device |
| `sampler-hub devices` | List paired devices |
| `sampler-hub devices remove <id>` | Unpair a device by id or id prefix |
| `sampler-hub profiles` | List profiles |
| `sampler-hub profiles remove <id>` | Remove a profile with its data, devices and MCP servers |
| `sampler-hub profiles promote <id>` | Let a profile manage the hub |
| `sampler-hub service install` | Start the hub with your session, now and at every login |
| `sampler-hub service start` / `stop` / `status` | Control the background service |
| `sampler-hub service uninstall` | Stop the hub and remove it from your session |
| `sampler-hub ticket` | Print the hub ticket, for `ping` |
| `sampler-hub ping <ticket>` | Check that a hub is reachable |

Every command takes `--data-dir <path>` to use another data directory, for example to run two hubs side by side.

## Managing the hub from Opera

Admin profiles, on a phone or a desktop, manage the hub from Opera's hub page:

- choose what each device syncs;
- rename or remove paired devices. Devices not seen for 30 days are marked inactive, and reinstalling Opera on the same device replaces its old entry;
- turn local network discovery and provider sharing on or off;
- install hub updates. The hub checks GitHub every 6 hours and Opera offers the new version;
- download, run and delete llama.cpp models.

Every member installs and runs its own MCP servers on the hub; a member's servers only reach its own folder on the hub, never the host's files. Browsers can sync but never manage the hub.

## Local models with llama.cpp

The first model you download from Opera makes the hub install the official `llama-server` build that suits the machine: CUDA, Vulkan or Metal when available, CPU otherwise. Every download is checked against the digest GitHub publishes. You then pick a recommended model or search Hugging Face for any GGUF model and version.

The model is served to all your devices as the **llama.cpp** provider of the hub. One model is loaded at a time.

## How it works

- **Transport**: devices reach the hub through [iroh](https://github.com/n0-computer/iroh) QUIC connections, encrypted end to end and authenticated by key. They connect directly when they can and through a relay otherwise, so the hub works from anywhere without opening a port.
- **Pairing**: the code is stretched with Argon2id into a temporary key that opens a one-time pairing endpoint. A wrong code closes it after 5 attempts.
- **Access**: every request is denied by default. Each paired device has its own key and its own permissions, and access is checked again on every stream, so a removed device is cut off at once.
- **End to end**: every synced record is sealed on the device with its profile key (XChaCha20-Poly1305), record ids included. The password is stretched with Argon2id (64 MiB) into a proof for the hub and a key that locks the profile key. Device links use X25519 with a commitment, so a relaying hub cannot pick keys that match the six digits.
- **Storage**: the hub keeps its data in an SQLite database, each row encrypted again with XChaCha20-Poly1305. The key lives in the OS keychain, or in a key file when no keychain is available.

The hub keeps its data here:

| OS | Data directory |
|---|---|
| Windows | `%LOCALAPPDATA%\MaestroAI-Company\Sampler-Hub\data` |
| macOS | `~/Library/Application Support/company.MaestroAI-Company.Sampler-Hub` |
| Linux | `~/.local/share/samplerhub` |

## Build from source

You need a recent stable Rust toolchain.

```sh
cargo build --release -p sampler-hub                         # sampler-hub and sampler-hubw, the tray app
cargo build --release -p sampler-hub --no-default-features   # sampler-hub only, for headless servers
cargo test --workspace
```

The repository holds four crates:

| Crate | Role |
|---|---|
| `sampler-hub` | The hub: CLI, background service, tray, sync store, llama.cpp runtime |
| `sampler-hub-proto` | Protocol types shared by the hub and its clients |
| `sampler-hub-client` | The client library Opera uses, with Kotlin bindings (uniffi) for Android and a WebAssembly build for browsers |
| `uniffi-bindgen` | Generates the Kotlin bindings |

## License

[GNU AGPL v3](LICENSE)
