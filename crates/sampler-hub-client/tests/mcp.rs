//installs real mcp servers, run with --ignored
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use sampler_hub_client::{
    HubClient, HubError, HubProvider, HubSettings, McpListing, McpSource, PairedHub, ProxyInfo,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

//a dependency free stdio server
const ECHO_SERVER: &str = r#"
const readline = require("readline");
const send = (m) => process.stdout.write(JSON.stringify(m) + "\n");
readline.createInterface({ input: process.stdin }).on("line", (line) => {
  const m = JSON.parse(line);
  if (m.id === undefined) return;
  if (m.method === "initialize") {
    send({ jsonrpc: "2.0", id: m.id, result: { protocolVersion: m.params.protocolVersion,
      capabilities: { tools: {} }, serverInfo: { name: "echo", version: "1.0.0" } } });
  } else if (m.method === "tools/list") {
    send({ jsonrpc: "2.0", id: m.id, result: { tools: [{ name: "echo",
      inputSchema: { type: "object", properties: { text: { type: "string" } } } }] } });
  } else if (m.method === "tools/call") {
    const text = process.env.ECHO_PREFIX + m.params.arguments.text;
    send({ jsonrpc: "2.0", id: m.id, result: { content: [{ type: "text", text }] } });
  } else {
    send({ jsonrpc: "2.0", id: m.id, error: { code: -32601, message: "unknown" } });
  }
});
"#;

const ECHO_MANIFEST: &str = r#"{
  "manifest_version": "0.3",
  "name": "echo",
  "version": "1.0.0",
  "server": {
    "type": "node",
    "entry_point": "server/index.js",
    "mcp_config": {
      "command": "node",
      "args": ["${__dirname}/server/index.js"],
      "env": { "ECHO_PREFIX": "${user_config.prefix}" }
    }
  },
  "user_config": { "prefix": { "type": "string", "required": true, "sensitive": true } }
}"#;

fn random_hex<const N: usize>() -> String {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).unwrap();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn new_client() -> Arc<HubClient> {
    let mut key = [0u8; 32];
    getrandom::fill(&mut key).unwrap();
    HubClient::new(key.to_vec()).unwrap()
}

fn bundle() -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for (name, body) in [
        ("manifest.json", ECHO_MANIFEST),
        ("server/index.js", ECHO_SERVER),
    ] {
        writer
            .start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(body.as_bytes()).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

//serves one file to the hub, as often as asked
async fn serve(bytes: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                bytes.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&bytes).await.unwrap();
        }
    });
    format!("http://127.0.0.1:{port}/echo.mcpb")
}

async fn pair(client: &HubClient, data_dir: &std::path::Path, name: &str, kind: &str) -> PairedHub {
    let link = sampler_hub::hub::pairing_link(data_dir).unwrap().to_string();
    for _ in 0..10 {
        match client
            .pair(link.clone(), name.into(), kind.into(), None)
            .await
        {
            Err(HubError::Unreachable(_)) => tokio::time::sleep(Duration::from_secs(2)).await,
            result => {
                let paired = result.unwrap();
                join(data_dir, client);
                return paired;
            }
        }
    }
    panic!("hub never became reachable");
}

//binds a device to the first profile
fn join(data_dir: &std::path::Path, client: &HubClient) {
    let store = sampler_hub::store::Store::new(data_dir);
    let profile = match store.profiles().unwrap().into_iter().next() {
        Some(profile) => profile,
        None => store.create_profile("owner").unwrap(),
    };
    assert!(store.bind_device(&client.device_id(), &profile.id).unwrap());
    client
        .set_profile(profile.id.clone(), test_key(&profile.id))
        .unwrap();
}

//devices of one profile share it
fn test_key(profile: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(&profile.as_bytes()[..32])
}

//the http status of a call through the proxy
async fn status_line(proxy: &ProxyInfo, provider: &str) -> String {
    let body = r#"{"jsonrpc":"2.0","id":9,"method":"tools/list"}"#;
    let mut socket = TcpStream::connect(("127.0.0.1", proxy.port)).await.unwrap();
    let request = format!(
        "POST /{}/{provider}/ HTTP/1.1
host: localhost
content-type: application/json
content-length: {}
connection: close

{body}",
        proxy.token,
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    response.lines().next().unwrap_or_default().to_string()
}

//one json-rpc call through the device proxy
async fn rpc(proxy: &ProxyInfo, provider: &str, message: Value) -> Value {
    let body = message.to_string();
    let mut socket = TcpStream::connect(("127.0.0.1", proxy.port)).await.unwrap();
    let request = format!(
        "POST /{}/{provider}/ HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\naccept: application/json, text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        proxy.token,
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        unchunk(body)
    } else {
        body.to_string()
    };
    serde_json::from_str(&body).unwrap()
}

fn unchunk(mut body: &str) -> String {
    let mut out = String::new();
    loop {
        let (size, rest) = body.split_once("\r\n").unwrap();
        let size = usize::from_str_radix(size.trim(), 16).unwrap();
        if size == 0 {
            return out;
        }
        out.push_str(&rest[..size]);
        body = &rest[size + 2..];
    }
}

async fn call_tool(
    proxy: &ProxyInfo,
    provider: &str,
    id: u64,
    name: &str,
    arguments: Value,
) -> String {
    let answer = rpc(
        proxy,
        provider,
        json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": { "name": name, "arguments": arguments } }),
    )
    .await;
    assert_eq!(answer["id"], id, "{answer}");
    answer["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("{answer}"))
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "downloads node, uv and mcp servers"]
async fn hub_runs_mcp_servers() {
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-mcp-{}", random_hex::<8>()));
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let hub = tokio::spawn({
        let data_dir = data_dir.clone();
        async move {
            sampler_hub::hub::run(&data_dir, async {
                let _ = stop_rx.await;
            })
            .await
        }
    });
    let phone = new_client();
    let browser = new_client();
    let phone_hub = pair(&phone, &data_dir, "phone", "phone").await;
    let browser_hub = pair(&browser, &data_dir, "browser", "web").await;

    //browsers use servers but never manage them
    assert!(browser.mcp_status(browser_hub.clone()).await.is_err());

    let bytes = bundle();
    let sha256: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let manual = |id: &str, source: McpSource| McpListing {
        id: id.into(),
        title: id.into(),
        description: String::new(),
        version: "1.0.0".into(),
        publisher: "test".into(),
        repository: None,
        license: None,
        source,
        fields: Vec::new(),
        arguments: Vec::new(),
        env: Vec::new(),
        offline: true,
        registry: None,
    };
    let echo = manual(
        "Echo",
        McpSource::Mcpb {
            url: serve(bytes).await,
            sha256,
        },
    );
    //the form comes from the bundle manifest
    let echo = phone.mcp_prepare(phone_hub.clone(), echo).await.unwrap();
    let echo_listing = echo.clone();
    let prefix = echo.fields.iter().find(|f| f.key == "prefix").unwrap();
    assert!(prefix.required && prefix.secret);
    assert!(
        phone
            .mcp_install(phone_hub.clone(), echo.clone(), HashMap::new())
            .await
            .is_err(),
        "a required value is missing"
    );
    let values = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    let echo_id = phone
        .mcp_install(phone_hub.clone(), echo, values(&[("prefix", "hub says ")]))
        .await
        .unwrap();
    let time = phone
        .mcp_catalog(phone_hub.clone())
        .await
        .unwrap()
        .into_iter()
        .find(|l| l.id == "time")
        .unwrap();
    let time_id = phone
        .mcp_install(
            phone_hub.clone(),
            time,
            values(&[("timezone", "Europe/Paris")]),
        )
        .await
        .unwrap();
    let everything = manual(
        "Everything",
        McpSource::Npm {
            package: "@modelcontextprotocol/server-everything".into(),
            version: "1.0.0".into(),
        },
    );
    let everything_id = phone
        .mcp_install(phone_hub.clone(), everything, HashMap::new())
        .await
        .unwrap();

    let servers = running(&phone, &phone_hub, 3).await;
    let id_of = |title: &str| servers.iter().find(|p| p.name == title).unwrap().id.clone();
    let (echo, everything, time) = (id_of("Echo"), id_of("Everything"), id_of("Time"));
    let status = phone.mcp_status(phone_hub.clone()).await.unwrap();
    let echo_status = status.servers.iter().find(|s| s.id == echo_id).unwrap();
    assert!(echo_status.values.is_empty(), "secrets never leave the hub");
    assert_eq!(echo_status.secrets_set, ["prefix"]);
    assert_eq!(echo_status.tools, ["echo"]);
    //usage of each server and of the whole hub
    assert!(echo_status.memory.is_some_and(|m| m > 0), "{echo_status:?}");
    let hub_status = phone.status(phone_hub.clone()).await.unwrap();
    assert!(
        hub_status
            .memory
            .is_some_and(|m| m > echo_status.memory.unwrap())
    );

    let phone_proxy = phone.clone().start_proxy(phone_hub.clone()).await.unwrap();
    let browser_proxy = browser
        .clone()
        .start_proxy(browser_hub.clone())
        .await
        .unwrap();
    for (proxy, server) in [
        (&phone_proxy, &echo),
        (&browser_proxy, &echo),
        (&phone_proxy, &time),
    ] {
        let init = rpc(
            proxy,
            server,
            json!({ "jsonrpc": "2.0", "id": 0, "method": "initialize",
                "params": { "protocolVersion": "2025-06-18", "capabilities": {},
                    "clientInfo": { "name": "test", "version": "1" } } }),
        )
        .await;
        assert!(init["result"]["serverInfo"].is_object(), "{init}");
    }

    //two devices, same request ids, at once
    let (from_phone, from_browser, everything_echo, now) = tokio::join!(
        call_tool(&phone_proxy, &echo, 1, "echo", json!({ "text": "phone" })),
        call_tool(
            &browser_proxy,
            &echo,
            1,
            "echo",
            json!({ "text": "browser" })
        ),
        call_tool(
            &browser_proxy,
            &everything,
            2,
            "echo",
            json!({ "message": "hello" })
        ),
        call_tool(
            &phone_proxy,
            &time,
            3,
            "get_current_time",
            json!({ "timezone": "Europe/Paris" })
        ),
    );
    assert_eq!(from_phone, "hub says phone");
    assert_eq!(from_browser, "hub says browser");
    assert!(everything_echo.contains("hello"), "{everything_echo}");
    assert!(now.contains("Europe/Paris"), "{now}");

    //another profile never reaches these servers
    let flatmate = new_client();
    let link = sampler_hub::hub::pairing_link(&data_dir).unwrap().to_string();
    let flatmate_hub = flatmate
        .pair(link, "flatmate".into(), "phone".into(), None)
        .await
        .unwrap();
    let store = sampler_hub::store::Store::new(&data_dir);
    let other = store.create_profile("flatmate").unwrap();
    assert!(store.bind_device(&flatmate.device_id(), &other.id).unwrap());
    flatmate
        .set_profile(other.id.clone(), test_key(&other.id))
        .unwrap();
    let theirs = flatmate.mcp_status(flatmate_hub.clone()).await.unwrap();
    assert!(theirs.servers.is_empty());
    let listed = flatmate.providers(flatmate_hub.clone()).await.unwrap();
    assert!(listed.iter().all(|p| p.kind != "mcp"), "{listed:?}");
    assert!(
        flatmate
            .mcp_remove(flatmate_hub.clone(), echo_id.clone())
            .await
            .is_err()
    );
    let flatmate_proxy = flatmate
        .clone()
        .start_proxy(flatmate_hub.clone())
        .await
        .unwrap();
    assert!(status_line(&flatmate_proxy, &echo).await.contains("503"));
    //a member only shares its own folder
    let folders = flatmate
        .mcp_folders(flatmate_hub.clone(), None)
        .await
        .unwrap();
    assert!(folders.path.contains(&other.id), "{folders:?}");
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap();
    assert!(
        flatmate
            .mcp_folders(flatmate_hub.clone(), Some(home))
            .await
            .is_err()
    );

    //only an admin sees every profile's servers
    flatmate
        .mcp_install(
            flatmate_hub.clone(),
            echo_listing,
            values(&[("prefix", "flatmate says ")]),
        )
        .await
        .unwrap();
    assert!(flatmate.mcp_overview(flatmate_hub.clone()).await.is_err());
    let overview = phone.mcp_overview(phone_hub.clone()).await.unwrap();
    assert_eq!(overview.len(), 4, "{overview:?}");
    let owned_by = |profile: &str| {
        overview
            .iter()
            .filter(|s| s.owner.as_deref() == Some(profile))
            .count()
    };
    assert_eq!(owned_by(&other.id), 1);
    let echo_summary = overview.iter().find(|s| s.id == echo_id).unwrap();
    assert_eq!(
        (echo_summary.state.as_str(), echo_summary.tools),
        ("running", 1)
    );
    //an admin browser sees them too, without managing any
    let from_browser = browser.mcp_overview(browser_hub.clone()).await.unwrap();
    assert_eq!(from_browser.len(), 4);

    //an admin stops and removes another profile's server, a member cannot
    running(&flatmate, &flatmate_hub, 1).await;
    let theirs = overview
        .iter()
        .find(|s| s.owner.as_deref() == Some(other.id.as_str()))
        .unwrap()
        .id
        .clone();
    let stopped = phone
        .mcp_manage(phone_hub.clone(), theirs.clone(), "stop".into())
        .await
        .unwrap();
    assert!(!stopped.iter().find(|s| s.id == theirs).unwrap().enabled);
    running(&flatmate, &flatmate_hub, 0).await;
    assert!(
        flatmate
            .mcp_manage(flatmate_hub.clone(), echo_id.clone(), "stop".into())
            .await
            .is_err()
    );
    let left = phone
        .mcp_manage(phone_hub.clone(), theirs, "remove".into())
        .await
        .unwrap();
    assert_eq!(left.len(), 3);

    //a new secret restarts the server
    phone
        .mcp_configure(
            phone_hub.clone(),
            echo_id.clone(),
            values(&[("prefix", "changed ")]),
        )
        .await
        .unwrap();
    running(&phone, &phone_hub, 3).await;
    assert_eq!(
        call_tool(&phone_proxy, &echo, 4, "echo", json!({ "text": "phone" })).await,
        "changed phone"
    );

    phone
        .mcp_enable(phone_hub.clone(), everything_id.clone(), false)
        .await
        .unwrap();
    running(&phone, &phone_hub, 2).await;
    let status = phone.mcp_remove(phone_hub.clone(), time_id).await.unwrap();
    assert_eq!(status.servers.len(), 2);
    running(&phone, &phone_hub, 1).await;

    //an admin turns every mcp server off, then on again
    let settings = phone.hub_settings(phone_hub.clone()).await.unwrap();
    let off = HubSettings {
        mcp_servers: false,
        ..settings.clone()
    };
    phone
        .set_hub_settings(phone_hub.clone(), off)
        .await
        .unwrap();
    running(&phone, &phone_hub, 0).await;
    assert!(
        phone
            .mcp_enable(phone_hub.clone(), everything_id, true)
            .await
            .is_err()
    );
    phone
        .set_hub_settings(phone_hub.clone(), settings)
        .await
        .unwrap();
    running(&phone, &phone_hub, 1).await;

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}

//waits until this many servers answer
async fn running(client: &HubClient, hub: &PairedHub, count: usize) -> Vec<HubProvider> {
    let mut servers = Vec::new();
    for _ in 0..180 {
        servers = client.providers(hub.clone()).await.unwrap();
        servers.retain(|p| p.kind == "mcp");
        if servers.len() == count {
            return servers;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("expected {count} running servers, got {servers:?}");
}
