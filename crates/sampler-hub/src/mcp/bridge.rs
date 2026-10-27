//one stdio mcp session shared by every device
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

const PROTOCOL_VERSION: &str = "2025-06-18";
const INIT_TIMEOUT: Duration = Duration::from_secs(60);
//long tool calls still end
const CALL_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_LINE_BYTES: usize = 32 * 1024 * 1024;

pub struct Bridge {
    writer: mpsc::UnboundedSender<String>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Value>>>,
    next: AtomicU64,
    init: OnceLock<Value>,
}

impl std::fmt::Debug for Bridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Bridge")
    }
}

impl Bridge {
    //initializes once, devices reuse the result
    pub async fn connect<R, W>(reader: R, writer: W, roots: Vec<Value>) -> Result<Arc<Self>>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(write_lines(writer, rx));
        let bridge = Arc::new(Self {
            writer: tx,
            pending: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            init: OnceLock::new(),
        });
        tokio::spawn(read_lines(bridge.clone(), reader, roots));

        let init = tokio::time::timeout(
            INIT_TIMEOUT,
            bridge.call(json!({
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": { "roots": { "listChanged": false } },
                    "clientInfo": { "name": "sampler-hub", "version": env!("CARGO_PKG_VERSION") },
                },
            })),
        )
        .await
        .context("the server did not initialize")??;
        let result = init
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("initialize failed: {}", init["error"]))?;
        let _ = bridge.init.set(result);
        bridge.send(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))?;
        Ok(bridge)
    }

    //none for notifications, which stay on the hub
    pub async fn handle(&self, message: Value) -> Option<Value> {
        let id = message.get("id").cloned().filter(|id| !id.is_null())?;
        let method = message.get("method").and_then(Value::as_str)?;
        let result = match method {
            "initialize" => Ok(json!({ "jsonrpc": "2.0", "id": id, "result": self.init.get() })),
            "ping" => Ok(json!({ "jsonrpc": "2.0", "id": id, "result": {} })),
            _ => match tokio::time::timeout(CALL_TIMEOUT, self.call(message)).await {
                Ok(Ok(mut response)) => {
                    response["id"] = id.clone();
                    Ok(response)
                }
                Ok(Err(e)) => Err(e),
                Err(_) => Err(anyhow!("the server did not answer in time")),
            },
        };
        Some(result.unwrap_or_else(|e| {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32603, "message": format!("{e:#}") },
            })
        }))
    }

    //hub ids keep devices apart
    async fn call(&self, mut message: Value) -> Result<Value> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        message["id"] = json!(id);
        if let Err(e) = self.send(message) {
            self.pending.lock().unwrap().remove(&id);
            return Err(e);
        }
        rx.await.map_err(|_| anyhow!("the server stopped"))
    }

    fn send(&self, message: Value) -> Result<()> {
        self.writer
            .send(message.to_string())
            .map_err(|_| anyhow!("the server stopped"))
    }

    fn answer(&self, id: Value, result: Result<Value, (i64, &str)>) {
        let reply = match result {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err((code, message)) => {
                json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
            }
        };
        let _ = self.send(reply);
    }
}

async fn write_lines<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut lines: mpsc::UnboundedReceiver<String>,
) {
    while let Some(line) = lines.recv().await {
        let sent = async {
            writer.write_all(line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await
        };
        if sent.await.is_err() {
            return;
        }
    }
}

async fn read_lines<R: AsyncRead + Unpin>(bridge: Arc<Bridge>, reader: R, roots: Vec<Value>) {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    loop {
        line.clear();
        match (&mut reader)
            .take(MAX_LINE_BYTES as u64 + 1)
            .read_until(b'\n', &mut line)
            .await
        {
            Ok(0) | Err(_) => break,
            Ok(n) if n > MAX_LINE_BYTES => break,
            Ok(_) => {}
        }
        let Ok(message) = serde_json::from_slice::<Value>(&line) else {
            //servers may print logs on stdout
            debug!("ignored a non json line from an mcp server");
            continue;
        };
        if let Err(e) = dispatch(&bridge, message, &roots) {
            debug!("ignored an mcp message: {e:#}");
        }
    }
    //waiting devices learn the server stopped
    bridge.pending.lock().unwrap().clear();
}

fn dispatch(bridge: &Bridge, message: Value, roots: &[Value]) -> Result<()> {
    let id = message.get("id").cloned().filter(|id| !id.is_null());
    match (message.get("method").and_then(Value::as_str), id) {
        (Some(method), Some(id)) => {
            let result = match method {
                "roots/list" => Ok(json!({ "roots": roots })),
                "ping" => Ok(json!({})),
                //sampling and elicitation need a device
                _ => Err((-32601, "not supported by Sampler-Hub")),
            };
            bridge.answer(id, result);
        }
        (Some(_), None) => {}
        (None, Some(id)) => {
            let id = id.as_u64().context("unknown response id")?;
            let Some(waiter) = bridge.pending.lock().unwrap().remove(&id) else {
                bail!("no request {id}");
            };
            let _ = waiter.send(message);
        }
        (None, None) => bail!("not a json-rpc message"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    //echoes tools/call arguments after a delay
    async fn fake_server(stream: tokio::io::DuplexStream, inits: Arc<AtomicU64>) {
        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let message: Value = serde_json::from_str(&line).unwrap();
            let Some(id) = message.get("id").cloned() else {
                continue;
            };
            let reply = match message["method"].as_str().unwrap() {
                "initialize" => {
                    inits.fetch_add(1, Ordering::SeqCst);
                    json!({ "jsonrpc": "2.0", "id": id, "result": {
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": { "tools": {} },
                        "serverInfo": { "name": "fake", "version": "1" },
                    }})
                }
                "tools/call" => {
                    let delay = message["params"]["arguments"]["delay"].as_u64().unwrap();
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    json!({ "jsonrpc": "2.0", "id": id, "result": {
                        "content": [{ "type": "text", "text": message["params"]["arguments"]["text"] }],
                    }})
                }
                _ => {
                    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": "no" } })
                }
            };
            write
                .write_all(format!("{reply}\n").as_bytes())
                .await
                .unwrap();
        }
    }

    fn call(id: Value, text: &str, delay: u64) -> Value {
        json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": { "name": "echo", "arguments": { "text": text, "delay": delay } } })
    }

    #[tokio::test]
    async fn devices_share_one_session() {
        let (hub_side, server_side) = tokio::io::duplex(64 * 1024);
        let inits = Arc::new(AtomicU64::new(0));
        tokio::spawn(fake_server(server_side, inits.clone()));
        let (read, write) = tokio::io::split(hub_side);
        let bridge = Bridge::connect(read, write, Vec::new()).await.unwrap();

        //each device starts with its own initialize
        for device in ["a", "b"] {
            let reply = bridge
                .handle(
                    json!({ "jsonrpc": "2.0", "id": device, "method": "initialize", "params": {} }),
                )
                .await
                .unwrap();
            assert_eq!(reply["id"], device);
            assert_eq!(reply["result"]["serverInfo"]["name"], "fake");
        }
        assert_eq!(inits.load(Ordering::SeqCst), 1);

        //same ids from two devices, answers out of order
        let (slow, fast) = tokio::join!(
            bridge.handle(call(json!(1), "slow", 200)),
            bridge.handle(call(json!(1), "fast", 10)),
        );
        assert_eq!(slow.unwrap()["result"]["content"][0]["text"], "slow");
        let fast = fast.unwrap();
        assert_eq!(fast["id"], 1);
        assert_eq!(fast["result"]["content"][0]["text"], "fast");

        let notification = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
        assert!(bridge.handle(notification).await.is_none());
        let unknown = bridge
            .handle(json!({ "jsonrpc": "2.0", "id": 7, "method": "nope" }))
            .await
            .unwrap();
        assert_eq!(unknown["error"]["code"], -32601);
    }
}
