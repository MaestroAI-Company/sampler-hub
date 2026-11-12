//browser bindings, relays only
use std::sync::Arc;

use js_sys::{Promise, Uint8Array};
use sampler_hub_proto::{ForwardHead, read_frame, write_frame};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, Notify};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::{Forwarded, HubClient, HubError, HubSettings, PairedHub, ProfileLock, SyncRecord};

const CHUNK_BYTES: usize = 16 * 1024;

#[wasm_bindgen]
pub struct WebHubClient {
    inner: Arc<HubClient>,
}

enum Body {
    Stream(iroh::endpoint::RecvStream),
    //the hub explains a refused request
    Text(Vec<u8>),
    Done,
}

//a provider answer, the body is read chunk by chunk
#[wasm_bindgen]
pub struct WebResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Arc<Mutex<Body>>,
}

//text frames both ways
#[wasm_bindgen]
pub struct WebRealtime {
    send: Arc<Mutex<iroh::endpoint::SendStream>>,
    recv: Arc<Mutex<iroh::endpoint::RecvStream>>,
    //wakes a pending read
    cancelled: Arc<Notify>,
}

fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(value).map_err(JsValue::from)
}

fn from_js<T: DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(JsValue::from)
}

fn hub_error(error: HubError) -> JsValue {
    to_js(&error).unwrap_or_else(|e| e)
}

//rust futures become js promises
fn promise<T, F>(future: F) -> Promise
where
    T: Serialize,
    F: Future<Output = Result<T, HubError>> + 'static,
{
    future_to_promise(async move { to_js(&future.await.map_err(hub_error)?) })
}

#[wasm_bindgen]
impl WebHubClient {
    #[wasm_bindgen(constructor)]
    pub fn new(secret_key: Vec<u8>) -> Result<WebHubClient, JsValue> {
        Ok(Self {
            inner: HubClient::new(secret_key).map_err(hub_error)?,
        })
    }

    #[wasm_bindgen(js_name = deviceId)]
    pub fn device_id(&self) -> String {
        self.inner.device_id()
    }

    #[wasm_bindgen(js_name = setProfile)]
    pub fn set_profile(&self, profile_id: String, key: String) -> Result<(), JsValue> {
        self.inner.set_profile(profile_id, key).map_err(hub_error)
    }

    #[wasm_bindgen(js_name = clearProfile)]
    pub fn clear_profile(&self) {
        self.inner.clear_profile()
    }

    #[wasm_bindgen(js_name = profiles)]
    pub fn profiles(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.profiles(hub).await }))
    }

    #[wasm_bindgen(js_name = createProfile)]
    pub fn create_profile(
        &self,
        hub: JsValue,
        name: String,
        password: String,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move {
            inner.create_profile(hub, name, password).await
        }))
    }

    #[wasm_bindgen(js_name = unlockProfile)]
    pub fn unlock_profile(
        &self,
        hub: JsValue,
        profile_id: String,
        password: String,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move {
            inner.unlock_profile(hub, profile_id, password).await
        }))
    }

    #[wasm_bindgen(js_name = changePassword)]
    pub fn change_password(
        &self,
        hub: JsValue,
        current: String,
        new: String,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move {
            inner.change_password(hub, current, new).await
        }))
    }

    #[wasm_bindgen(js_name = renameProfile)]
    pub fn rename_profile(&self, hub: JsValue, name: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(
            async move { inner.rename_profile(hub, name).await },
        ))
    }

    #[wasm_bindgen(js_name = setProfileButterfly)]
    pub fn set_profile_butterfly(&self, hub: JsValue, butterfly: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move {
            inner.set_profile_butterfly(hub, butterfly).await
        }))
    }

    #[wasm_bindgen(js_name = lockedKey)]
    pub fn locked_key(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.locked_key(hub).await }))
    }

    #[wasm_bindgen(js_name = linkStart)]
    pub fn link_start(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.link_start(hub).await }))
    }

    #[wasm_bindgen(js_name = linkWatch)]
    pub fn link_watch(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.link_watch(hub).await }))
    }

    #[wasm_bindgen(js_name = inviteWatch)]
    pub fn invite_watch(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.invite_watch(hub).await }))
    }

    #[wasm_bindgen(js_name = linkApprove)]
    pub fn link_approve(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.link_approve(hub).await }))
    }

    #[wasm_bindgen(js_name = linkDeny)]
    pub fn link_deny(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.link_deny(hub).await }))
    }

    #[wasm_bindgen(js_name = linkJoin)]
    pub fn link_join(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.link_join(hub).await }))
    }

    #[wasm_bindgen(js_name = linkFetch)]
    pub fn link_fetch(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.link_fetch(hub).await }))
    }

    pub fn pair(&self, link: String, device_name: String, kind: String) -> Promise {
        let inner = self.inner.clone();
        promise(async move { inner.pair(link, device_name, kind, None).await })
    }

    #[wasm_bindgen(js_name = pairWithCode)]
    pub fn pair_with_code(&self, code: String, device_name: String, kind: String) -> Promise {
        let inner = self.inner.clone();
        promise(async move { inner.pair_with_code(code, device_name, kind, None).await })
    }

    pub fn invite(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.invite(hub).await }))
    }

    #[wasm_bindgen(js_name = renameDevice)]
    pub fn rename_device(
        &self,
        hub: JsValue,
        id: String,
        name: String,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(
            async move { inner.rename_device(hub, id, name).await },
        ))
    }

    #[wasm_bindgen(js_name = removeDevice)]
    pub fn remove_device(&self, hub: JsValue, id: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.remove_device(hub, id).await }))
    }

    pub fn status(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.status(hub).await }))
    }

    pub fn unpair(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.unpair(hub).await }))
    }

    pub fn devices(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.devices(hub).await }))
    }

    //admin profiles manage the hub from a browser
    #[wasm_bindgen(js_name = hubSettings)]
    pub fn hub_settings(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.hub_settings(hub).await }))
    }

    #[wasm_bindgen(js_name = setHubSettings)]
    pub fn set_hub_settings(&self, hub: JsValue, settings: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        let settings = from_js::<HubSettings>(settings)?;
        Ok(promise(async move {
            inner.set_hub_settings(hub, settings).await
        }))
    }

    #[wasm_bindgen(js_name = updateHub)]
    pub fn update_hub(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.update_hub(hub).await }))
    }

    #[wasm_bindgen(js_name = removeProfile)]
    pub fn remove_profile(&self, hub: JsValue, id: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.remove_profile(hub, id).await }))
    }

    #[wasm_bindgen(js_name = setProfileRole)]
    pub fn set_profile_role(
        &self,
        hub: JsValue,
        id: String,
        role: String,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move {
            inner.set_profile_role(hub, id, role).await
        }))
    }

    #[wasm_bindgen(js_name = mcpOverview)]
    pub fn mcp_overview(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.mcp_overview(hub).await }))
    }

    #[wasm_bindgen(js_name = mcpManage)]
    pub fn mcp_manage(&self, hub: JsValue, id: String, action: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(
            async move { inner.mcp_manage(hub, id, action).await },
        ))
    }

    #[wasm_bindgen(js_name = modelsCatalog)]
    pub fn models_catalog(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.models_catalog(hub).await }))
    }

    #[wasm_bindgen(js_name = modelsSearch)]
    pub fn models_search(&self, hub: JsValue, query: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(
            async move { inner.models_search(hub, query).await },
        ))
    }

    #[wasm_bindgen(js_name = modelsFiles)]
    pub fn models_files(&self, hub: JsValue, repo: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.models_files(hub, repo).await }))
    }

    #[wasm_bindgen(js_name = modelsDownload)]
    pub fn models_download(
        &self,
        hub: JsValue,
        repo: String,
        file: String,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move {
            inner.models_download(hub, repo, file).await
        }))
    }

    #[wasm_bindgen(js_name = modelsCancel)]
    pub fn models_cancel(&self, hub: JsValue, file: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.models_cancel(hub, file).await }))
    }

    #[wasm_bindgen(js_name = modelsDelete)]
    pub fn models_delete(&self, hub: JsValue, file: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.models_delete(hub, file).await }))
    }

    #[wasm_bindgen(js_name = modelsStatus)]
    pub fn models_status(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.models_status(hub).await }))
    }

    pub fn push(
        &self,
        hub: JsValue,
        collection: String,
        records: JsValue,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        let records = from_js::<Vec<SyncRecord>>(records)?;
        Ok(promise(async move {
            inner.push(hub, collection, records).await
        }))
    }

    pub fn pull(&self, hub: JsValue, collection: String, since: f64) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move {
            inner.pull(hub, collection, since as u64).await
        }))
    }

    pub fn watch(&self, hub: JsValue, since: f64) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.watch(hub, since as u64).await }))
    }

    pub fn lease(&self, hub: JsValue, name: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.lease(hub, name).await }))
    }

    pub fn release(&self, hub: JsValue, name: String) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.release(hub, name).await }))
    }

    pub fn providers(&self, hub: JsValue) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        Ok(promise(async move { inner.providers(hub).await }))
    }

    //no local proxy in a browser
    pub fn forward(
        &self,
        hub: JsValue,
        provider: String,
        method: String,
        path: String,
        headers: JsValue,
        body: Vec<u8>,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        let head = ForwardHead {
            provider,
            method,
            path,
            headers: from_js(headers)?,
        };
        Ok(future_to_promise(async move {
            match inner.forward(&hub, head, &body).await.map_err(hub_error)? {
                Forwarded::Response {
                    status,
                    headers,
                    body,
                } => Ok(WebResponse {
                    status,
                    headers,
                    body: Arc::new(Mutex::new(Body::Stream(body))),
                }
                .into()),
                Forwarded::Refused(message) => Ok(WebResponse {
                    status: 502,
                    headers: vec![("content-type".into(), "text/plain".into())],
                    body: Arc::new(Mutex::new(Body::Text(message.into_bytes()))),
                }
                .into()),
            }
        }))
    }

    //only llama.cpp serves realtime
    pub fn realtime(
        &self,
        hub: JsValue,
        provider: String,
        path: String,
    ) -> Result<Promise, JsValue> {
        let (inner, hub) = (self.inner.clone(), from_js::<PairedHub>(hub)?);
        let head = ForwardHead {
            provider,
            method: "GET".into(),
            path,
            headers: Vec::new(),
        };
        Ok(future_to_promise(async move {
            let (send, recv) = inner.realtime(&hub, head).await.map_err(hub_error)?;
            Ok(WebRealtime {
                send: Arc::new(Mutex::new(send)),
                recv: Arc::new(Mutex::new(recv)),
                cancelled: Default::default(),
            }
            .into())
        }))
    }
}

#[wasm_bindgen(js_name = qrSvg)]
pub fn qr_svg(text: String, dark: String, light: String) -> Result<String, JsValue> {
    crate::qr_svg(text, dark, light).map_err(hub_error)
}

#[wasm_bindgen(js_name = openLockedKey)]
pub fn open_locked_key(lock: JsValue, password: String) -> Result<String, JsValue> {
    crate::open_locked_key(from_js::<ProfileLock>(lock)?, password).map_err(hub_error)
}

#[wasm_bindgen(js_name = profileBackupKey)]
pub fn profile_backup_key(key: String) -> Result<String, JsValue> {
    crate::profile_backup_key(key).map_err(hub_error)
}

#[wasm_bindgen]
impl WebResponse {
    #[wasm_bindgen(getter)]
    pub fn status(&self) -> u16 {
        self.status
    }

    #[wasm_bindgen(getter)]
    pub fn headers(&self) -> Result<JsValue, JsValue> {
        to_js(&self.headers)
    }

    //undefined once the body ended
    pub fn read(&self) -> Promise {
        let body = self.body.clone();
        future_to_promise(async move {
            let mut guard = body.lock().await;
            let stream = match &mut *guard {
                Body::Stream(stream) => stream,
                Body::Text(text) => {
                    let chunk = Uint8Array::from(text.as_slice());
                    *guard = Body::Done;
                    return Ok(chunk.into());
                }
                Body::Done => return Ok(JsValue::UNDEFINED),
            };
            let mut buf = vec![0u8; CHUNK_BYTES];
            let read = stream.read(&mut buf).await;
            match read {
                Ok(Some(n)) => Ok(Uint8Array::from(&buf[..n]).into()),
                Ok(None) => {
                    *guard = Body::Done;
                    Ok(JsValue::UNDEFINED)
                }
                Err(e) => {
                    *guard = Body::Done;
                    Err(hub_error(HubError::Unreachable(e.to_string())))
                }
            }
        })
    }

    //a closed reader stops the provider
    pub fn cancel(&self) {
        if let Ok(mut guard) = self.body.try_lock()
            && let Body::Stream(mut stream) = std::mem::replace(&mut *guard, Body::Done)
        {
            let _ = stream.stop(0u32.into());
        }
    }
}

#[wasm_bindgen]
impl WebRealtime {
    pub fn send(&self, text: String) -> Promise {
        let send = self.send.clone();
        future_to_promise(async move {
            write_frame(&mut *send.lock().await, &text)
                .await
                .map_err(|e| hub_error(HubError::Unreachable(e.to_string())))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    //undefined once the session ended
    pub fn read(&self) -> Promise {
        let (recv, cancelled) = (self.recv.clone(), self.cancelled.clone());
        future_to_promise(async move {
            let mut recv = recv.lock().await;
            let next = n0_future::future::or(read_frame::<_, String>(&mut *recv), async {
                cancelled.notified().await;
                Ok(None)
            })
            .await;
            match next {
                Ok(Some(text)) => Ok(JsValue::from_str(&text)),
                Ok(None) => Ok(JsValue::UNDEFINED),
                Err(e) => Err(hub_error(HubError::Unreachable(e.to_string()))),
            }
        })
    }

    pub fn close(&self) {
        self.cancelled.notify_one();
        if let Ok(mut send) = self.send.try_lock() {
            let _ = send.finish();
        }
    }
}
