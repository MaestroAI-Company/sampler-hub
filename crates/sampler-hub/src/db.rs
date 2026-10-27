use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use tokio::sync::watch;

use anyhow::{Context, Result, anyhow, bail};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use sampler_hub_proto::{LockedKey, Record};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

const DB_FILE: &str = "hub.db";
const KEY_FILE: &str = "data.key";
const NONCE_BYTES: usize = 24;
//rows carry their profile since 1
const SCHEMA_VERSION: u32 = 1;
//keeps a pull page well under the frame limit
const PULL_BYTES: usize = 2 * 1024 * 1024;

//what a password must match to unlock
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileKey {
    pub locked: LockedKey,
    pub auth_hash: String,
}

//contents sealed per row
pub struct Db {
    conn: Mutex<Connection>,
    cipher: XChaCha20Poly1305,
    //latest seq so watchers wake on writes
    changes: watch::Sender<u64>,
}

impl Db {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let key = data_key(data_dir)?;
        Self::open_with_key(&data_dir.join(DB_FILE), &key)
    }

    pub fn open_with_key(path: &Path, key: &[u8; 32]) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let conn =
            Connection::open(path).with_context(|| format!("cannot open {}", path.display()))?;
        let version: u32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        //devices push everything again
        if version < SCHEMA_VERSION {
            conn.execute_batch(
                "DROP INDEX IF EXISTS records_seq;
                DROP TABLE IF EXISTS records;",
            )?;
        }
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS records (
                profile TEXT NOT NULL,
                collection TEXT NOT NULL,
                id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                device TEXT NOT NULL,
                deleted INTEGER NOT NULL,
                payload BLOB,
                PRIMARY KEY (profile, collection, id)
            );
            CREATE INDEX IF NOT EXISTS records_seq ON records (profile, collection, seq);
            CREATE TABLE IF NOT EXISTS profile_keys (
                profile TEXT PRIMARY KEY,
                keys BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mcp_secrets (
                server TEXT PRIMARY KEY,
                secrets BLOB NOT NULL
            );
            DROP TABLE IF EXISTS grants;
            PRAGMA user_version = {SCHEMA_VERSION};"
        ))?;
        let cipher = XChaCha20Poly1305::new_from_slice(key).map_err(|_| anyhow!("bad data key"))?;
        let latest: u64 =
            conn.query_row("SELECT COALESCE(MAX(seq), 0) FROM records", [], |row| {
                row.get(0)
            })?;
        Ok(Self {
            conn: Mutex::new(conn),
            cipher,
            changes: watch::Sender::new(latest),
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    pub fn push(
        &self,
        profile: &str,
        collection: &str,
        device: &str,
        records: &[Record],
    ) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let mut latest = None;
        for record in records {
            let current: Option<u64> = tx
                .query_row(
                    "SELECT updated_at FROM records
                     WHERE profile = ?1 AND collection = ?2 AND id = ?3",
                    params![profile, collection, record.id],
                    |row| row.get(0),
                )
                .optional()?;
            //ties keep the first so echoes never bump seq
            if current.is_some_and(|current| current >= record.updated_at) {
                continue;
            }
            let seq: u64 =
                tx.query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM records", [], |row| {
                    row.get(0)
                })?;
            //sealed tombstones keep their payload
            let payload = match &record.payload {
                Some(payload) => {
                    Some(self.seal(&aad(&[profile, collection, &record.id]), payload.as_bytes())?)
                }
                None => None,
            };
            tx.execute(
                "INSERT OR REPLACE INTO records (profile, collection, id, seq, updated_at, device, deleted, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![profile, collection, record.id, seq, record.updated_at, device, record.deleted, payload],
            )?;
            latest = Some(seq);
        }
        tx.commit()?;
        if let Some(seq) = latest {
            self.changes.send_replace(seq);
        }
        Ok(())
    }

    //last seq becomes the next cursor
    pub fn pull(
        &self,
        profile: &str,
        collection: &str,
        since: u64,
        limit: usize,
    ) -> Result<(Vec<Record>, u64, bool)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, seq, updated_at, deleted, payload FROM records
             WHERE profile = ?1 AND collection = ?2 AND seq > ?3 ORDER BY seq LIMIT ?4",
        )?;
        let rows = stmt.query_map(
            params![profile, collection, since, limit as i64 + 1],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                ))
            },
        )?;
        let mut records = Vec::new();
        let mut cursor = since;
        let mut more = false;
        let mut bytes = 0;
        for row in rows {
            let (id, seq, updated_at, deleted, sealed) = row?;
            if records.len() == limit || bytes > PULL_BYTES {
                more = true;
                break;
            }
            bytes += sealed.as_ref().map_or(0, Vec::len);
            let payload = match sealed {
                Some(sealed) => Some(String::from_utf8(
                    self.unseal(&aad(&[profile, collection, &id]), &sealed)?,
                )?),
                None => None,
            };
            records.push(Record {
                id,
                updated_at,
                deleted,
                payload,
            });
            cursor = seq;
        }
        Ok((records, cursor, more))
    }

    //newest seq of a profile
    pub fn latest(&self, profile: &str) -> Result<u64> {
        Ok(self.conn.lock().unwrap().query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM records WHERE profile = ?1",
            params![profile],
            |row| row.get(0),
        )?)
    }

    pub fn remove_profile(&self, profile: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM records WHERE profile = ?1", params![profile])?;
        conn.execute(
            "DELETE FROM profile_keys WHERE profile = ?1",
            params![profile],
        )?;
        Ok(())
    }

    pub fn put_profile_key(&self, profile: &str, key: &ProfileKey) -> Result<()> {
        let sealed = self.seal(&aad(&["profile_keys", profile]), &serde_json::to_vec(key)?)?;
        self.conn.lock().unwrap().execute(
            "INSERT OR REPLACE INTO profile_keys (profile, keys) VALUES (?1, ?2)",
            params![profile, sealed],
        )?;
        Ok(())
    }

    pub fn profile_key(&self, profile: &str) -> Result<Option<ProfileKey>> {
        let sealed: Option<Vec<u8>> = self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT keys FROM profile_keys WHERE profile = ?1",
                params![profile],
                |row| row.get(0),
            )
            .optional()?;
        match sealed {
            Some(sealed) => Ok(Some(serde_json::from_slice(
                &self.unseal(&aad(&["profile_keys", profile]), &sealed)?,
            )?)),
            None => Ok(None),
        }
    }

    //secret config of a hosted mcp server
    pub fn put_mcp_secrets(&self, server: &str, secrets: &BTreeMap<String, String>) -> Result<()> {
        let sealed = self.seal(
            &aad(&["mcp_secrets", server]),
            &serde_json::to_vec(secrets)?,
        )?;
        self.conn.lock().unwrap().execute(
            "INSERT OR REPLACE INTO mcp_secrets (server, secrets) VALUES (?1, ?2)",
            params![server, sealed],
        )?;
        Ok(())
    }

    pub fn mcp_secrets(&self, server: &str) -> Result<BTreeMap<String, String>> {
        let sealed: Option<Vec<u8>> = self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT secrets FROM mcp_secrets WHERE server = ?1",
                params![server],
                |row| row.get(0),
            )
            .optional()?;
        match sealed {
            Some(sealed) => Ok(serde_json::from_slice(
                &self.unseal(&aad(&["mcp_secrets", server]), &sealed)?,
            )?),
            None => Ok(BTreeMap::new()),
        }
    }

    pub fn drop_mcp_secrets(&self, server: &str) -> Result<()> {
        self.conn
            .lock()
            .unwrap()
            .execute("DELETE FROM mcp_secrets WHERE server = ?1", params![server])?;
        Ok(())
    }

    fn seal(&self, aad: &[u8], plain: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; NONCE_BYTES];
        getrandom::fill(&mut nonce).map_err(|e| anyhow!("no system randomness: {e}"))?;
        let sealed = self
            .cipher
            .encrypt(&XNonce::from(nonce), Payload { msg: plain, aad })
            .map_err(|_| anyhow!("encryption failed"))?;
        Ok([nonce.as_slice(), &sealed].concat())
    }

    fn unseal(&self, aad: &[u8], sealed: &[u8]) -> Result<Vec<u8>> {
        if sealed.len() < NONCE_BYTES {
            bail!("corrupt row");
        }
        let (nonce, sealed) = sealed.split_at(NONCE_BYTES);
        let nonce: [u8; NONCE_BYTES] = nonce.try_into()?;
        self.cipher
            .decrypt(&XNonce::from(nonce), Payload { msg: sealed, aad })
            .map_err(|_| anyhow!("cannot decrypt row, wrong data key"))
    }
}

//binds ciphertext to its row
fn aad(parts: &[&str]) -> Vec<u8> {
    parts.join("\0").into_bytes()
}

//os keychain or a key file
fn data_key(data_dir: &Path) -> Result<[u8; 32]> {
    let file = data_dir.join(KEY_FILE);
    if file.exists() {
        let bytes = std::fs::read(&file)?;
        return bytes
            .try_into()
            .map_err(|_| anyhow!("corrupt {}", file.display()));
    }
    #[cfg(any(windows, target_os = "macos"))]
    match keychain_key(data_dir) {
        Ok(key) => return Ok(key),
        //a new key would lock data away
        Err(e) if data_dir.join(DB_FILE).exists() => {
            bail!("os keychain unavailable and {DB_FILE} already exists: {e}")
        }
        Err(e) => tracing::warn!("os keychain unavailable, using {}: {e}", file.display()),
    }
    let mut key = [0u8; 32];
    getrandom::fill(&mut key).map_err(|e| anyhow!("no system randomness: {e}"))?;
    if !crate::config::write_private(&file, &key)? {
        return data_key(data_dir);
    }
    Ok(key)
}

//a moved data dir keeps its key
pub(crate) fn copy_keychain_key(old: &Path, new: &Path) -> Result<()> {
    #[cfg(any(windows, target_os = "macos"))]
    {
        let entry = |dir: &Path| keyring::Entry::new("sampler-hub", &dir.display().to_string());
        match entry(old)?.get_secret() {
            Ok(secret) => entry(new)?.set_secret(&secret)?,
            Err(keyring::Error::NoEntry) => {}
            Err(e) => return Err(e.into()),
        }
    }
    #[cfg(not(any(windows, target_os = "macos")))]
    let _ = (old, new);
    Ok(())
}

pub(crate) fn forget_keychain_key(old: &Path) {
    #[cfg(any(windows, target_os = "macos"))]
    if let Ok(entry) = keyring::Entry::new("sampler-hub", &old.display().to_string()) {
        let _ = entry.delete_credential();
    }
    #[cfg(not(any(windows, target_os = "macos")))]
    let _ = old;
}

#[cfg(any(windows, target_os = "macos"))]
fn keychain_key(data_dir: &Path) -> Result<[u8; 32]> {
    let entry = keyring::Entry::new("sampler-hub", &data_dir.display().to_string())?;
    match entry.get_secret() {
        Ok(bytes) => bytes
            .try_into()
            .map_err(|_| anyhow!("corrupt keychain entry")),
        Err(keyring::Error::NoEntry) => {
            let mut key = [0u8; 32];
            getrandom::fill(&mut key).map_err(|e| anyhow!("no system randomness: {e}"))?;
            entry.set_secret(&key)?;
            Ok(key)
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> Db {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).unwrap();
        let suffix: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let path = std::env::temp_dir()
            .join(format!("sampler-hub-db-{suffix}"))
            .join(DB_FILE);
        Db::open_with_key(&path, &[7u8; 32]).unwrap()
    }

    fn record(id: &str, updated_at: u64, payload: &str) -> Record {
        Record {
            id: id.into(),
            updated_at,
            deleted: false,
            payload: Some(payload.into()),
        }
    }

    #[test]
    fn last_writer_wins() {
        let db = temp_db();
        db.push("p", "providers", "a", &[record("x", 10, "new")])
            .unwrap();
        db.push("p", "providers", "b", &[record("x", 5, "old")])
            .unwrap();
        //a tie keeps the first write
        db.push("p", "providers", "z", &[record("x", 10, "echo")])
            .unwrap();
        let (records, cursor, more) = db.pull("p", "providers", 0, 10).unwrap();
        assert_eq!(records, vec![record("x", 10, "new")]);
        assert!(!more);

        let tombstone = Record {
            id: "x".into(),
            updated_at: 20,
            deleted: true,
            payload: None,
        };
        db.push("p", "providers", "b", std::slice::from_ref(&tombstone))
            .unwrap();
        let (records, _, _) = db.pull("p", "providers", cursor, 10).unwrap();
        assert_eq!(records, vec![tombstone]);
    }

    #[test]
    fn pull_pages_by_cursor() {
        let db = temp_db();
        let records: Vec<_> = (0..5).map(|i| record(&format!("r{i}"), 1, "p")).collect();
        db.push("p", "providers", "a", &records).unwrap();
        let (page, cursor, more) = db.pull("p", "providers", 0, 3).unwrap();
        assert_eq!((page.len(), more), (3, true));
        let (page, _, more) = db.pull("p", "providers", cursor, 3).unwrap();
        assert_eq!((page.len(), more), (2, false));
    }

    #[test]
    fn payloads_are_sealed() {
        let db = temp_db();
        db.push("p", "providers", "a", &[record("x", 1, "sk-secret-key")])
            .unwrap();
        let raw: Vec<u8> = db
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT payload FROM records", [], |row| row.get(0))
            .unwrap();
        assert!(!raw.windows(13).any(|w| w == b"sk-secret-key"));
    }

    #[test]
    fn profiles_never_see_each_other() {
        let db = temp_db();
        db.push("owner", "conversations", "a", &[record("x", 1, "mine")])
            .unwrap();
        db.push(
            "flatmate",
            "conversations",
            "b",
            &[record("x", 2, "theirs")],
        )
        .unwrap();
        let (records, _, _) = db.pull("owner", "conversations", 0, 10).unwrap();
        assert_eq!(records, vec![record("x", 1, "mine")]);
        let (records, _, _) = db.pull("flatmate", "conversations", 0, 10).unwrap();
        assert_eq!(records, vec![record("x", 2, "theirs")]);
        assert!(db.latest("flatmate").unwrap() > db.latest("owner").unwrap());

        db.remove_profile("flatmate").unwrap();
        assert_eq!(db.latest("flatmate").unwrap(), 0);
        assert_eq!(db.pull("owner", "conversations", 0, 10).unwrap().0.len(), 1);
    }

    #[test]
    fn old_schema_is_dropped() {
        let path = std::env::temp_dir()
            .join(format!("sampler-hub-db-old-{}", std::process::id()))
            .join(DB_FILE);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let _ = std::fs::remove_file(&path);
        Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE records (collection TEXT, id TEXT, seq INTEGER, updated_at INTEGER,
                 device TEXT, deleted INTEGER, payload BLOB, PRIMARY KEY (collection, id));
                 INSERT INTO records VALUES ('conversations', 'x', 1, 1, 'a', 0, NULL);",
            )
            .unwrap();
        let db = Db::open_with_key(&path, &[7u8; 32]).unwrap();
        db.push("p", "conversations", "a", &[record("x", 1, "new")])
            .unwrap();
        drop(db);
        //a second open keeps the rows
        let db = Db::open_with_key(&path, &[7u8; 32]).unwrap();
        assert_eq!(db.pull("p", "conversations", 0, 10).unwrap().0.len(), 1);
    }
}
