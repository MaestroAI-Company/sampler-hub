//end to end profile keys
use anyhow::{Result, anyhow, bail};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use curve25519_dalek::montgomery::MontgomeryPoint;
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sampler_hub_proto::{LockedKey, Record};
use sha2::{Digest, Sha256};

const NONCE_BYTES: usize = 24;
//about a second on a phone
pub const MEMORY_KIB: u32 = 64 * 1024;
pub const PASSES: u32 = 3;
//floor the hub cannot go under
const MIN_MEMORY_KIB: u32 = 19 * 1024;
const MIN_PASSES: u32 = 2;
const SEALED_PREFIX: &str = "o1:";

pub type Key = [u8; 32];

//what a password unlocks
pub struct Derived {
    //proves the password to the hub
    pub auth_key: String,
    wrap_key: Key,
}

pub fn new_key() -> Result<Key> {
    random()
}

pub fn encode_key(key: &Key) -> String {
    B64.encode(key)
}

pub fn decode_key(text: &str) -> Result<Key> {
    B64.decode(text)?
        .try_into()
        .map_err(|_| anyhow!("a profile key has 32 bytes"))
}

//argon2 is slow on purpose
pub fn derive(password: &str, salt: &str, memory_kib: u32, passes: u32) -> Result<Derived> {
    if memory_kib < MIN_MEMORY_KIB || passes < MIN_PASSES {
        bail!("the hub asked for weak password settings");
    }
    let salt = B64.decode(salt)?;
    let params =
        Params::new(memory_kib, passes, 1, Some(64)).map_err(|e| anyhow!("argon2 params: {e}"))?;
    let mut seed = [0u8; 64];
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into(password.as_bytes(), &salt, &mut seed)
        .map_err(|e| anyhow!("argon2: {e}"))?;
    let (auth, wrap) = seed.split_at(32);
    Ok(Derived {
        auth_key: B64.encode(auth),
        wrap_key: wrap.try_into()?,
    })
}

//a fresh salt every time the password changes
pub fn lock(
    key: &Key,
    password: &str,
    memory_kib: u32,
    passes: u32,
) -> Result<(LockedKey, String)> {
    let salt = B64.encode(random::<16>()?);
    let derived = derive(password, &salt, memory_kib, passes)?;
    let wrapped = seal(&derived.wrap_key, b"opera-profile-key-1", key)?;
    Ok((
        LockedKey {
            salt,
            memory_kib,
            passes,
            wrapped: B64.encode(wrapped),
        },
        derived.auth_key,
    ))
}

pub fn unlock(derived: &Derived, locked: &LockedKey) -> Result<Key> {
    let plain = open(
        &derived.wrap_key,
        b"opera-profile-key-1",
        &B64.decode(&locked.wrapped)?,
    )
    .map_err(|_| anyhow!("wrong password"))?;
    plain
        .try_into()
        .map_err(|_| anyhow!("a profile key has 32 bytes"))
}

//hides ids, payloads and tombstones
pub fn seal_record(key: &Key, profile: &str, collection: &str, record: Record) -> Result<Record> {
    let hidden = hidden_id(key, collection, &record.id);
    let body = if record.deleted {
        ""
    } else {
        record.payload.as_deref().unwrap_or_default()
    };
    //the real id travels inside
    let mut plain = Vec::with_capacity(2 + record.id.len() + body.len());
    plain.extend_from_slice(&u16::try_from(record.id.len())?.to_be_bytes());
    plain.extend_from_slice(record.id.as_bytes());
    plain.extend_from_slice(body.as_bytes());
    let aad = record_aad(
        profile,
        collection,
        &hidden,
        record.updated_at,
        record.deleted,
    );
    let sealed = seal(&subkey(key, b"opera-records-1"), &aad, &plain)?;
    Ok(Record {
        id: hidden,
        updated_at: record.updated_at,
        deleted: record.deleted,
        payload: Some(format!("{SEALED_PREFIX}{}", B64.encode(sealed))),
    })
}

pub fn open_record(key: &Key, profile: &str, collection: &str, record: Record) -> Result<Record> {
    let sealed = record
        .payload
        .as_deref()
        .and_then(|payload| payload.strip_prefix(SEALED_PREFIX))
        .ok_or_else(|| anyhow!("record {} is not sealed", record.id))?;
    let aad = record_aad(
        profile,
        collection,
        &record.id,
        record.updated_at,
        record.deleted,
    );
    let plain = open(&subkey(key, b"opera-records-1"), &aad, &B64.decode(sealed)?)
        .map_err(|_| anyhow!("record {} does not open with this profile key", record.id))?;
    let (len, rest) = plain
        .split_first_chunk::<2>()
        .ok_or_else(|| anyhow!("corrupt record"))?;
    let len = u16::from_be_bytes(*len) as usize;
    if rest.len() < len {
        bail!("corrupt record");
    }
    let (id, body) = rest.split_at(len);
    let id = String::from_utf8(id.to_vec())?;
    //catches a payload moved to another id
    if hidden_id(key, collection, &id) != record.id {
        bail!("record {} was moved", record.id);
    }
    Ok(Record {
        id,
        updated_at: record.updated_at,
        deleted: record.deleted,
        payload: (!record.deleted)
            .then(|| String::from_utf8(body.to_vec()))
            .transpose()?,
    })
}

//stable so newest wins still works
fn hidden_id(key: &Key, collection: &str, id: &str) -> String {
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&subkey(key, b"opera-ids-1"))
        .expect("hmac takes any key length");
    mac.update(collection.as_bytes());
    mac.update(b"\0");
    mac.update(id.as_bytes());
    hex(&mac.finalize().into_bytes())
}

fn record_aad(
    profile: &str,
    collection: &str,
    id: &str,
    updated_at: u64,
    deleted: bool,
) -> Vec<u8> {
    format!("opera-record-1\0{profile}\0{collection}\0{id}\0{updated_at}\0{deleted}").into_bytes()
}

//one side of a device link
pub struct LinkKeys {
    secret: Key,
    pub public: String,
}

impl LinkKeys {
    pub fn new() -> Result<Self> {
        let secret = random()?;
        let public = MontgomeryPoint::mul_base_clamped(secret);
        Ok(Self {
            secret,
            public: B64.encode(public.as_bytes()),
        })
    }

    //stops a relay from grinding the digits
    pub fn commitment(&self) -> String {
        commitment(&self.public)
    }

    //approver seals the key for the joiner
    pub fn seal_for(&self, joiner: &str, profile_key: &Key) -> Result<String> {
        let key = self.link_key(&self.public, joiner, joiner)?;
        Ok(B64.encode(seal(&key, b"opera-link-1", profile_key)?))
    }

    pub fn open_from(&self, approver: &str, sealed: &str) -> Result<Key> {
        let key = self.link_key(approver, &self.public, approver)?;
        let plain = open(&key, b"opera-link-1", &B64.decode(sealed)?)
            .map_err(|_| anyhow!("the profile key does not open"))?;
        plain
            .try_into()
            .map_err(|_| anyhow!("a profile key has 32 bytes"))
    }

    fn link_key(&self, approver: &str, joiner: &str, other: &str) -> Result<Key> {
        let other: [u8; 32] = B64
            .decode(other)?
            .try_into()
            .map_err(|_| anyhow!("a link key has 32 bytes"))?;
        let shared = MontgomeryPoint(other).mul_clamped(self.secret);
        if shared.as_bytes() == &[0u8; 32] {
            bail!("invalid link key");
        }
        let mut key = [0u8; 32];
        Hkdf::<Sha256>::new(None, shared.as_bytes())
            .expand(
                format!("opera-link-key-1\0{approver}\0{joiner}").as_bytes(),
                &mut key,
            )
            .map_err(|_| anyhow!("hkdf output too long"))?;
        Ok(key)
    }
}

pub fn commitment(public: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"opera-link-commit-1");
    hash.update(public.as_bytes());
    hex(&hash.finalize())
}

//compared by the user on both screens
pub fn link_digits(approver: &str, joiner: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"opera-link-digits-1");
    hash.update(approver.as_bytes());
    hash.update(b"\0");
    hash.update(joiner.as_bytes());
    let digest = hash.finalize();
    let number = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) % 1_000_000;
    format!("{number:06}")
}

//the cloud backup never reuses the record key
pub fn backup_key(key: &Key) -> Key {
    subkey(key, b"opera-cloud-backup-1")
}

fn subkey(key: &Key, info: &[u8]) -> Key {
    let mut out = [0u8; 32];
    Hkdf::<Sha256>::new(None, key)
        .expand(info, &mut out)
        .expect("32 bytes fit hkdf");
    out
}

fn seal(key: &Key, aad: &[u8], plain: &[u8]) -> Result<Vec<u8>> {
    let nonce: [u8; NONCE_BYTES] = random()?;
    let sealed = XChaCha20Poly1305::new_from_slice(key)
        .map_err(|_| anyhow!("bad key"))?
        .encrypt(&XNonce::from(nonce), Payload { msg: plain, aad })
        .map_err(|_| anyhow!("encryption failed"))?;
    Ok([nonce.as_slice(), &sealed].concat())
}

fn open(key: &Key, aad: &[u8], sealed: &[u8]) -> Result<Vec<u8>> {
    if sealed.len() < NONCE_BYTES {
        bail!("corrupt ciphertext");
    }
    let (nonce, sealed) = sealed.split_at(NONCE_BYTES);
    let nonce: [u8; NONCE_BYTES] = nonce.try_into()?;
    XChaCha20Poly1305::new_from_slice(key)
        .map_err(|_| anyhow!("bad key"))?
        .decrypt(&XNonce::from(nonce), Payload { msg: sealed, aad })
        .map_err(|_| anyhow!("decryption failed"))
}

fn random<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).map_err(|e| anyhow!("no system randomness: {e}"))?;
    Ok(bytes)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    //cheapest allowed settings keep tests fast
    const TEST_MEMORY: u32 = MIN_MEMORY_KIB;
    const TEST_PASSES: u32 = MIN_PASSES;

    fn record(id: &str, payload: Option<&str>) -> Record {
        Record {
            id: id.into(),
            updated_at: 42,
            deleted: payload.is_none(),
            payload: payload.map(String::from),
        }
    }

    #[test]
    fn password_unlocks_the_key() {
        let key = new_key().unwrap();
        let (locked, auth) = lock(&key, "correct horse", TEST_MEMORY, TEST_PASSES).unwrap();
        let derived = derive(
            "correct horse",
            &locked.salt,
            locked.memory_kib,
            locked.passes,
        )
        .unwrap();
        assert_eq!(derived.auth_key, auth);
        assert_eq!(unlock(&derived, &locked).unwrap(), key);

        let wrong = derive(
            "wrong horse",
            &locked.salt,
            locked.memory_kib,
            locked.passes,
        )
        .unwrap();
        assert_ne!(wrong.auth_key, auth);
        assert!(unlock(&wrong, &locked).is_err());
        //a hub cannot weaken the settings
        assert!(derive("correct horse", &locked.salt, 1024, 1).is_err());
    }

    #[test]
    fn backup_key_is_its_own() {
        let key = new_key().unwrap();
        assert_eq!(backup_key(&key), backup_key(&key));
        assert_ne!(backup_key(&key), key);
        assert_ne!(backup_key(&key), subkey(&key, b"opera-records-1"));
    }

    #[test]
    fn records_round_trip_hidden() {
        let key = new_key().unwrap();
        let plain = record("conv-1", Some(r#"{"title":"secret plans"}"#));
        let sealed = seal_record(&key, "p", "conversations", plain.clone()).unwrap();
        assert_ne!(sealed.id, "conv-1");
        assert!(!sealed.payload.as_deref().unwrap().contains("secret"));
        //stable hidden id
        let again = seal_record(&key, "p", "conversations", plain.clone()).unwrap();
        assert_eq!(again.id, sealed.id);
        assert_eq!(
            open_record(&key, "p", "conversations", sealed).unwrap(),
            plain
        );

        let tombstone = record("conv-1", None);
        let sealed = seal_record(&key, "p", "conversations", tombstone.clone()).unwrap();
        assert!(sealed.payload.is_some());
        assert_eq!(
            open_record(&key, "p", "conversations", sealed).unwrap(),
            tombstone
        );
    }

    #[test]
    fn tampered_records_are_refused() {
        let key = new_key().unwrap();
        let sealed = seal_record(&key, "p", "messages", record("m", Some("hi"))).unwrap();
        let mut newer = sealed.clone();
        newer.updated_at += 1;
        assert!(open_record(&key, "p", "messages", newer).is_err());
        let mut deleted = sealed.clone();
        deleted.deleted = true;
        assert!(open_record(&key, "p", "messages", deleted).is_err());
        assert!(open_record(&key, "other", "messages", sealed.clone()).is_err());
        assert!(open_record(&key, "p", "conversations", sealed.clone()).is_err());
        assert!(open_record(&new_key().unwrap(), "p", "messages", sealed.clone()).is_err());
        //a payload moved under another record
        let other = seal_record(&key, "p", "messages", record("n", Some("yo"))).unwrap();
        let mut swapped = other.clone();
        swapped.payload = sealed.payload.clone();
        assert!(open_record(&key, "p", "messages", swapped).is_err());
        assert!(open_record(&key, "p", "messages", record("m", Some("plain"))).is_err());
    }

    #[test]
    fn link_hands_the_key_over() {
        let profile_key = new_key().unwrap();
        let approver = LinkKeys::new().unwrap();
        let joiner = LinkKeys::new().unwrap();
        assert_eq!(commitment(&approver.public), approver.commitment());
        let digits = link_digits(&approver.public, &joiner.public);
        assert_eq!(digits.len(), 6);
        assert_ne!(digits, link_digits(&joiner.public, &approver.public));

        let sealed = approver.seal_for(&joiner.public, &profile_key).unwrap();
        assert_eq!(
            joiner.open_from(&approver.public, &sealed).unwrap(),
            profile_key
        );
        //a relay with its own key gets nothing
        let relay = LinkKeys::new().unwrap();
        assert!(relay.open_from(&approver.public, &sealed).is_err());
    }

    #[test]
    fn fixed_vectors() {
        //catches a silent change in any primitive
        let key = [7u8; 32];
        assert_eq!(
            hidden_id(&key, "conversations", "conv-1"),
            "adf2d151de3264ff5ebd34768737e2761ddcd69f88858c5352d984bbb7f8eb2a"
        );
        assert_eq!(link_digits("a", "b"), "931443");
        assert_eq!(subkey(&key, b"opera-records-1")[..4], [78, 124, 177, 66]);
        assert_eq!(
            commitment("pub"),
            "59844e0623636fd23025353c1277049baecbf2c54bef334926e7a6fd07de7b3f"
        );
    }
}
