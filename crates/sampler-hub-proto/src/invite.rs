//one time codes typed where no camera can scan
use anyhow::{Result, anyhow, bail};
use argon2::{Algorithm, Argon2, Params, Version};
use hmac::{Hmac, KeyInit, Mac};
use iroh::{EndpointId, SecretKey};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

pub const INVITE_ALPN: &[u8] = b"sampler/invite/1";
//crockford base32 skips look alike letters
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const CODE_LEN: usize = 8;
const SALT: &[u8] = b"sampler-hub-invite-1";
//owasp minimum, slows guessing from the endpoint id
const ARGON_MEMORY_KIB: u32 = 19 * 1024;
const ARGON_PASSES: u32 = 2;

#[derive(Debug, Serialize, Deserialize)]
pub struct InviteRequest {
    //hmac of the joining device id
    pub proof: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InviteResponse {
    Link { link: String },
    Rejected,
}

//both sides derive the same endpoint from the code
pub struct InviteKeys {
    pub endpoint: SecretKey,
    proof: [u8; 32],
}

//shown as XXXX-XXXX
pub fn new_code() -> Result<String> {
    let mut bytes = [0u8; CODE_LEN];
    getrandom::fill(&mut bytes).map_err(|e| anyhow!("no system randomness: {e}"))?;
    let code: String = bytes
        .iter()
        .map(|b| ALPHABET[(b & 31) as usize] as char)
        .collect();
    Ok(format!("{}-{}", &code[..4], &code[4..]))
}

//forgives case, spaces, dashes and look alikes
pub fn normalize(code: &str) -> Result<String> {
    let normalized: String = code
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .map(|c| match c.to_ascii_uppercase() {
            'O' => '0',
            'I' | 'L' => '1',
            other => other,
        })
        .collect();
    if normalized.len() != CODE_LEN || !normalized.bytes().all(|b| ALPHABET.contains(&b)) {
        bail!("a pairing code has {CODE_LEN} letters or digits");
    }
    Ok(normalized)
}

pub fn derive(code: &str) -> Result<InviteKeys> {
    let code = normalize(code)?;
    let params = Params::new(ARGON_MEMORY_KIB, ARGON_PASSES, 1, Some(64))
        .map_err(|e| anyhow!("argon2 params: {e}"))?;
    let mut seed = [0u8; 64];
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into(code.as_bytes(), SALT, &mut seed)
        .map_err(|e| anyhow!("argon2: {e}"))?;
    let (endpoint, proof) = seed.split_at(32);
    Ok(InviteKeys {
        endpoint: SecretKey::from_bytes(endpoint.try_into()?),
        proof: proof.try_into()?,
    })
}

impl InviteKeys {
    pub fn id(&self) -> EndpointId {
        self.endpoint.public()
    }

    //bound to the device so a relay cannot replay it
    pub fn proof(&self, device: &EndpointId) -> String {
        let tag = self.mac(device).finalize().into_bytes();
        tag.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn verify(&self, device: &EndpointId, proof: &str) -> bool {
        let Some(bytes) = decode_hex(proof) else {
            return false;
        };
        self.mac(device).verify_slice(&bytes).is_ok()
    }

    fn mac(&self, device: &EndpointId) -> Hmac<Sha256> {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&self.proof)
            .expect("hmac takes any key length");
        mac.update(device.as_bytes());
        mac
    }
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_survive_typing() {
        let code = new_code().unwrap();
        assert_eq!(code.len(), CODE_LEN + 1);
        let typed = code.to_lowercase().replace('-', " ");
        assert_eq!(normalize(&typed).unwrap(), code.replace('-', ""));
        assert!(normalize("ABCD-EFG").is_err());
        assert!(normalize("ABCD-EFGU").is_err());
    }

    #[test]
    fn proof_needs_the_code() {
        let keys = derive("abcd-efgh").unwrap();
        let same = derive("ABCD EFGH").unwrap();
        assert_eq!(keys.id(), same.id());
        let device = SecretKey::generate().public();
        let other = SecretKey::generate().public();
        let proof = same.proof(&device);
        assert!(keys.verify(&device, &proof));
        assert!(!keys.verify(&other, &proof));
        assert!(!derive("ABCD-EFGJ").unwrap().verify(&device, &proof));
    }
}
