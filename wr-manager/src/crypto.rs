use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Nonce};
use anyhow::{Context, Result};

/// AES-256-GCM encryption/decryption for secrets at rest.
pub struct SecretCrypto {
    cipher: Aes256Gcm,
}

impl SecretCrypto {
    /// Build from the `WRT_SECRET_ENCRYPTION_KEY` environment variable.
    /// The key must be hex-encoded (64 hex chars = 32 bytes).
    pub fn from_env() -> Result<Self> {
        let hex_key = std::env::var("WRT_SECRET_ENCRYPTION_KEY")
            .context("WRT_SECRET_ENCRYPTION_KEY must be set")?;
        Self::from_hex(&hex_key)
    }

    /// Build from a hex-encoded key string (64 hex chars = 32 bytes).
    pub fn from_hex(hex_key: &str) -> Result<Self> {
        let key_bytes = hex::decode(hex_key).context("encryption key must be valid hex")?;
        anyhow::ensure!(
            key_bytes.len() == 32,
            "encryption key must be 32 bytes (64 hex chars), got {} bytes",
            key_bytes.len()
        );
        let key = aes_gcm::Key::<Aes256Gcm>::from_slice(&key_bytes);
        Ok(Self {
            cipher: Aes256Gcm::new(key),
        })
    }

    /// Encrypt a plaintext value. Returns `(ciphertext, nonce)`.
    pub fn encrypt(&self, plaintext: &str) -> Result<(Vec<u8>, Vec<u8>)> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext.as_bytes())
            .map_err(|e| anyhow::anyhow!("encryption failed: {e}"))?;
        Ok((ciphertext, nonce.to_vec()))
    }

    /// Decrypt ciphertext using the provided nonce.
    pub fn decrypt(&self, ciphertext: &[u8], nonce: &[u8]) -> Result<String> {
        let nonce = Nonce::from_slice(nonce);
        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| anyhow::anyhow!("decryption failed: {e}"))?;
        String::from_utf8(plaintext).context("decrypted secret is not valid UTF-8")
    }
}
