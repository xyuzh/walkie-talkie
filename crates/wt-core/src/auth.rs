//! Token signing and verification. Uses iroh's `SecretKey` for sign and `PublicKey` for verify
//! (same key material as the transport identity).

use anyhow::{anyhow, Context, Result};
use data_encoding::BASE32_NOPAD;
use iroh::{PublicKey, SecretKey};
use rand::RngCore;
use wt_proto::token::{Cap, CapabilityToken, SignedToken, TokenId};
use wt_proto::NodeId;

use crate::store::{unix_secs, Store, TokenRow};

/// Allow ±5 minutes of clock skew on `exp` checks.
pub const CLOCK_SKEW_SECS: i64 = 300;

/// Sign a new `CapabilityToken` using the issuer's secret key.
pub fn sign_token(
    issuer: &SecretKey,
    sub: NodeId,
    caps: Vec<Cap>,
    ttl_secs: Option<u64>,
) -> Result<(CapabilityToken, SignedToken)> {
    let iss_bytes = issuer.public().as_bytes().to_owned();
    let iss = NodeId(iss_bytes);
    let exp = match ttl_secs {
        Some(ttl) => unix_secs().saturating_add(ttl),
        // "unlimited" — encode as a far-future stamp (year 2100).
        None => 4_102_444_800,
    };
    let mut id: TokenId = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut id);

    let claims = CapabilityToken {
        iss,
        sub,
        exp,
        caps,
        id,
    };
    let mut claims_cbor = Vec::with_capacity(128);
    ciborium::into_writer(&claims, &mut claims_cbor).context("encode claims")?;
    let sig = issuer.sign(&claims_cbor);
    let signed = SignedToken {
        claims_cbor,
        sig: sig.to_bytes().to_vec(),
    };
    Ok((claims, signed))
}

/// Encode a signed token to base32 for CLI paste-ability.
pub fn token_encode_base32(t: &SignedToken) -> Result<String> {
    let mut buf = Vec::new();
    ciborium::into_writer(t, &mut buf).context("encode signed token")?;
    Ok(BASE32_NOPAD.encode(&buf))
}

pub fn token_decode_base32(s: &str) -> Result<SignedToken> {
    let s = s.trim();
    let bytes = BASE32_NOPAD
        .decode(s.as_bytes())
        .context("token is not valid base32 (no padding)")?;
    let t: SignedToken = ciborium::from_reader(&bytes[..]).context("decode signed token")?;
    Ok(t)
}

// ===== Local agent tokens (v3 orchestration) =====
//
// A *local* bearer credential, distinct from the Ed25519 `CapabilityToken` above. It binds a
// spawned/registered agent to the daemon over the 0600 Unix socket; the daemon resolves it to an
// agent via `Store::agent_by_token(blake3(token))`. No signatures — trust is the socket's owner
// permission plus possession of the secret. Never stored in the clear (only its blake3 hash is).

/// Mint a new random agent token: 32 random bytes encoded as no-pad base32 (52 chars).
pub fn new_agent_token() -> String {
    let mut secret = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut secret);
    BASE32_NOPAD.encode(&secret)
}

/// blake3 hash (32 bytes) of an agent token, for storage and lookup. Trims surrounding whitespace
/// so a token read from an env var / arg with a stray newline still matches.
pub fn agent_token_hash(token: &str) -> Vec<u8> {
    blake3::hash(token.trim().as_bytes()).as_bytes().to_vec()
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("bad signature")]
    BadSignature,
    #[error("malformed claims")]
    BadClaims,
    #[error("token expired (exp={exp}, now={now})")]
    Expired { exp: u64, now: u64 },
    #[error("subject mismatch: token.sub != initiator")]
    SubjectMismatch,
    #[error("token revoked")]
    Revoked,
    #[error("missing required capability: {0}")]
    MissingCap(Cap),
    #[error("issuer pubkey is not the local identity")]
    NotForUs,
    #[error("peer not authorized: {0}")]
    PeerNotKnown(NodeId),
}

/// Verify a signed token sent by `initiator` against an expected resource-owner identity (the
/// local install). Returns the decoded claims on success.
///
/// Rules:
/// - `iss` must equal `local_nodeid` (we only accept tokens we issued).
/// - signature verifies against `iss` pubkey.
/// - `sub == initiator`.
/// - `now() - skew <= exp`.
/// - token id not in revocation list.
/// - `required_cap` is in `claims.caps`.
pub async fn verify_token(
    signed: &SignedToken,
    local_nodeid: NodeId,
    initiator: NodeId,
    required_cap: Cap,
    store: &Store,
) -> std::result::Result<CapabilityToken, AuthError> {
    let claims = verify_signed_claims(signed)?;
    if claims.iss != local_nodeid {
        return Err(AuthError::NotForUs);
    }

    if claims.sub != initiator {
        return Err(AuthError::SubjectMismatch);
    }
    validate_claims(&claims, required_cap, store).await?;
    Ok(claims)
}

/// Decode a signed token and verify its Ed25519 signature against the issuer NodeId embedded in
/// the claims. This does not check whether the token is addressed to us, expired, revoked, or
/// carries a particular capability.
pub fn verify_signed_claims(
    signed: &SignedToken,
) -> std::result::Result<CapabilityToken, AuthError> {
    let claims: CapabilityToken =
        ciborium::from_reader(&signed.claims_cbor[..]).map_err(|_| AuthError::BadClaims)?;
    let iss_pk = PublicKey::from_bytes(&claims.iss.0).map_err(|_| AuthError::BadSignature)?;
    let sig_bytes: [u8; 64] = signed
        .sig
        .as_slice()
        .try_into()
        .map_err(|_| AuthError::BadSignature)?;
    let sig = ed25519_signature_from_bytes(&sig_bytes);
    iss_pk
        .verify(&signed.claims_cbor, &sig)
        .map_err(|_| AuthError::BadSignature)?;
    Ok(claims)
}

async fn validate_claims(
    claims: &CapabilityToken,
    required_cap: Cap,
    store: &Store,
) -> std::result::Result<(), AuthError> {
    let now = unix_secs() as i64;
    let exp = claims.exp as i64;
    if now - CLOCK_SKEW_SECS > exp {
        return Err(AuthError::Expired {
            exp: claims.exp,
            now: now as u64,
        });
    }
    if let Ok(Some(row)) = store.token_find(&claims.id).await {
        if row.revoked {
            return Err(AuthError::Revoked);
        }
    }
    if !claims.caps.iter().any(|c| c == &required_cap) {
        return Err(AuthError::MissingCap(required_cap));
    }
    Ok(())
}

/// Build a `TokenRow` from claims + signed bytes (revoked=false).
pub fn token_row(claims: &CapabilityToken, signed: &SignedToken) -> Result<TokenRow> {
    let mut raw = Vec::new();
    ciborium::into_writer(signed, &mut raw)?;
    Ok(TokenRow {
        id: claims.id,
        iss: claims.iss,
        sub: claims.sub,
        exp: claims.exp,
        caps: claims.caps.clone(),
        raw,
        revoked: false,
    })
}

/// Decode signed token claims (no verification) — used when importing/inspecting.
pub fn decode_claims(signed: &SignedToken) -> Result<CapabilityToken> {
    ciborium::from_reader(&signed.claims_cbor[..]).map_err(|e| anyhow!("decode claims: {e}"))
}

fn ed25519_signature_from_bytes(b: &[u8; 64]) -> iroh::Signature {
    // iroh re-exports ed25519_dalek::Signature; build from bytes.
    iroh::Signature::from_bytes(b)
}

/// Quick guard for incoming streams: peer must be in our `peers` table.
pub async fn require_peer_known(
    store: &Store,
    nodeid: &NodeId,
) -> std::result::Result<(), AuthError> {
    use crate::store::PeerSelectorBytes;
    match store.peer_get(&PeerSelectorBytes::NodeId(*nodeid)).await {
        Ok(Some(_)) => Ok(()),
        _ => Err(AuthError::PeerNotKnown(*nodeid)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_store() -> Store {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "wt-auth-test-{}-{}-{}.db",
            std::process::id(),
            unix_secs(),
            n
        ));
        Store::open_at(&path).unwrap()
    }

    fn node_from_secret(secret: &SecretKey) -> NodeId {
        NodeId(secret.public().as_bytes().to_owned())
    }

    fn signed_claims(issuer: &SecretKey, claims: &CapabilityToken) -> SignedToken {
        let mut claims_cbor = Vec::new();
        ciborium::into_writer(claims, &mut claims_cbor).unwrap();
        let sig = issuer.sign(&claims_cbor);
        SignedToken {
            claims_cbor,
            sig: sig.to_bytes().to_vec(),
        }
    }

    #[tokio::test]
    async fn sign_and_verify_token_accepts_valid_claims() {
        let issuer = SecretKey::generate();
        let subject = SecretKey::generate();
        let store = temp_store();

        let (claims, signed) = sign_token(
            &issuer,
            node_from_secret(&subject),
            vec![Cap::Msg],
            Some(60),
        )
        .unwrap();
        let verified = verify_token(
            &signed,
            node_from_secret(&issuer),
            node_from_secret(&subject),
            Cap::Msg,
            &store,
        )
        .await
        .unwrap();

        assert_eq!(verified.iss, node_from_secret(&issuer));
        assert_eq!(verified.sub, node_from_secret(&subject));
        assert_eq!(verified.id, claims.id);
    }

    #[test]
    fn token_base32_roundtrips_and_rejects_garbage() {
        let issuer = SecretKey::generate();
        let subject = SecretKey::generate();
        let (_, signed) = sign_token(
            &issuer,
            node_from_secret(&subject),
            vec![Cap::Msg],
            Some(60),
        )
        .unwrap();

        let encoded = token_encode_base32(&signed).unwrap();
        let decoded = token_decode_base32(&format!(" \n{encoded}\t")).unwrap();
        assert_eq!(decoded.claims_cbor, signed.claims_cbor);
        assert_eq!(decoded.sig, signed.sig);
        assert!(token_decode_base32("not-base32!").is_err());
    }

    #[tokio::test]
    async fn verify_rejects_wrong_issuer_subject_missing_cap_and_revocation() {
        let issuer = SecretKey::generate();
        let other = SecretKey::generate();
        let subject = SecretKey::generate();
        let store = temp_store();
        let (_, signed) = sign_token(
            &issuer,
            node_from_secret(&subject),
            vec![Cap::Msg],
            Some(60),
        )
        .unwrap();

        assert!(matches!(
            verify_token(
                &signed,
                node_from_secret(&other),
                node_from_secret(&subject),
                Cap::Msg,
                &store
            )
            .await,
            Err(AuthError::NotForUs)
        ));
        assert!(matches!(
            verify_token(
                &signed,
                node_from_secret(&issuer),
                node_from_secret(&other),
                Cap::Msg,
                &store
            )
            .await,
            Err(AuthError::SubjectMismatch)
        ));

        let (_, no_caps) =
            sign_token(&issuer, node_from_secret(&subject), Vec::new(), Some(60)).unwrap();
        assert!(matches!(
            verify_token(
                &no_caps,
                node_from_secret(&issuer),
                node_from_secret(&subject),
                Cap::Msg,
                &store
            )
            .await,
            Err(AuthError::MissingCap(Cap::Msg))
        ));

        let (claims, signed) = sign_token(
            &issuer,
            node_from_secret(&subject),
            vec![Cap::Msg],
            Some(60),
        )
        .unwrap();
        let mut row = token_row(&claims, &signed).unwrap();
        row.revoked = true;
        store.token_insert(&row).await.unwrap();
        assert!(matches!(
            verify_token(
                &signed,
                node_from_secret(&issuer),
                node_from_secret(&subject),
                Cap::Msg,
                &store
            )
            .await,
            Err(AuthError::Revoked)
        ));
    }

    #[tokio::test]
    async fn verify_rejects_expired_token_outside_skew() {
        let issuer = SecretKey::generate();
        let subject = SecretKey::generate();
        let store = temp_store();
        let claims = CapabilityToken {
            iss: node_from_secret(&issuer),
            sub: node_from_secret(&subject),
            exp: unix_secs().saturating_sub((CLOCK_SKEW_SECS as u64) + 1),
            caps: vec![Cap::Msg],
            id: [3; 16],
        };
        let signed = signed_claims(&issuer, &claims);

        assert!(matches!(
            verify_token(
                &signed,
                node_from_secret(&issuer),
                node_from_secret(&subject),
                Cap::Msg,
                &store
            )
            .await,
            Err(AuthError::Expired { .. })
        ));
    }

    #[test]
    fn verify_signed_claims_rejects_forgery_and_malformed_payloads() {
        let issuer = SecretKey::generate();
        let subject = SecretKey::generate();
        let (_, mut signed) = sign_token(
            &issuer,
            node_from_secret(&subject),
            vec![Cap::Msg],
            Some(60),
        )
        .unwrap();

        signed.sig[0] ^= 0x01;
        assert!(matches!(
            verify_signed_claims(&signed),
            Err(AuthError::BadSignature)
        ));

        let short_sig = SignedToken {
            claims_cbor: signed.claims_cbor.clone(),
            sig: vec![0; 63],
        };
        assert!(matches!(
            verify_signed_claims(&short_sig),
            Err(AuthError::BadSignature)
        ));

        let malformed = SignedToken {
            claims_cbor: vec![0xff],
            sig: vec![0; 64],
        };
        assert!(matches!(
            verify_signed_claims(&malformed),
            Err(AuthError::BadClaims)
        ));
    }

    #[test]
    fn agent_token_is_random_and_hash_is_stable() {
        let t1 = new_agent_token();
        let t2 = new_agent_token();
        assert_ne!(t1, t2);
        assert_eq!(t1.len(), 52); // base32(32 bytes), no padding
        assert_eq!(agent_token_hash(&t1), agent_token_hash(&t1));
        assert_eq!(agent_token_hash(&t1).len(), 32);
        assert_ne!(agent_token_hash(&t1), agent_token_hash(&t2));
        // Surrounding whitespace is ignored (env/arg with a trailing newline still matches).
        assert_eq!(
            agent_token_hash(&t1),
            agent_token_hash(&format!("  {t1}\n"))
        );
    }
}
