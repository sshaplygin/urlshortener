//! Collision-free short-code generation.
//!
//! Codes are minted from a strictly increasing counter held in YDB, so the same
//! value is never handed out twice — duplication is impossible by construction
//! rather than merely unlikely.
//!
//! A bare counter would publish `1, 2, 3, …`, letting anyone enumerate every
//! link the service has ever created. So the counter value is first run through
//! a keyed Feistel permutation. A Feistel network is a bijection, so scrambling
//! preserves uniqueness exactly while making the published codes
//! indistinguishable from random without the key.
//!
//! Codes are still not derived from the target URL: a content hash lets an
//! attacker precompute a colliding URL, claim the code first, and take over the
//! link a later caller expects to own.

use std::fmt;

use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use ydb::TableClient;

use crate::db;

/// Alphabet for generated short codes. Base62, so codes stay URL-safe and
/// case-sensitive without needing escaping.
const CODE_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// Rounds in the Feistel network. Three rounds already give a pseudo-random
/// permutation; each half here is only ~24 bits wide, so use well past that.
const FEISTEL_ROUNDS: u8 = 8;

/// Longest code whose space (`62^n`) still fits in a `u64`. `62^11` overflows.
pub const MAX_CODE_LENGTH: usize = 10;

#[derive(Debug)]
pub enum CodeError {
    /// Every code of the configured length has been issued. Raising
    /// `CODE_LENGTH` widens the space for codes minted from then on.
    SpaceExhausted,
    Db(ydb::YdbError),
}

impl fmt::Display for CodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodeError::SpaceExhausted => f.write_str("short code space is exhausted"),
            CodeError::Db(err) => write!(f, "reserve code block: {err}"),
        }
    }
}

impl std::error::Error for CodeError {}

/// Hands out short codes that can never collide.
pub struct CodeAllocator {
    table_client: TableClient,
    /// How many counter values to claim per database round-trip.
    block_size: u64,
    /// Size of the code space, `62^length`.
    space: u64,
    length: usize,
    cipher: Feistel,
    reserved: Mutex<Block>,
}

/// A half-open range of counter values claimed by this process.
#[derive(Default)]
struct Block {
    next: u64,
    end: u64,
}

impl CodeAllocator {
    /// `secret` keys the permutation. It must stay stable for the lifetime of
    /// the deployment: a different key is a different permutation, so codes
    /// minted after a rotation can collide with codes minted before it.
    pub fn new(
        table_client: TableClient,
        secret: &str,
        length: usize,
        block_size: u64,
    ) -> Result<Self, CodeError> {
        let space = code_space(length).ok_or(CodeError::SpaceExhausted)?;

        Ok(CodeAllocator {
            table_client,
            block_size: block_size.max(1),
            space,
            length,
            cipher: Feistel::new(secret, space),
            reserved: Mutex::new(Block::default()),
        })
    }

    /// Returns a code no previous call has returned.
    pub async fn next_code(&self) -> Result<String, CodeError> {
        let id = self.next_id().await?;

        if id >= self.space {
            return Err(CodeError::SpaceExhausted);
        }

        Ok(encode(self.cipher.permute(id, self.space), self.length))
    }

    async fn next_id(&self) -> Result<u64, CodeError> {
        // The lock is deliberately held across the reservation: concurrent
        // callers then queue behind one block fetch instead of each opening a
        // transaction against the same counter row.
        let mut reserved = self.reserved.lock().await;

        if reserved.next >= reserved.end {
            let start = db::reserve_code_block(&self.table_client, self.block_size)
                .await
                .map_err(CodeError::Db)?;

            if start >= self.space {
                return Err(CodeError::SpaceExhausted);
            }

            reserved.next = start;
            reserved.end = start.saturating_add(self.block_size);
        }

        let id = reserved.next;
        reserved.next += 1;

        Ok(id)
    }
}

/// Size of the code space for `length` characters, or `None` when it overflows.
fn code_space(length: usize) -> Option<u64> {
    if length == 0 || length > MAX_CODE_LENGTH {
        return None;
    }

    (CODE_ALPHABET.len() as u64).checked_pow(length as u32)
}

/// Renders `value` as a fixed-width base62 string, left-padded with the first
/// alphabet character. Fixed width keeps the mapping a bijection onto every
/// string of that length.
fn encode(mut value: u64, length: usize) -> String {
    let base = CODE_ALPHABET.len() as u64;
    let mut buf = vec![CODE_ALPHABET[0] as char; length];

    for slot in buf.iter_mut().rev() {
        *slot = CODE_ALPHABET[(value % base) as usize] as char;
        value /= base;
    }

    buf.into_iter().collect()
}

/// Keyed pseudo-random permutation over `[0, 2^(2 * half_bits))`.
struct Feistel {
    key: [u8; 32],
    half_bits: u32,
    half_mask: u64,
}

impl Feistel {
    fn new(secret: &str, space: u64) -> Self {
        // Accept any secret string by folding it into a fixed-width key.
        let key: [u8; 32] = Sha256::digest(secret.as_bytes()).into();

        // Smallest even-split power-of-two domain that covers the code space.
        let needed_bits = u64::BITS - space.saturating_sub(1).leading_zeros();
        let half_bits = needed_bits.div_ceil(2).max(1);

        Feistel {
            key,
            half_bits,
            half_mask: (1u64 << half_bits) - 1,
        }
    }

    /// Maps `value` to a distinct value in `[0, space)`.
    ///
    /// Cycle-walking keeps the result in range: the network is a bijection on
    /// the power-of-two domain, so re-applying it until the output lands below
    /// `space` is itself a bijection on `[0, space)`. It terminates because
    /// every orbit is a cycle that returns to the in-range input.
    fn permute(&self, value: u64, space: u64) -> u64 {
        // An empty space has no in-range output, so the walk below would spin
        // forever. `CodeAllocator::new` rejects such a length before building a
        // Feistel, making this unreachable — guard anyway, because the failure
        // mode is a silent hang rather than an error.
        debug_assert!(space > 0, "permute called with an empty code space");
        if space == 0 {
            return 0;
        }

        let mut value = self.round_trip(value);

        while value >= space {
            value = self.round_trip(value);
        }

        value
    }

    fn round_trip(&self, value: u64) -> u64 {
        let mut left = (value >> self.half_bits) & self.half_mask;
        let mut right = value & self.half_mask;

        for round in 0..FEISTEL_ROUNDS {
            let mixed = left ^ self.f(round, right);
            left = right;
            right = mixed;
        }

        (left << self.half_bits) | right
    }

    /// Round function: a keyed hash truncated to one half-width.
    fn f(&self, round: u8, value: u64) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(self.key);
        hasher.update([round]);
        hasher.update(value.to_be_bytes());

        let digest = hasher.finalize();
        let mut head = [0u8; 8];
        head.copy_from_slice(&digest[..8]);

        u64::from_be_bytes(head) & self.half_mask
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn cipher(length: usize) -> (Feistel, u64) {
        let space = code_space(length).unwrap_or_default();
        (Feistel::new("test-secret", space), space)
    }

    #[test]
    fn encodes_fixed_width_base62() {
        assert_eq!(encode(0, 4), "AAAA");
        assert_eq!(encode(1, 4), "AAAB");
        assert_eq!(encode(61, 4), "AAA9");
        assert_eq!(encode(62, 4), "AABA");
    }

    #[test]
    fn encoded_codes_use_only_the_alphabet() {
        let (feistel, space) = cipher(8);

        for id in 0..1_000 {
            let code = encode(feistel.permute(id, space), 8);
            assert_eq!(code.chars().count(), 8);
            assert!(
                code.bytes().all(|b| CODE_ALPHABET.contains(&b)),
                "unexpected character in {code}"
            );
        }
    }

    #[test]
    fn permutation_is_a_bijection_over_the_whole_space() {
        // Exhaustive rather than sampled: at length 2 the space is only 3844
        // values, so the permutation can be proven total and duplicate-free
        // instead of merely tested at a few thousand points.
        let (feistel, space) = cipher(2);
        let outputs: HashSet<u64> = (0..space).map(|id| feistel.permute(id, space)).collect();

        assert_eq!(
            outputs.len(),
            space as usize,
            "not injective over the space"
        );
        assert!(
            outputs.iter().all(|&code| code < space),
            "an output escaped the code space"
        );
        // Injective over `space` inputs, all landing inside `space` => onto.
        // Every code of this length is reachable, and none twice.
    }

    #[test]
    fn permutation_is_injective() {
        // The whole point of the scheme: distinct counter values can never
        // produce the same code, so a duplicate is impossible rather than rare.
        let (feistel, space) = cipher(6);
        let codes: HashSet<u64> = (0..20_000).map(|id| feistel.permute(id, space)).collect();

        assert_eq!(codes.len(), 20_000);
    }

    #[test]
    fn permutation_stays_inside_the_code_space() {
        for length in [1, 2, 3, 6, 8, 10] {
            let (feistel, space) = cipher(length);

            for id in 0..500.min(space) {
                assert!(
                    feistel.permute(id, space) < space,
                    "length {length} escaped its space"
                );
            }
        }
    }

    #[test]
    fn consecutive_ids_do_not_produce_consecutive_codes() {
        // Sequential output would let anyone walk every link ever created.
        let (feistel, space) = cipher(8);
        let sequential = (0..256)
            .filter(|id| feistel.permute(*id, space) + 1 == feistel.permute(id + 1, space))
            .count();

        assert!(sequential < 4, "{sequential} sequential pairs, expected ~0");
    }

    #[test]
    fn permutation_is_stable_for_a_given_secret() {
        // Codes are stored, not recomputed, but a permutation that drifts
        // between runs would reissue values the counter has already spent.
        let (first, space) = cipher(8);
        let second = Feistel::new("test-secret", space);

        for id in [0, 1, 42, 999_999] {
            assert_eq!(first.permute(id, space), second.permute(id, space));
        }
    }

    #[test]
    fn different_secrets_give_different_permutations() {
        let (feistel, space) = cipher(8);
        let other = Feistel::new("another-secret", space);

        let differing = (0..64)
            .filter(|id| feistel.permute(*id, space) != other.permute(*id, space))
            .count();

        assert!(differing > 60, "only {differing}/64 codes differ");
    }

    #[test]
    fn code_space_rejects_unusable_lengths() {
        assert_eq!(code_space(0), None);
        assert_eq!(code_space(MAX_CODE_LENGTH + 1), None);
        assert_eq!(code_space(1), Some(62));
        assert_eq!(code_space(2), Some(3_844));
    }
}
