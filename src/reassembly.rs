//! Bounded client-side reassembly of fragmented snapshot datagrams.

use crate::protocol::{
    MAX_SNAPSHOT_FRAGMENTS, MAX_SNAPSHOT_FRAME_BYTES, ProtocolError, SnapshotDatagram,
    SnapshotFragment, SnapshotFrame, decode_snapshot_datagram, decode_snapshot_owned,
};
use std::collections::BTreeMap;

/// Incomplete snapshots kept at once; the oldest is evicted for a newer one.
pub const SNAPSHOT_REASSEMBLY_MAX_PENDING: usize = 4;
/// Chunk bytes buffered across all incomplete snapshots.
///
/// Two maximum-sized frames fit, so a newer maximum-sized snapshot can always
/// displace an older one.
pub const SNAPSHOT_REASSEMBLY_MAX_BUFFERED_BYTES: usize = 2 * MAX_SNAPSHOT_FRAME_BYTES;
/// Datagrams an incomplete snapshot may go without storing a new fragment
/// before it is dropped.
///
/// A server sends the fragments of one snapshot back to back, so this many
/// datagrams carry the fragments of several maximum-sized snapshots. An
/// incomplete snapshot that saw no progress in that window will not complete.
pub const SNAPSHOT_REASSEMBLY_MAX_IDLE_DATAGRAMS: u64 =
    (SNAPSHOT_REASSEMBLY_MAX_PENDING * MAX_SNAPSHOT_FRAGMENTS) as u64;

/// Deterministic counters describing what a [`SnapshotReassembler`] did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotReassemblyStats {
    /// Whole-snapshot datagrams delivered without reassembly.
    pub whole_snapshots: u64,
    /// Snapshots completed from fragments, verified, and delivered.
    pub reassembled_snapshots: u64,
    /// Fragments stored, including the fragment that completes a snapshot.
    pub buffered_fragments: u64,
    /// Datagrams ignored because their tick is not newer than the newest delivered snapshot.
    pub stale_datagrams: u64,
    /// Fragments ignored because the same tick and index were already buffered.
    pub duplicate_fragments: u64,
    /// Incomplete snapshots dropped to respect the pending-snapshot or byte bound,
    /// including fragments refused because every pending snapshot was newer.
    pub evicted_snapshots: u64,
    /// Incomplete snapshots dropped because a snapshot at the same or a newer tick was delivered.
    pub superseded_snapshots: u64,
    /// Incomplete snapshots dropped because no fragment was stored for them during the last
    /// [`SNAPSHOT_REASSEMBLY_MAX_IDLE_DATAGRAMS`] datagrams.
    pub expired_snapshots: u64,
    /// Datagrams rejected as malformed or inconsistent, including a completed
    /// reassembly that fails snapshot verification.
    pub rejected_datagrams: u64,
}

#[derive(Debug)]
struct PendingSnapshot {
    chunks: Vec<Option<Vec<u8>>>,
    received: usize,
    bytes: usize,
    /// Value of the reassembler's datagram count when a fragment was last stored.
    last_stored_at: u64,
}

/// Turns received snapshot datagrams back into verified [`SnapshotFrame`]s.
///
/// Feed every server-to-client datagram to [`accept`](Self::accept); it accepts
/// both whole snapshots and fragments. Delivered ticks strictly increase, so a
/// late or reordered snapshot never replaces a newer one. Use one reassembler per
/// connection.
///
/// Memory is bounded: at most [`SNAPSHOT_REASSEMBLY_MAX_PENDING`] incomplete
/// snapshots and [`SNAPSHOT_REASSEMBLY_MAX_BUFFERED_BYTES`] chunk bytes are kept.
/// When a bound is reached the oldest incomplete snapshot is dropped, so newer
/// ticks win. An incomplete snapshot that stores no fragment during
/// [`SNAPSHOT_REASSEMBLY_MAX_IDLE_DATAGRAMS`] datagrams is dropped too, so
/// abandoned snapshots at any tick cannot occupy the bounds indefinitely.
/// Losing any fragment loses only that snapshot; a later snapshot replaces it.
///
/// Fragments are identified by tick. The server sends at most one frame per
/// tick on a connection, so all fragments of one tick belong to one frame.
#[derive(Debug, Default)]
pub struct SnapshotReassembler {
    pending: BTreeMap<u64, PendingSnapshot>,
    buffered_bytes: usize,
    newest_tick: Option<u64>,
    /// Datagrams passed to `accept`; the deterministic clock for idle expiry.
    datagrams: u64,
    stats: SnapshotReassemblyStats,
}

impl SnapshotReassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Accepts one received datagram.
    ///
    /// Returns a snapshot when a whole snapshot arrived or a fragment completed
    /// one, `Ok(None)` when the datagram was buffered or ignored, and an error
    /// when it is malformed or inconsistent. Errors leave the reassembler usable.
    pub fn accept(&mut self, datagram: &[u8]) -> Result<Option<SnapshotFrame>, ProtocolError> {
        self.datagrams += 1;
        self.expire_idle();
        let result = match decode_snapshot_datagram(datagram) {
            Ok(SnapshotDatagram::Snapshot(frame)) => Ok(self.accept_whole(frame)),
            Ok(SnapshotDatagram::Fragment(fragment)) => self.accept_fragment(fragment),
            Err(error) => Err(error),
        };
        if result.is_err() {
            self.stats.rejected_datagrams += 1;
        }
        result
    }

    /// Tick of the newest snapshot delivered so far.
    pub fn newest_tick(&self) -> Option<u64> {
        self.newest_tick
    }

    /// Number of incomplete snapshots currently buffered.
    pub fn pending_snapshots(&self) -> usize {
        self.pending.len()
    }

    /// Chunk bytes currently buffered across incomplete snapshots.
    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    pub fn stats(&self) -> SnapshotReassemblyStats {
        self.stats
    }

    fn accept_whole(&mut self, frame: SnapshotFrame) -> Option<SnapshotFrame> {
        if self.is_stale(frame.tick) {
            self.stats.stale_datagrams += 1;
            return None;
        }
        self.stats.whole_snapshots += 1;
        Some(self.deliver(frame))
    }

    fn accept_fragment(
        &mut self,
        fragment: SnapshotFragment<'_>,
    ) -> Result<Option<SnapshotFrame>, ProtocolError> {
        let tick = fragment.tick;
        if self.is_stale(tick) {
            self.stats.stale_datagrams += 1;
            return Ok(None);
        }
        let index = usize::from(fragment.index);
        let count = usize::from(fragment.count);
        let chunk_len = fragment.chunk.len();

        if let Some(pending) = self.pending.get(&tick) {
            if pending.chunks.len() != count {
                return Err(ProtocolError::InconsistentFragment { tick });
            }
            if pending.chunks[index].is_some() {
                self.stats.duplicate_fragments += 1;
                return Ok(None);
            }
            let frame_bytes = pending.bytes + chunk_len;
            if frame_bytes > MAX_SNAPSHOT_FRAME_BYTES {
                // No valid snapshot frame is this large; the tick cannot complete.
                self.remove_pending(tick);
                return Err(ProtocolError::PayloadTooLarge {
                    maximum: MAX_SNAPSHOT_FRAME_BYTES,
                    actual: frame_bytes,
                });
            }
        } else if self.pending.len() >= SNAPSHOT_REASSEMBLY_MAX_PENDING
            && !self.evict_oldest_before(tick)
        {
            self.stats.evicted_snapshots += 1;
            return Ok(None);
        }

        while self.buffered_bytes + chunk_len > SNAPSHOT_REASSEMBLY_MAX_BUFFERED_BYTES {
            if !self.evict_oldest_before(tick) {
                // Every other buffered snapshot is newer: drop this one instead.
                self.remove_pending(tick);
                self.stats.evicted_snapshots += 1;
                return Ok(None);
            }
        }

        let pending = self.pending.entry(tick).or_insert_with(|| PendingSnapshot {
            chunks: vec![None; count],
            received: 0,
            bytes: 0,
            last_stored_at: 0,
        });
        pending.chunks[index] = Some(fragment.chunk.to_vec());
        pending.received += 1;
        pending.bytes += chunk_len;
        pending.last_stored_at = self.datagrams;
        self.buffered_bytes += chunk_len;
        self.stats.buffered_fragments += 1;
        if pending.received < count {
            return Ok(None);
        }

        let Some(complete) = self.remove_pending(tick) else {
            return Ok(None);
        };
        let mut frame_bytes = Vec::with_capacity(complete.bytes);
        for chunk in complete.chunks.into_iter().flatten() {
            frame_bytes.extend_from_slice(&chunk);
        }
        let frame = decode_snapshot_owned(frame_bytes)?;
        if frame.tick != tick {
            return Err(ProtocolError::InconsistentFragment { tick });
        }
        self.stats.reassembled_snapshots += 1;
        Ok(Some(self.deliver(frame)))
    }

    fn is_stale(&self, tick: u64) -> bool {
        self.newest_tick.is_some_and(|newest| tick <= newest)
    }

    fn deliver(&mut self, frame: SnapshotFrame) -> SnapshotFrame {
        let mut newer = self.pending.split_off(&frame.tick);
        let same_tick = newer.remove(&frame.tick);
        let older = std::mem::replace(&mut self.pending, newer);
        for superseded in older.into_values().chain(same_tick) {
            self.buffered_bytes -= superseded.bytes;
            self.stats.superseded_snapshots += 1;
        }
        self.newest_tick = Some(frame.tick);
        frame
    }

    /// Drops incomplete snapshots that stored no fragment during the idle bound.
    fn expire_idle(&mut self) {
        let now = self.datagrams;
        let mut expired_bytes = 0;
        let mut expired = 0;
        self.pending.retain(|_, pending| {
            let idle = now - pending.last_stored_at > SNAPSHOT_REASSEMBLY_MAX_IDLE_DATAGRAMS;
            if idle {
                expired_bytes += pending.bytes;
                expired += 1;
            }
            !idle
        });
        self.buffered_bytes -= expired_bytes;
        self.stats.expired_snapshots += expired;
    }

    /// Evicts the oldest pending snapshot if it is older than `tick`.
    fn evict_oldest_before(&mut self, tick: u64) -> bool {
        match self.pending.first_key_value() {
            Some((&oldest, _)) if oldest < tick => {
                self.remove_pending(oldest);
                self.stats.evicted_snapshots += 1;
                true
            }
            _ => false,
        }
    }

    fn remove_pending(&mut self, tick: u64) -> Option<PendingSnapshot> {
        let removed = self.pending.remove(&tick)?;
        self.buffered_bytes -= removed.bytes;
        Some(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        PROTOCOL_VERSION, SNAPSHOT_FRAGMENT_KIND, decode_snapshot, encode_snapshot,
        encode_snapshot_fragments, snapshot_hash,
    };

    fn frame(tick: u64, payload_len: usize) -> Vec<u8> {
        let payload: Vec<u8> = (0..payload_len)
            .map(|index| (index as u64).wrapping_mul(31).wrapping_add(tick) as u8)
            .collect();
        encode_snapshot(&SnapshotFrame {
            tick,
            state_hash: snapshot_hash(tick, &payload),
            payload,
        })
        .unwrap()
    }

    fn fragments(tick: u64, payload_len: usize, budget: usize) -> Vec<Vec<u8>> {
        encode_snapshot_fragments(&frame(tick, payload_len), budget).unwrap()
    }

    fn raw_fragment(tick: u64, index: u8, count: u8, chunk: &[u8]) -> Vec<u8> {
        let mut bytes = vec![PROTOCOL_VERSION, SNAPSHOT_FRAGMENT_KIND];
        bytes.extend_from_slice(&tick.to_be_bytes());
        bytes.push(index);
        bytes.push(count);
        bytes.extend_from_slice(&u16::try_from(chunk.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(chunk);
        bytes
    }

    fn assert_bounded(reassembler: &SnapshotReassembler) {
        assert!(reassembler.pending_snapshots() <= SNAPSHOT_REASSEMBLY_MAX_PENDING);
        assert!(reassembler.buffered_bytes() <= SNAPSHOT_REASSEMBLY_MAX_BUFFERED_BYTES);
        assert_eq!(
            reassembler.buffered_bytes(),
            reassembler
                .pending
                .values()
                .map(|pending| pending.bytes)
                .sum::<usize>()
        );
    }

    #[test]
    fn whole_snapshots_are_delivered_in_strictly_increasing_tick_order() {
        let mut reassembler = SnapshotReassembler::new();

        assert_eq!(
            reassembler.accept(&frame(1, 8)).unwrap(),
            Some(decode_snapshot(&frame(1, 8)).unwrap())
        );
        assert_eq!(reassembler.accept(&frame(1, 8)).unwrap(), None);
        assert_eq!(reassembler.accept(&frame(3, 8)).unwrap().unwrap().tick, 3);
        assert_eq!(reassembler.accept(&frame(2, 8)).unwrap(), None);

        assert_eq!(reassembler.newest_tick(), Some(3));
        assert_eq!(reassembler.stats().whole_snapshots, 2);
        assert_eq!(reassembler.stats().stale_datagrams, 2);
    }

    #[test]
    fn in_order_fragments_reassemble_the_exact_snapshot() {
        let original = frame(7, 5_000);
        let datagrams = encode_snapshot_fragments(&original, 1_200).unwrap();
        let mut reassembler = SnapshotReassembler::new();

        for datagram in &datagrams[..datagrams.len() - 1] {
            assert_eq!(reassembler.accept(datagram).unwrap(), None);
            assert_bounded(&reassembler);
        }
        assert_eq!(reassembler.pending_snapshots(), 1);
        let delivered = reassembler
            .accept(datagrams.last().unwrap())
            .unwrap()
            .unwrap();

        assert_eq!(delivered, decode_snapshot(&original).unwrap());
        assert_eq!(reassembler.pending_snapshots(), 0);
        assert_eq!(reassembler.buffered_bytes(), 0);
        assert_eq!(
            reassembler.stats(),
            SnapshotReassemblyStats {
                reassembled_snapshots: 1,
                buffered_fragments: datagrams.len() as u64,
                ..SnapshotReassemblyStats::default()
            }
        );
    }

    #[test]
    fn reordered_and_duplicated_fragments_reassemble_once() {
        let original = frame(9, 4_000);
        let datagrams = encode_snapshot_fragments(&original, 900).unwrap();
        assert!(datagrams.len() >= 4);
        let mut reassembler = SnapshotReassembler::new();

        let mut order: Vec<usize> = (0..datagrams.len()).rev().collect();
        order.insert(1, order[0]);
        let last = order.pop().unwrap();
        for index in order {
            assert_eq!(reassembler.accept(&datagrams[index]).unwrap(), None);
        }
        let delivered = reassembler.accept(&datagrams[last]).unwrap().unwrap();
        assert_eq!(delivered, decode_snapshot(&original).unwrap());
        assert_eq!(reassembler.accept(&datagrams[0]).unwrap(), None);

        let stats = reassembler.stats();
        assert_eq!(stats.reassembled_snapshots, 1);
        assert_eq!(stats.duplicate_fragments, 1);
        assert_eq!(stats.stale_datagrams, 1);
    }

    #[test]
    fn a_lost_fragment_is_superseded_by_a_newer_snapshot() {
        let lossy = fragments(4, 3_000, 1_000);
        let mut reassembler = SnapshotReassembler::new();
        for datagram in &lossy[1..] {
            assert_eq!(reassembler.accept(datagram).unwrap(), None);
        }
        assert_eq!(reassembler.pending_snapshots(), 1);

        for datagram in fragments(5, 3_000, 1_000) {
            if let Some(snapshot) = reassembler.accept(&datagram).unwrap() {
                assert_eq!(snapshot.tick, 5);
            }
        }
        assert_eq!(reassembler.newest_tick(), Some(5));
        assert_eq!(reassembler.pending_snapshots(), 0);
        assert_eq!(reassembler.buffered_bytes(), 0);
        assert_eq!(reassembler.accept(&lossy[0]).unwrap(), None);

        let stats = reassembler.stats();
        assert_eq!(stats.superseded_snapshots, 1);
        assert_eq!(stats.stale_datagrams, 1);
        assert_eq!(stats.reassembled_snapshots, 1);
    }

    #[test]
    fn a_whole_snapshot_supersedes_pending_fragments_of_older_and_equal_ticks() {
        let mut reassembler = SnapshotReassembler::new();
        reassembler.accept(&fragments(1, 3_000, 1_000)[0]).unwrap();
        reassembler.accept(&fragments(2, 3_000, 1_000)[0]).unwrap();
        reassembler.accept(&fragments(3, 3_000, 1_000)[0]).unwrap();

        assert_eq!(reassembler.accept(&frame(2, 8)).unwrap().unwrap().tick, 2);

        assert_eq!(reassembler.pending_snapshots(), 1);
        assert_eq!(reassembler.stats().superseded_snapshots, 2);
        assert_bounded(&reassembler);
    }

    #[test]
    fn pending_snapshot_bound_keeps_the_newest_ticks() {
        let mut reassembler = SnapshotReassembler::new();
        let first_fragments: Vec<_> = (1..=6).map(|tick| fragments(tick, 3_000, 1_000)).collect();
        for tick_fragments in &first_fragments[1..5] {
            reassembler.accept(&tick_fragments[0]).unwrap();
        }
        assert_eq!(
            reassembler.pending_snapshots(),
            SNAPSHOT_REASSEMBLY_MAX_PENDING
        );

        // Tick 6 evicts tick 2, the oldest pending snapshot.
        reassembler.accept(&first_fragments[5][0]).unwrap();
        assert_eq!(
            reassembler.pending.keys().copied().collect::<Vec<_>>(),
            [3, 4, 5, 6]
        );
        // Tick 1 is older than every pending snapshot, so it is refused.
        assert_eq!(reassembler.accept(&first_fragments[0][0]).unwrap(), None);
        assert_eq!(
            reassembler.pending.keys().copied().collect::<Vec<_>>(),
            [3, 4, 5, 6]
        );
        assert_eq!(reassembler.stats().evicted_snapshots, 2);
        assert_bounded(&reassembler);

        // Completing the newest tick supersedes every older pending snapshot.
        let mut delivered = None;
        for datagram in &first_fragments[5][1..] {
            delivered = reassembler.accept(datagram).unwrap().or(delivered);
        }
        assert_eq!(delivered.unwrap().tick, 6);
        assert_eq!(reassembler.pending_snapshots(), 0);
        assert_eq!(reassembler.buffered_bytes(), 0);
        assert_eq!(reassembler.stats().superseded_snapshots, 3);
    }

    #[test]
    fn buffered_byte_bound_evicts_older_snapshots_first() {
        let budget = 60_000;
        let big: Vec<_> = (1..=3)
            .map(|tick| fragments(tick, crate::MAX_SNAPSHOT_PAYLOAD_BYTES, budget))
            .collect();
        assert!(big.iter().all(|datagrams| datagrams.len() == 2));
        let mut reassembler = SnapshotReassembler::new();

        reassembler.accept(&big[1][0]).unwrap();
        reassembler.accept(&big[2][0]).unwrap();
        assert_bounded(&reassembler);
        // A third large chunk does not fit alongside two others; tick 1 is oldest and refused.
        assert_eq!(reassembler.accept(&big[0][0]).unwrap(), None);
        assert_eq!(
            reassembler.pending.keys().copied().collect::<Vec<_>>(),
            [2, 3]
        );
        assert_bounded(&reassembler);

        // The newer snapshots were kept and still complete.
        assert_eq!(reassembler.accept(&big[1][1]).unwrap().unwrap().tick, 2);
        assert_eq!(reassembler.accept(&big[2][1]).unwrap().unwrap().tick, 3);
        assert_eq!(reassembler.buffered_bytes(), 0);
        assert_eq!(reassembler.stats().evicted_snapshots, 1);
    }

    #[test]
    fn buffered_byte_bound_evicts_an_older_snapshot_for_a_newer_one() {
        let budget = 60_000;
        let big: Vec<_> = (1..=3)
            .map(|tick| fragments(tick, crate::MAX_SNAPSHOT_PAYLOAD_BYTES, budget))
            .collect();
        let mut reassembler = SnapshotReassembler::new();

        reassembler.accept(&big[0][0]).unwrap();
        reassembler.accept(&big[1][0]).unwrap();
        reassembler.accept(&big[2][0]).unwrap();

        assert_eq!(
            reassembler.pending.keys().copied().collect::<Vec<_>>(),
            [2, 3]
        );
        assert_eq!(reassembler.stats().evicted_snapshots, 1);
        assert_bounded(&reassembler);
    }

    #[test]
    fn inconsistent_fragment_counts_are_rejected_without_losing_progress() {
        let original = frame(5, 30);
        let datagrams = encode_snapshot_fragments(&original, 30).unwrap();
        assert_eq!(datagrams.len(), 4);
        let mut reassembler = SnapshotReassembler::new();
        reassembler.accept(&datagrams[0]).unwrap();

        assert_eq!(
            reassembler.accept(&raw_fragment(5, 1, 3, b"conflict")),
            Err(ProtocolError::InconsistentFragment { tick: 5 })
        );
        let mut delivered = None;
        for datagram in &datagrams[1..] {
            delivered = reassembler.accept(datagram).unwrap();
        }
        assert_eq!(delivered.unwrap(), decode_snapshot(&original).unwrap());
        assert_eq!(reassembler.stats().rejected_datagrams, 1);
    }

    #[test]
    fn malicious_datagrams_are_rejected_without_state_changes() {
        let mut reassembler = SnapshotReassembler::new();
        reassembler.accept(&fragments(2, 3_000, 1_000)[0]).unwrap();
        let before = (
            reassembler.pending_snapshots(),
            reassembler.buffered_bytes(),
        );

        let mut wrong_version = raw_fragment(3, 0, 2, b"chunk");
        wrong_version[0] = PROTOCOL_VERSION + 1;
        let mut unknown_kind = raw_fragment(3, 0, 2, b"chunk");
        unknown_kind[1] = 0xff;
        let mut declared_too_long = raw_fragment(3, 0, 2, b"chunk");
        declared_too_long[12..14].copy_from_slice(&u16::MAX.to_be_bytes());
        let malicious = [
            Vec::new(),
            vec![PROTOCOL_VERSION],
            wrong_version,
            unknown_kind,
            declared_too_long,
            raw_fragment(3, 0, 0, b"chunk"),
            raw_fragment(3, 0, u8::MAX, b"chunk"),
            raw_fragment(3, 7, 2, b"chunk"),
            raw_fragment(3, 0, 2, b""),
            raw_fragment(u64::MAX, u8::MAX, u8::MAX, b"chunk"),
        ];
        for datagram in &malicious {
            assert!(reassembler.accept(datagram).is_err(), "{datagram:?}");
        }

        assert_eq!(
            (
                reassembler.pending_snapshots(),
                reassembler.buffered_bytes()
            ),
            before
        );
        assert_eq!(
            reassembler.stats().rejected_datagrams,
            malicious.len() as u64
        );
    }

    #[test]
    fn abandoned_far_future_fragments_do_not_block_later_snapshots() {
        let mut reassembler = SnapshotReassembler::new();
        for offset in 0..SNAPSHOT_REASSEMBLY_MAX_PENDING as u64 {
            assert_eq!(
                reassembler
                    .accept(&raw_fragment(u64::MAX - offset, 0, 2, &[0xaa]))
                    .unwrap(),
                None
            );
        }
        assert_eq!(
            reassembler.pending_snapshots(),
            SNAPSHOT_REASSEMBLY_MAX_PENDING
        );

        let mut delivered = Vec::new();
        for tick in 1..=100 {
            for datagram in fragments(tick, 3_000, 1_100) {
                if let Some(snapshot) = reassembler.accept(&datagram).unwrap() {
                    delivered.push(snapshot.tick);
                }
                assert_bounded(&reassembler);
            }
        }

        // The far-future snapshots never complete, so they expire after the
        // idle bound and newer legitimate snapshots are delivered again.
        assert_eq!(
            reassembler.stats().expired_snapshots,
            SNAPSHOT_REASSEMBLY_MAX_PENDING as u64
        );
        assert_eq!(reassembler.newest_tick(), Some(100));
        assert_eq!(reassembler.pending_snapshots(), 0);
        let blocked_ticks = SNAPSHOT_REASSEMBLY_MAX_IDLE_DATAGRAMS / 3 + 2;
        assert!(
            delivered.len() as u64 >= 100 - blocked_ticks,
            "delivered {delivered:?}"
        );
    }

    #[test]
    fn an_incomplete_snapshot_expires_after_the_idle_datagram_bound() {
        let datagrams = fragments(5, 3_000, 1_100);
        assert_eq!(datagrams.len(), 3);
        let mut reassembler = SnapshotReassembler::new();
        reassembler.accept(&datagrams[0]).unwrap();

        // Unrelated datagrams advance the idle count without touching tick 5.
        for _ in 0..SNAPSHOT_REASSEMBLY_MAX_IDLE_DATAGRAMS - 1 {
            assert!(reassembler.accept(&[]).is_err());
        }
        // A stored fragment resets the idle count.
        assert_eq!(reassembler.accept(&datagrams[1]).unwrap(), None);
        for _ in 0..SNAPSHOT_REASSEMBLY_MAX_IDLE_DATAGRAMS {
            assert!(reassembler.accept(&[]).is_err());
        }
        assert_eq!(reassembler.pending_snapshots(), 1);
        assert_eq!(reassembler.stats().expired_snapshots, 0);

        assert!(reassembler.accept(&[]).is_err());
        assert_eq!(reassembler.pending_snapshots(), 0);
        assert_eq!(reassembler.buffered_bytes(), 0);
        assert_eq!(reassembler.stats().expired_snapshots, 1);
        // The last fragment alone now starts a new incomplete snapshot.
        assert_eq!(reassembler.accept(&datagrams[2]).unwrap(), None);
        assert_eq!(reassembler.pending_snapshots(), 1);
    }

    #[test]
    fn forged_chunks_fail_snapshot_verification_after_reassembly() {
        let mut datagrams = fragments(6, 2_000, 700);
        let last = datagrams[1].len() - 1;
        datagrams[1][last] ^= 0xff;
        let mut reassembler = SnapshotReassembler::new();

        let results: Vec<_> = datagrams
            .iter()
            .map(|datagram| reassembler.accept(datagram))
            .collect();
        assert!(matches!(
            results.last().unwrap(),
            Err(ProtocolError::InvalidStateHash { .. })
        ));
        assert_eq!(reassembler.pending_snapshots(), 0);
        assert_eq!(reassembler.buffered_bytes(), 0);
        assert_eq!(reassembler.newest_tick(), None);

        let mut delivered = None;
        for datagram in fragments(6, 2_000, 700) {
            delivered = reassembler.accept(&datagram).unwrap().or(delivered);
        }
        assert_eq!(delivered.unwrap().tick, 6);
    }

    #[test]
    fn fragment_tick_must_match_the_reassembled_snapshot_tick() {
        let mut datagrams = fragments(6, 2_000, 700);
        for datagram in &mut datagrams {
            datagram[2..10].copy_from_slice(&7_u64.to_be_bytes());
        }
        let mut reassembler = SnapshotReassembler::new();

        let results: Vec<_> = datagrams
            .iter()
            .map(|datagram| reassembler.accept(datagram))
            .collect();
        assert_eq!(
            results.last().unwrap(),
            &Err(ProtocolError::InconsistentFragment { tick: 7 })
        );
        assert_eq!(reassembler.newest_tick(), None);
    }

    #[test]
    fn oversized_fragment_sets_are_dropped_before_reassembly() {
        let chunk = vec![0; usize::from(u16::MAX)];
        let mut reassembler = SnapshotReassembler::new();

        assert_eq!(
            reassembler.accept(&raw_fragment(1, 0, 2, &chunk)).unwrap(),
            None
        );
        assert_eq!(
            reassembler.accept(&raw_fragment(1, 1, 2, &chunk)),
            Err(ProtocolError::PayloadTooLarge {
                maximum: MAX_SNAPSHOT_FRAME_BYTES,
                actual: 2 * chunk.len(),
            })
        );
        assert_eq!(reassembler.pending_snapshots(), 0);
        assert_eq!(reassembler.buffered_bytes(), 0);
    }

    #[test]
    fn a_single_fragment_snapshot_completes_immediately() {
        let original = frame(8, 12);
        let datagrams = encode_snapshot_fragments(&original, 1_200).unwrap();
        assert_eq!(datagrams.len(), 1);

        let delivered = SnapshotReassembler::new().accept(&datagrams[0]).unwrap();
        assert_eq!(delivered.unwrap(), decode_snapshot(&original).unwrap());
    }

    #[test]
    fn deterministic_lossy_reordered_stream_stays_bounded_and_monotonic() {
        let originals: Vec<_> = (1..=40_u64)
            .map(|tick| frame(tick, 500 + (tick as usize * 137) % 4_000))
            .collect();
        let mut stream: Vec<Vec<u8>> = Vec::new();
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for original in &originals {
            for datagram in encode_snapshot_fragments(original, 600).unwrap() {
                match next() % 10 {
                    0 => {}
                    1 => {
                        stream.push(datagram.clone());
                        stream.push(datagram);
                    }
                    _ => stream.push(datagram),
                }
            }
        }
        for index in (1..stream.len()).rev() {
            if next() % 4 == 0 {
                let swap_with = index.saturating_sub(1 + (next() % 6) as usize);
                stream.swap(index, swap_with);
            }
        }

        let mut reassembler = SnapshotReassembler::new();
        let mut last_tick = 0;
        let mut delivered = 0;
        for datagram in &stream {
            if let Some(snapshot) = reassembler.accept(datagram).unwrap() {
                assert!(snapshot.tick > last_tick);
                assert_eq!(
                    snapshot,
                    decode_snapshot(&originals[snapshot.tick as usize - 1]).unwrap()
                );
                last_tick = snapshot.tick;
                delivered += 1;
            }
            assert_bounded(&reassembler);
        }
        assert!(delivered > 0);
        assert_eq!(reassembler.stats().reassembled_snapshots, delivered);
    }
}
