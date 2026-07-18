//! phase_buffer.rs
//! Peer-Frame-Sammlung fuer eine einzelne Vote-Phase.
//!
//! Generisch in `N` (Peer-Anzahl). Nutzt `heapless::Vec<_, N>` fuer
//! den No-Alloc-Hot-Path.

use heapless::Vec;

use crate::net::udp_frame::UdpFrame;
use crate::sys_state::state_loop_iface::{
    Clock, CollectOutcome, LoopConfig, PeerMask, Transport,
};

// ============================================================
// PhaseBuffer
// ============================================================

/// Sammelt Peer-Frames innerhalb einer Vote-Phase. Kennt nur die
/// erwarteten Slots und die bereits gesehenen Frames; Zeitsteuerung
/// liegt beim Aufrufer (`collect_peer_frames`).
pub(crate) struct PhaseBuffer<'a, const N: usize> {
    cfg:      &'a LoopConfig<N>,
    /// Vor der Phase bereits als fehlend gefuehrte Peers (Slot-Bits).
    /// Diese werden nicht mehr erwartet.
    excluded: PeerMask,
    /// Welche Slots haben in dieser Phase bereits geliefert.
    seen:     PeerMask,
    frames:   Vec<UdpFrame, N>,
}

impl<'a, const N: usize> PhaseBuffer<'a, N> {
    pub fn new(cfg: &'a LoopConfig<N>, excluded: PeerMask) -> Self {
        Self { cfg, excluded, seen: PeerMask::EMPTY, frames: Vec::new() }
    }

    /// Anzahl in dieser Phase erwarteter Peer-Frames.
    #[inline] pub fn expected_count(&self) -> u32 {
        (N as u32).saturating_sub(self.excluded.count())
    }

    /// Nimmt ein Frame entgegen. Rueckgabe: Slot, falls das Frame
    /// erstmals von einem erwarteten Peer kam; sonst None (unbekannte
    /// ID, ausgeschlossener Peer, oder Duplikat).
    pub fn accept(&mut self, f: UdpFrame) -> AcceptOutcome {
        let id = f.node_id();
        let Some(slot) = self.cfg.slot_of(id) else {
            return AcceptOutcome::Foreign;
        };
        if self.excluded.contains(slot) {
            return AcceptOutcome::Excluded { slot };
        }
        if self.seen.contains(slot) {
            return AcceptOutcome::Duplicate;
        }
        // heapless::Vec::push kann nur bei voller Kapazitaet fehlen.
        // Durch die vorherigen Filter ist self.frames.len() < N.
        let _ = self.frames.push(f);
        self.seen.set(slot);
        AcceptOutcome::Accepted { slot }
    }

    /// Alle erwarteten Peers haben geliefert.
    #[inline] pub fn complete(&self) -> bool {
        self.seen.count() == self.expected_count()
    }

    /// Quorum inklusive dieses Knotens erreicht.
    #[inline] pub fn has_quorum(&self) -> bool {
        self.seen.count() + 1 >= self.cfg.quorum as u32
    }

    /// Slots, die noch fehlen (nur unter den erwarteten).
    pub fn missing(&self) -> PeerMask {
        let mut m = PeerMask::EMPTY;
        for slot in 0..N {
            if !self.excluded.contains(slot) && !self.seen.contains(slot) {
                m.set(slot);
            }
        }
        m
    }

    /// Verbraucht den Buffer und gibt die Frame-Sammlung zurueck.
    pub fn into_frames(self) -> Vec<UdpFrame, N> { self.frames }
}

/// Ergebnis eines `accept`-Aufrufs.
pub(crate) enum AcceptOutcome {
    Accepted { slot: usize },
    /// Peer, der zuvor als fehlend gefuehrt wurde -> Rejoin-Signal.
    Excluded { slot: usize },
    Duplicate,
    Foreign,
}

// ============================================================
// collect_peer_frames
// ============================================================

/// Sammelt Peer-Frames bis Quorum oder Timeout.
///
/// Poll-basiert: `try_recv` in Schritten von `poll_tick`, harte
/// Deadline `now + peer_timeout`.
pub(crate) fn collect_peer_frames<C, T, const N: usize>(
    cfg:      &LoopConfig<N>,
    clock:    &C,
    transport: &mut T,
    excluded:  PeerMask,
) -> CollectOutcome<N>
where
    C: Clock,
    T: Transport,
{
    let deadline = clock.now() + cfg.peer_timeout;
    let mut buf  = PhaseBuffer::<N>::new(cfg, excluded);

    // Rejoin-Detektion: nur der ERSTE Rueckkehrer wird gemeldet. Weitere
    // (unwahrscheinlich innerhalb einer Phase) fallen als Foreign durch,
    // bis die FSM den Zustand aktualisiert hat.
    let mut returned: Option<usize> = None;

    while clock.now() < deadline {
        while let Some(f) = transport.try_recv() {
            match buf.accept(f) {
                AcceptOutcome::Accepted { .. } => {}
                AcceptOutcome::Excluded { slot } if returned.is_none() => {
                    returned = Some(slot);
                }
                _ => {} // Duplicate / Foreign / weitere Excluded ignorieren
            }
        }
        if buf.complete() { break; }
        clock.sleep_until(clock.now() + cfg.poll_tick);
    }

    // Prioritaet: Rejoin > Complete > Quorum > QuorumLost.
    // Rejoin-Signal muss die FSM sehen, auch wenn Quorum sonst reicht,
    // damit der Uebergang Degraded -> Operational sauber getriggert wird.
    if let Some(slot) = returned {
        return CollectOutcome::PeerReturned { slot };
    }
    if buf.complete() {
        return CollectOutcome::Complete(buf.into_frames());
    }
    if buf.has_quorum() {
        let missing = buf.missing();
        return CollectOutcome::Quorum { frames: buf.into_frames(), missing };
    }
    CollectOutcome::QuorumLost { new_missing: buf.missing() }
}

// ============================================================
// Tests — Buffer-Verhalten ohne Clock/Transport
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use core::time::Duration;

    fn cfg2oo3() -> LoopConfig<2> {
        LoopConfig {
            node_id: 1,
            peers:   [2, 3],
            quorum:  2,
            cycle_period: Duration::from_millis(50),
            peer_timeout: Duration::from_millis(20),
            sync_timeout: Duration::from_millis(200),
            poll_tick:    Duration::from_millis(1),
            rejoining:    false,
        }
    }

    // Hier wuerdest du weitere Tests fuer accept/complete/has_quorum
    // schreiben, sobald ein Test-Konstruktor fuer UdpFrame existiert.
    // Die Signatur `UdpFrame::for_test(node_id)` waere ausreichend.

    #[test]
    fn expected_count_full() {
        let cfg = cfg2oo3();
        let buf = PhaseBuffer::<2>::new(&cfg, PeerMask::EMPTY);
        assert_eq!(buf.expected_count(), 2);
    }

    #[test]
    fn expected_count_with_excluded() {
        let cfg = cfg2oo3();
        let mut ex = PeerMask::EMPTY;
        ex.set(0);
        let buf = PhaseBuffer::<2>::new(&cfg, ex);
        assert_eq!(buf.expected_count(), 1);
    }
}