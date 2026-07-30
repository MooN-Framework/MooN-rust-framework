//! Peer-Zeitsynchronisation nach Cristian mit symmetrischer,
//! Mehr-Peer-Aggregation.
//!
//! Jeder Knoten schaetzt fuer jeden Peer den Uhrenoffset relativ zur eigenen
//! monotonen Uhr und eine bewiesene obere Fehlerschranke aus der kleinsten
//! beobachteten Round-Trip-Zeit.
//!
//! Protokoll pro Runde:
//!   1. Requester sendet TimeSyncReq { t1 } mit t1 = lokale Zeit (ns).
//!   2. Responder empfaengt bei t2 (eigene Uhr), sendet
//!      TimeSyncResp { t1, t2, t3 } mit t3 = lokale Zeit beim Senden.
//!   3. Requester empfaengt bei t4 (eigene Uhr) und berechnet:
//!        delay  = (t4 - t1) - (t3 - t2)
//!        offset = ((t2 - t1) + (t3 - t4)) / 2     // peer_clock - self_clock
//!        error_bound = (delay - 2 * DT_MIN_NS) / 2
//!
//! Die Probe mit kleinstem delay pro Peer wird behalten (NTP-Filterung).
//!
//! Konvergenz ueber alle Peers via Median, damit ein beliebig fehlerhafter
//! Peer die Gruppenzeit nicht verschieben kann.

use crate::framework::traits::CyclePayload;
use crate::framework::udp_frame::{Payload, UdpFrame};

/// Minimale physisch moegliche Netzwerk-Uebertragungszeit in ns.
///
/// Konservative Auslegung fuer Gigabit-Ethernet mit einem Switch-Hop:
/// Frame-Serialisierung + Wire-Propagation + Switch-Store-and-Forward.
/// Muss fuer die Zielhardware messtechnisch validiert werden.
pub const DT_MIN_NS: u64 = 20_000;

/// Anzahl Sync-Runden pro Peer. Aus allen Proben wird die mit dem kleinsten
/// Delay behalten (NTP-Filterung: kleiner Delay => symmetrischste Latenz).
pub const SAMPLES_PER_PEER: usize = 8;

/// Eine einzelne Cristian-Messung mit allen vier Zeitstempeln.
#[derive(Debug, Clone, Copy)]
pub struct SyncSample {
    /// Requester-Sendezeit (lokale Uhr).
    pub t1: u64,
    /// Responder-Empfangszeit (Peer-Uhr).
    pub t2: u64,
    /// Responder-Sendezeit (Peer-Uhr).
    pub t3: u64,
    /// Requester-Empfangszeit (lokale Uhr).
    pub t4: u64,
}

impl SyncSample {
    /// Netz-Round-Trip-Zeit in ns, ohne Responder-Verarbeitungszeit.
    pub fn delay(&self) -> i64 {
        (self.t4 as i64 - self.t1 as i64) - (self.t3 as i64 - self.t2 as i64)
    }

    /// Geschaetzter Uhrenoffset (peer_clock - self_clock) in ns.
    pub fn offset(&self) -> i64 {
        ((self.t2 as i64 - self.t1 as i64) + (self.t3 as i64 - self.t4 as i64)) / 2
    }

    /// Bewiesene obere Schranke fuer den Betrag des Offset-Fehlers in ns.
    ///
    /// Herleitung: Bei asymmetrischer Latenz (alpha + beta = delay - 2*dt_min)
    /// weicht die Offset-Schaetzung maximal um (delay - 2*dt_min) / 2 vom
    /// wahren Wert ab. Bei symmetrischer Latenz ist der Fehler 0.
    pub fn error_bound(&self) -> i64 {
        let d = self.delay();
        let raw = (d - 2 * DT_MIN_NS as i64) / 2;
        if raw < 0 { 0 } else { raw }
    }
}

/// Aggregiertes Ergebnis fuer einen Peer nach abgeschlossener Synchronisation.
#[derive(Debug, Clone, Copy)]
pub struct PeerClock {
    pub peer_id: u8,
    /// peer_clock - self_clock in ns.
    pub offset_ns: i64,
    /// Bewiesene Fehlerschranke fuer offset_ns.
    pub error_bound_ns: i64,
    /// Anzahl der insgesamt genommenen Proben.
    pub samples_used: u32,
}

#[derive(Debug)]
struct PeerSyncState {
    peer_id: u8,
    /// t1 des zuletzt gesendeten, noch nicht beantworteten Requests.
    /// Wird als Korrelationsschluessel benutzt, um verspaetete Duplikate
    /// oder Responses fuer andere Requester zu verwerfen.
    pending_t1: Option<u64>,
    samples_taken: u32,
    /// Beste bisher gesehene Probe (kleinster Delay).
    best: Option<SyncSample>,
}

impl PeerSyncState {
    fn new(peer_id: u8) -> Self {
        Self {
            peer_id,
            pending_t1: None,
            samples_taken: 0,
            best: None,
        }
    }

    fn record_sample(&mut self, sample: SyncSample) {
        self.samples_taken = self.samples_taken.saturating_add(1);
        let keep = match self.best {
            None => true,
            Some(prev) => sample.delay() < prev.delay(),
        };
        if keep {
            self.best = Some(sample);
        }
        self.pending_t1 = None;
    }

    fn is_complete(&self) -> bool {
        self.samples_taken >= SAMPLES_PER_PEER as u32
    }

    fn as_peer_clock(&self) -> Option<PeerClock> {
        self.best.map(|s| PeerClock {
            peer_id: self.peer_id,
            offset_ns: s.offset(),
            error_bound_ns: s.error_bound(),
            samples_used: self.samples_taken,
        })
    }
}

/// Koordinator fuer die Zeitsynchronisation mit allen bekannten Peers.
///
/// Wird waehrend der PeerSync-Phase der Lifecycle-State-Machine verwendet
/// und liefert am Ende die Uhrenoffsets, die im weiteren Betrieb angewendet
/// werden.
pub struct PeerSync {
    peers: Vec<PeerSyncState>,
}

impl PeerSync {
    pub fn new(peer_ids: &[u8]) -> Self {
        Self {
            peers: peer_ids.iter().map(|&id| PeerSyncState::new(id)).collect(),
        }
    }

    /// Vom Sender aufzurufen, direkt nachdem `send_time_sync_req` das
    /// Tupel (seq, t1) geliefert hat. Vermerkt t1 als offenen Request.
    pub fn record_outgoing_request(&mut self, peer_id: u8, t1: u64) {
        if let Some(state) = self.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            state.pending_t1 = Some(t1);
        }
    }

    /// True, wenn fuer diesen Peer aktuell ein Request offen ist.
    /// Nutzt der Koordinator, um nicht zwei Requests gleichzeitig
    /// an denselben Peer zu senden.
    pub fn has_pending(&self, peer_id: u8) -> bool {
        self.peers
            .iter()
            .find(|p| p.peer_id == peer_id)
            .and_then(|p| p.pending_t1)
            .is_some()
    }

    /// Vom Koordinator aufzurufen, wenn eine Response-Timeout erreicht wurde.
    /// Loescht den offenen Request, damit ein neuer gesendet werden kann.
    pub fn mark_timeout(&mut self, peer_id: u8) {
        if let Some(state) = self.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            state.pending_t1 = None;
        }
    }

    /// Verarbeitet eine eingegangene Response. Verwirft:
    /// - Antworten von unbekannten Peers,
    /// - Antworten ohne passenden pending_t1 (nicht fuer uns bestimmt oder verspaetet),
    /// - Proben mit negativem oder unplausiblem Delay.
    pub fn on_response(&mut self, peer_id: u8, t1: u64, t2: u64, t3: u64, t4_local: u64) {
        let state = match self.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            Some(s) => s,
            None => return,
        };

        match state.pending_t1 {
            Some(expected) if expected == t1 => {}
            _ => return,
        }

        let sample = SyncSample {
            t1,
            t2,
            t3,
            t4: t4_local,
        };

        if sample.delay() < 0 {
            state.pending_t1 = None;
            return;
        }

        state.record_sample(sample);
    }

    /// True, wenn alle Peers die konfigurierte Probenzahl erreicht haben.
    pub fn is_complete(&self) -> bool {
        !self.peers.is_empty() && self.peers.iter().all(|p| p.is_complete())
    }

    /// Ergebnis der Synchronisation — nur sinnvoll, wenn `is_complete()`.
    pub fn finalize(&self) -> Vec<PeerClock> {
        self.peers
            .iter()
            .filter_map(|p| p.as_peer_clock())
            .collect()
    }

    /// Konvergenzfunktion: Median ueber alle Peer-Offsets plus eigene Uhr (0).
    /// Rueckgabe: Korrektur in ns, die auf die eigene Uhr angewendet werden
    /// muss, um dem Gruppenmedian zu folgen.
    ///
    /// Median toleriert einen beliebig fehlerhaften Peer bei drei Knoten
    /// (Ausreisser landet am Rand, nicht in der Mitte).
    pub fn convergence_correction(&self) -> Option<i64> {
        let mut offsets: Vec<i64> = self
            .peers
            .iter()
            .filter_map(|p| p.as_peer_clock().map(|c| c.offset_ns))
            .collect();
        if offsets.is_empty() {
            return None;
        }
        offsets.push(0); // eigene Uhr = Referenz
        offsets.sort_unstable();
        Some(offsets[offsets.len() / 2])
    }

    /// Maximale bewiesene Fehlerschranke ueber alle Peers.
    /// Geht als epsilon in die Voter-Timeout-Auslegung ein.
    pub fn max_error_bound(&self) -> Option<i64> {
        self.peers
            .iter()
            .filter_map(|p| p.as_peer_clock())
            .map(|c| c.error_bound_ns)
            .max()
    }
}

/// Hilfstyp, um aus einem `RecvOutcome::TimeSync` die relevanten Felder
/// nach Request/Response zu unterscheiden.
pub enum SyncFields {
    Request {
        peer_id: u8,
        t1: u64,
        t2_local: u64,
    },
    Response {
        peer_id: u8,
        t1: u64,
        t2: u64,
        t3: u64,
        t4_local: u64,
    },
}

/// Extrahiert Sync-Felder aus einem empfangenen Frame.
pub fn extract_sync_fields<P: CyclePayload>(
    frame: &UdpFrame<P>,
    local_recv_ns: u64,
) -> Option<SyncFields> {
    match frame.payload() {
        Payload::TimeSyncReq { t1 } => Some(SyncFields::Request {
            peer_id: frame.node_id(),
            t1,
            t2_local: local_recv_ns,
        }),
        Payload::TimeSyncResp { t1, t2, t3 } => Some(SyncFields::Response {
            peer_id: frame.node_id(),
            t1,
            t2,
            t3,
            t4_local: local_recv_ns,
        }),
        _ => None,
    }
}

// ------------------------------------------------------------
// Tests
// ------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_delay_and_offset_symmetric() {
        // Symmetrische Latenz von 100 us in beide Richtungen,
        // Peer-Uhr laeuft 1 ms vor.
        let t1 = 1_000_000;
        let peer_offset = 1_000_000;
        let one_way = 100_000;
        let proc_time = 50_000;
        let t2 = t1 + one_way + peer_offset;
        let t3 = t2 + proc_time;
        let t4 = t3 - peer_offset + one_way;

        let s = SyncSample { t1, t2, t3, t4 };
        assert_eq!(s.delay(), 2 * one_way as i64);
        assert_eq!(s.offset(), peer_offset as i64);
    }

    #[test]
    fn error_bound_zero_when_delay_is_two_dt_min() {
        let t1 = 0;
        let t2 = DT_MIN_NS;
        let t3 = t2 + 1000;
        let t4 = t3 + DT_MIN_NS;
        let s = SyncSample { t1, t2, t3, t4 };
        assert_eq!(s.error_bound(), 0);
    }

    #[test]
    fn convergence_uses_median_ignoring_outlier() {
        let mut ps = PeerSync::new(&[1, 2]);
        // Beide Peers so anlegen, als seien Proben durchgelaufen.
        // Peer 1: Offset +100, Peer 2: Offset -1_000_000 (Ausreisser).
        ps.peers[0].best = Some(SyncSample {
            t1: 0,
            t2: 100,
            t3: 100,
            t4: 0,
        });
        ps.peers[0].samples_taken = SAMPLES_PER_PEER as u32;
        ps.peers[1].best = Some(SyncSample {
            t1: 0,
            t2: (-1_000_000_i64) as u64,
            t3: (-1_000_000_i64) as u64,
            t4: 0,
        });
        ps.peers[1].samples_taken = SAMPLES_PER_PEER as u32;
        // Median von {+100, -1_000_000, 0} = 0 (eigene Uhr).
        assert_eq!(ps.convergence_correction(), Some(0));
    }

    #[test]
    fn on_response_rejects_unmatched_t1() {
        let mut ps = PeerSync::new(&[1]);
        ps.record_outgoing_request(1, 12345);
        // Falsche t1 — muss verworfen werden.
        ps.on_response(1, 99999, 100, 200, 300);
        assert_eq!(ps.peers[0].samples_taken, 0);
        assert!(ps.peers[0].best.is_none());
    }
}
