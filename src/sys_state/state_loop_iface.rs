use core::time::Duration;
use heapless::Vec;

use crate::net::udp_frame::UdpFrame;

// ============================================================
// Systemweite Obergrenze
// ============================================================

/// Maximale unterstuetzte Peer-Anzahl. Von der Bitmasken-Wahl u8
/// aufgesetzt: mehr als 8 Peers erfordern u16 in PeerMask und
/// koherente Anpassung ueberall.
pub const MAX_PEERS: usize = 8;

// ============================================================
// Trait-Grenzen
// ============================================================

pub trait Clock {
    fn now(&self) -> core::time::Duration;   // monoton, Startpunkt egal
    fn sleep_until(&self, deadline: core::time::Duration);
}

pub trait Transport {
    /// Nicht-blockierend. Naechstes Frame oder None.
    fn try_recv(&mut self) -> Option<UdpFrame>;
    fn send(&mut self, frame: &UdpFrame) -> Result<(), TransportError>;
}

#[derive(Debug, Clone, Copy)]
pub enum TransportError { Io, WouldBlock }

/// Inhaltliche Arbeit pro Phase. Generisch in `N`, damit vote_* die
/// gesammelten Peer-Frames als korrekt dimensionierten Slice bekommt.
pub trait CycleExecutor<const N: usize> {
    fn self_test(&mut self) -> Result<(), ExecError>;
    fn read_inputs(&mut self) -> Result<(), ExecError>;
    fn calc_critical(&mut self) -> Result<(), ExecError>;

    fn build_result_frame(&self) -> UdpFrame;
    fn vote_result(&mut self, own: &UdpFrame, peers: &[UdpFrame])
        -> Result<(), ExecError>;

    fn build_ack_frame(&self) -> UdpFrame;
    /// Rueckgabe = gewaehlte Publisher-ID. Executor merkt sich intern,
    /// ob dieser Knoten der Publisher ist.
    fn vote_publisher(&mut self, own: &UdpFrame, peers: &[UdpFrame])
        -> Result<u8, ExecError>;

    fn publish(&mut self) -> Result<(), ExecError>;
    fn enter_failsafe(&mut self);
}

#[derive(Debug, Clone, Copy)]
pub enum ExecError { NoConsensus, Unrecoverable, Local }

// ============================================================
// PeerMask — Bitmaske ueber Slot-Indizes in LoopConfig::peers
// ============================================================

/// Bit i = Peer i (Slot in `LoopConfig::peers`) fehlt.
/// Wichtig: die Maske indiziert **Slots**, nicht Node-IDs. Damit ist
/// die Groesse fix an N gekoppelt und unabhaengig vom ID-Wertebereich.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PeerMask(pub u8);

impl PeerMask {
    pub const EMPTY: Self = Self(0);

    #[inline] pub fn set(&mut self, slot: usize) {
        debug_assert!(slot < MAX_PEERS);
        self.0 |= 1 << slot;
    }
    #[inline] pub fn clear(&mut self, slot: usize) {
        debug_assert!(slot < MAX_PEERS);
        self.0 &= !(1 << slot);
    }
    #[inline] pub fn contains(self, slot: usize) -> bool {
        debug_assert!(slot < MAX_PEERS);
        (self.0 >> slot) & 1 == 1
    }
    #[inline] pub fn count(self) -> u32 { self.0.count_ones() }
    #[inline] pub fn is_empty(self) -> bool { self.0 == 0 }

    /// Iterator ueber gesetzte Slot-Indizes, aufsteigend.
    pub fn iter(self) -> impl Iterator<Item = usize> {
        (0..MAX_PEERS).filter(move |i| self.contains(*i))
    }
    #[inline] pub fn as_u8(&self) -> u8 { self.0 }
    #[inline] pub const fn from_u8(b: u8) -> Self { PeerMask(b) }
}

// ============================================================
// CollectOutcome — Ergebnis einer Peer-Frame-Sammlung
// ============================================================

/// Ergebnis der Frame-Sammlung fuer eine Vote-Phase. Generisch in N,
/// damit die enthaltene Frame-Sammlung ohne Heap auskommt.
pub enum CollectOutcome<const N: usize> {
    /// Alle erwarteten Peers haben geliefert.
    Complete(Vec<UdpFrame, N>),
    /// Quorum erreicht, aber nicht komplett. `missing` markiert die
    /// fehlenden Slots. Nur relevant, wenn Quorum < erwartete Peers.
    Quorum { frames: Vec<UdpFrame, N>, missing: PeerMask },
    /// Quorum nicht erreicht: neu ausgefallene Peers zusaetzlich zu
    /// den bereits vor der Phase bekannten.
    QuorumLost { new_missing: PeerMask },
    /// Ein Peer, der zuvor als fehlend gefuehrt wurde, meldet sich
    /// wieder. Slot-Index des Rueckkehrers.
    PeerReturned { slot: usize },
}

// ============================================================
// Konfiguration
// ============================================================

pub struct LoopConfig<const N: usize> {
    pub node_id: u8,
    pub peers: [u8; N],

    /// Anzahl uebereinstimmender Knoten (dieser + Peer-Frames), die fuer
    /// Konsens noetig sind. Fuer 2oo3: 2. Fuer 5oo7: 5. Muss <= N+1.
    pub quorum: u8,

    pub cycle_period: Duration,
    pub peer_timeout: Duration,
    pub sync_timeout: Duration,
    pub poll_tick: Duration,
    pub rejoining: bool,
}

impl<const N: usize> LoopConfig<N> {
    /// Compile-Zeit-Grenzen sind auf stable Rust nur begrenzt
    /// ausdrueckbar; die Runtime-Assertions laufen einmalig beim Start.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if N == 0                    { return Err(ConfigError::NoPeers); }
        if N > MAX_PEERS             { return Err(ConfigError::TooManyPeers); }
        if self.quorum == 0          { return Err(ConfigError::QuorumZero); }
        if self.quorum as usize > N + 1 { return Err(ConfigError::QuorumTooHigh); }
        // Node-ID darf nicht auch in peers stehen.
        if self.peers.iter().any(|&p| p == self.node_id) {
            return Err(ConfigError::SelfInPeers);
        }
        // Peer-IDs muessen paarweise verschieden sein.
        for i in 0..N { for j in (i+1)..N {
            if self.peers[i] == self.peers[j] {
                return Err(ConfigError::DuplicatePeers);
            }
        }}
        Ok(())
    }

    /// Slot-Index (0..N) fuer eine Peer-ID, falls vorhanden.
    #[inline] pub fn slot_of(&self, id: u8) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ConfigError {
    NoPeers, TooManyPeers,
    QuorumZero, QuorumTooHigh,
    SelfInPeers, DuplicatePeers,
}