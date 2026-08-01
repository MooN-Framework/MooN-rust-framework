//! Framework-weite Konfigurationstypen.
//!
//! Zentrale Anlaufstelle fuer die Datentypen, die den Rahmen der
//! Systemarbeit vorgeben. Aktuell nur `ParticipantConfig`; spaeter
//! sollen hier auch die anderen Konfig-Structs zusammengefuehrt werden
//! (TransportConfig, DiagnosticConfig, HealthConfig, CycleTimingConfig).

// -------------------------------------------------------------
// Framework-Grenzen
// -------------------------------------------------------------

/// Compile-Zeit-Obergrenze fuer die Gesamtzahl von Nodes im System.
/// Begrenzt durch die 8-Bit-Peer-Maske im Wire-Protokoll: ein Bit pro Peer,
/// eigener Node explizit → 8 Nodes maximal.
pub const MAX_TOTAL_NODES: usize = 8;

/// Anzahl der Peer-Slots pro Node (jeder Node kennt sich selbst + Peers).
/// Wird als const-Grosse fuer `heapless::Vec` im gesamten Framework
/// verwendet.
pub const MAX_PEERS: usize = MAX_TOTAL_NODES - 1;

// -------------------------------------------------------------
// ParticipantConfig (M-oo-N)
// -------------------------------------------------------------

/// M-oo-N-Konfiguration: beschreibt Sollstaerke und Untergrenze.
///
/// - `nominal_participants` (N): Gesamtzahl der Nodes im Normalbetrieb.
///   Wird zum Startup erwartet; Discovery muss GENAU N Nodes finden,
///   sonst Fehlstart.
/// - `min_participants` (M): Untergrenze fuer sicheren Betrieb. Sinkt
///   die aktive Anzahl darunter, geht das System in Failsafe.
///
/// Klassische Deployments:
/// - 2oo3: nominal=3, minimum=2 (Fail-Operational, verkraftet 1 Ausfall)
/// - 2oo2: nominal=2, minimum=2 (Fail-Safe, keine Toleranz)
/// - 6oo8: nominal=8, minimum=6 (verkraftet 2 Ausfaelle)
/// - 8oo8: nominal=8, minimum=8 (Fail-Safe, keine Toleranz)
#[derive(Debug, Clone, Copy)]
pub struct ParticipantConfig {
    pub nominal_participants: u8,
    pub min_participants: u8,
}

impl ParticipantConfig {
    /// Erzeugt eine ParticipantConfig. Panic bei ungueltigen Werten
    /// (Konstruktions-Zeit — sollte beim Start sofort auffliegen, nicht
    /// im laufenden Betrieb).
    ///
    /// Reihenfolge: `minimum` zuerst, weil M die Sicherheitsuntergrenze
    /// ist und intuitiv zuerst gedacht wird ("mindestens X von Y").
    pub fn new(minimum: u8, nominal: u8) -> Self {
        assert!(minimum >= 1, "min_participants muss >= 1 sein");
        assert!(
            minimum <= nominal,
            "min_participants ({}) darf nicht groesser als nominal ({}) sein",
            minimum,
            nominal
        );
        assert!(
            nominal as usize <= MAX_TOTAL_NODES,
            "nominal_participants ({}) ueberschreitet Framework-Grenze ({})",
            nominal,
            MAX_TOTAL_NODES
        );
        Self {
            nominal_participants: nominal,
            min_participants: minimum,
        }
    }

    /// Anzahl der verkraftbaren Ausfaelle bis Failsafe: N - M.
    pub fn tolerable_failures(&self) -> u8 {
        self.nominal_participants - self.min_participants
    }

    /// Maximale Anzahl Peers (eigener Node ausgenommen).
    pub fn max_peers(&self) -> usize {
        (self.nominal_participants - 1) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_configs_work() {
        let two_oo_three = ParticipantConfig::new(2, 3);
        assert_eq!(two_oo_three.tolerable_failures(), 1);
        assert_eq!(two_oo_three.max_peers(), 2);

        let two_oo_two = ParticipantConfig::new(2, 2);
        assert_eq!(two_oo_two.tolerable_failures(), 0);

        let six_oo_eight = ParticipantConfig::new(6, 8);
        assert_eq!(six_oo_eight.tolerable_failures(), 2);
    }

    #[test]
    #[should_panic(expected = "min_participants muss >= 1 sein")]
    fn zero_minimum_panics() {
        let _ = ParticipantConfig::new(0, 3);
    }

    #[test]
    #[should_panic(expected = "darf nicht groesser als nominal")]
    fn minimum_over_nominal_panics() {
        let _ = ParticipantConfig::new(3, 2);
    }

    #[test]
    #[should_panic(expected = "ueberschreitet Framework-Grenze")]
    fn nominal_over_max_panics() {
        let _ = ParticipantConfig::new(5, 9);
    }
}