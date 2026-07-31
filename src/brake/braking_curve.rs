#![deny(unsafe_code)]

use serde::Deserialize;
use std::fmt;

pub const BRAKE_BUILDUP_TIME_S: f64 = 2.5;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecelerationStage {
    pub v_min: f64, // m/s (einschließlich)
    pub v_max: f64, // m/s (ausschließlich)
    pub a: f64,     // m/s^2
}

pub const DECELERATION_STAGES: &[DecelerationStage] = &[
    DecelerationStage {
        v_min: 0.0,
        v_max: 10.0,
        a: 1.0,
    },
    DecelerationStage {
        v_min: 10.0,
        v_max: 20.0,
        a: 0.9,
    },
    DecelerationStage {
        v_min: 20.0,
        v_max: 30.0,
        a: 0.8,
    },
    DecelerationStage {
        v_min: 30.0,
        v_max: 40.0,
        a: 0.7,
    },
    DecelerationStage {
        v_min: 40.0,
        v_max: 50.0,
        a: 0.6,
    },
    DecelerationStage {
        v_min: 50.0,
        v_max: f64::INFINITY,
        a: 0.5,
    },
];

// ---------------------------------------------------------------------------
// Ein- und Ausgabetypen
// ---------------------------------------------------------------------------

/// Eingabegrößen für eine Bremswegberechnung.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub struct BrakeInput {
    /// Aktuelle Geschwindigkeit `v_0` in m/s.
    pub current_speed: f64,
    /// Zielgeschwindigkeit `v_ziel` in m/s (0 = vollständiger Halt).
    pub target_speed: f64,
    /// Verfügbare Reststrecke `s_verfuegbar` in Metern.
    pub available_distance: f64,
}

impl BrakeInput {
    pub fn new(current_speed: f64, target_speed: f64, available_distance: f64) -> Self {
        Self {
            current_speed,
            target_speed,
            available_distance,
        }
    }
}

/// Ergebnis einer Bremswegberechnung.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BrakeResult {
    // Gesamtbremsweg `s_gesamt` in Metern.
    pub total_distance: f64,
    // Entscheidung: Notbremse auslösen?
    pub emergency_brake: bool,
    // Is the brake result valid?
    pub valid_entry: bool,
}

impl BrakeResult {
    fn new_empty_result() -> Self {
        Self {
            total_distance: 0.0,
            emergency_brake: false,
            valid_entry: false,
        }
    }

    fn new_valid_result(total_distance: f64, emergency_brake: bool) -> Self {
        Self {
            total_distance,
            emergency_brake,
            valid_entry: true,
        }
    }
}

/// Fehler bei der Bremswegberechnung.
///
/// Alle Fehler sind reine Eingabe-Validierungsfehler; die Rechnung selbst
/// ist unter validen Eingaben totalfunktional (keine Division durch Null,
/// keine Überläufe im relevanten Wertebereich).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BrakeError {
    /// Ein Eingabewert war NaN oder unendlich.
    NotFinite,
    /// Ein Geschwindigkeitswert war negativ.
    NegativeSpeed,
    /// Verfügbare Strecke war negativ.
    NegativeDistance,
    /// Zielgeschwindigkeit größer als aktuelle Geschwindigkeit
    /// – für ein Bremsmodell nicht sinnvoll.
    TargetAboveCurrent,
}

impl fmt::Display for BrakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrakeError::NotFinite => write!(f, "input contains NaN or infinity"),
            BrakeError::NegativeSpeed => write!(f, "speed must be non-negative"),
            BrakeError::NegativeDistance => write!(f, "available distance must be non-negative"),
            BrakeError::TargetAboveCurrent => {
                write!(f, "target speed must not exceed current speed")
            }
        }
    }
}

impl std::error::Error for BrakeError {}

// ---------------------------------------------------------------------------
// Hauptfunktion
// ---------------------------------------------------------------------------

/// Berechnet den Bremsweg und die Notbremsentscheidung für die gegebene Eingabe.
///
/// Vorgehen:
///   1. Eingaben validieren.
///   2. Reaktionsweg `s_reaktion = v_0 · t_brems` bestimmen.
///   3. Über die Verzögerungsstufen von `v_0` abwärts zu `v_ziel` iterieren
///      und Teilbremswege gemäß `s_i = (v_i² − v_{i+1}²) / (2 · a_i)` summieren.
///   4. Notbremsentscheidung anhand `s_gesamt ≥ s_verfuegbar`.
pub fn compute_braking_curve(input: BrakeInput) -> Result<BrakeResult, BrakeError> {
    validate(&input)?;

    let reaction_distance = input.current_speed * BRAKE_BUILDUP_TIME_S;
    let braking_distance = compute_braking_distance(input.current_speed, input.target_speed);
    let total_distance = reaction_distance + braking_distance;
    let emergency_brake = total_distance >= input.available_distance;

    Ok(BrakeResult::new_valid_result(
        total_distance,
        emergency_brake,
    ))
}

// ---------------------------------------------------------------------------
// Interna
// ---------------------------------------------------------------------------

fn validate(input: &BrakeInput) -> Result<(), BrakeError> {
    if !input.current_speed.is_finite()
        || !input.target_speed.is_finite()
        || !input.available_distance.is_finite()
    {
        return Err(BrakeError::NotFinite);
    }
    if input.current_speed < 0.0 || input.target_speed < 0.0 {
        return Err(BrakeError::NegativeSpeed);
    }
    if input.available_distance < 0.0 {
        return Err(BrakeError::NegativeDistance);
    }
    if input.target_speed > input.current_speed {
        return Err(BrakeError::TargetAboveCurrent);
    }
    Ok(())
}

/// Bremsphasen-Anteil: iteriert über die Stufentabelle von `v_start` abwärts
/// zu `v_target`. Innerhalb jeder Stufe wird der Anteil gemäß
/// `s = (v_hi² − v_lo²) / (2 · a)` addiert.
///
/// Erwartet gültige Eingaben (`v_start ≥ v_target ≥ 0`, beide finit).
fn compute_braking_distance(v_start: f64, v_target: f64) -> f64 {
    // Sonderfall: keine Bremsphase nötig.
    if v_start <= v_target {
        return 0.0;
    }

    let mut distance = 0.0;
    let mut v_upper = v_start;

    // Wir laufen die Stufen von der höchsten zur niedrigsten Geschwindigkeit,
    // weil wir abbremsen. Also die Slice in umgekehrter Reihenfolge.
    for stage in DECELERATION_STAGES.iter().rev() {
        // Diese Stufe deckt [stage.v_min, stage.v_max) ab.
        // Wir bremsen in dieser Stufe von `v_upper` runter auf
        // `max(stage.v_min, v_target)` – je nachdem was zuerst erreicht wird.

        if v_upper <= stage.v_min {
            // Wir sind noch nicht in dieser Stufe angekommen (weil wir
            // von oben nach unten iterieren, heißt das: v_upper liegt
            // unterhalb dieser Stufe).
            continue;
        }

        // v_lower ist die Untergrenze für diese Stufe im aktuellen Zug.
        let v_lower = stage.v_min.max(v_target);

        // Wenn v_upper bereits kleiner als v_lower ist, ist in dieser
        // Stufe nichts mehr zu tun.
        if v_upper <= v_lower {
            continue;
        }

        // Teilbremsweg dieser Stufe: (v_upper² − v_lower²) / (2 · a)
        distance += (v_upper * v_upper - v_lower * v_lower) / (2.0 * stage.a);

        // Nach dieser Stufe ist die neue Obergrenze v_lower.
        v_upper = v_lower;

        // Ziel erreicht? Dann fertig.
        if v_upper <= v_target {
            break;
        }
    }

    distance
}
