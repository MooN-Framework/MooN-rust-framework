Generelle Marschrichtung
========================
- Softwaresynchronisierte m-oo-n Systeme
- Implementierung in Rust
- formal saubere Beschreibung
- Unsafe Rust umgehen?!
- Fokus auf Rechenschritt+Voting, Ein-/Ausgabe nur am Rande

Themen und Software-synchronisierte m-oo-n Systeme
==================================================

- Untersuchung: Welche Synchronisationsfrequenz erreichbar? Wovon abhängig? Wodurch begrenzt?
- geeignete CRC/Hash-Funktion: 
  - Timing abhängig von Größe des CriticalBlocks -> Limit/Abwägung Speichergröße
  - Fehleroffenbarungswahrscheinlichkeit / Kollisionswahrscheinlichkeit
- Saubere (FMEA-inspirierte) + vollständige (hazop?) Übersicht Fehlerfälle mit Testmöglichkeiten
- Test automatisieren? Wie systematisch testen?
- Besseres Management: Verteilung der Software von einer Konsole, Flottenmanagement, Konfigurationsfiles?, Discovery
- Rekonfiguration in beide Richtungen, Hot Plugging
- gute, überzeugende Demo
- Eingabe (Demo-getrieben)
- Ausgabe (Demo-getrieben)
- Thema systematisieren
- Fail-Safe _und_ Fail-Operational Anwendungen (bis 1-oo-n)
- inhomogene Redundanz
- Erreichbares Sicherheitsniveau zu bestimmen:
  - Modell für zufällige Fehler im Rechenkanal + Konzept für systematische Fehler
  - Theorie/Literatur: Common Cause Fehler
  - Einschätzung SIL nach Bahnnormen
- Redundante Kommunikation
- Was passiert bei unklaren Mehrheitsverhältnissen? 2v4, je zwei gleiche CRCs
- Speichertest, CPU-Test, noch was?

MA Kevin Weiss
==============
BBB-Meetings: https://vc2.sonia.de/b/rooms/cla-xx9-242/join
