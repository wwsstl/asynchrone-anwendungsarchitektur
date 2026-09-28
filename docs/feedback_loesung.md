Auch Abschnitt 3 (Task Manager) auf Konsistenz mit dem neuen Status-Pool-Design prüfen

## 1. Entscheidungen für die offenen Entscheidungen in der nächsten Abstimmung
*Abschnitt2: Empfehlung nehmen
*Abschnitt4: Empfehlung nehmen
*Abschnitt5: Empfehlung nehmen
*Abschnitt6: Empfehlung nehmen
*Abschnitt7: Empfehlung nehmen
*Abschnitt8: Empfehlung nehmen
*Abschnitt9: Empfehlung nehmen
*Abschnitt10: Empfehlung nehmen
*Abschnitt11: Empfehlung nehmen

*Strategie: Virtual Threads durchgängig
*Status-Pool-Implementierung: Map + periodischer Sweep (Option B, empfohlen)
*Unterstützt Cloud-API 2 eine Sammel-Statusabfrage (mehrere TaskIds pro Aufruf)?: ja
*Persistenzstrategie für Statusinformationen: rein in-memory (Phase 1)
*Der REST-API-Vertrag soll nur Start/Abbruch/Status sein
*Die Pipeline bleibt vollständig auf dem eigenen Producer/Consumer/Status-Pool-Modell aufgebaut.

nehmen Java21
SpringBoot 4
Jackson 3

