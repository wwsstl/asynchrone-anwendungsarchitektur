# Ablauf einer Aufgabe (erste Ebene)

Sequenzdiagramm: [`funktionsweise_sequenz.mmd`](funktionsweise_sequenz.mmd) – erste Verfeinerung des Überblicks in [`anwendungsarchitektur.mmd`](anwendungsarchitektur.mmd). Die Nummern der Abschnitte entsprechen den Phasen im Diagramm. Chinesische Fassung: [`../funktionsweise_sequenz_zh.md`](../funktionsweise_sequenz_zh.md).

Stand: Phase 1 nach [`erste_phase_zusatz_anforderungen.md`](erste_phase_zusatz_anforderungen.md) – genau eine Instanz der Anwendung (ein Prozess, ein Server). Die Zustände des BatchgenAuftrags und die Ursachencodes sind dort festgelegt; dieses Dokument beschreibt den Ablauf.

Grundsätze:

- Die Aufgabennummer ist die Nummer des BatchgenAuftrags. Jede Aufgabe hat genau einen BatchgenAuftrag. Abgebrochene Aufgaben werden nicht fortgesetzt; ihre Reste in der `pendingbox` räumt das eigenständige Programm CleanMassenanlageAuftrag auf (Abschnitt 7).
- Der Zustand einer Aufgabe liegt in der Cloud: Status des BatchgenAuftrags, Status der Datensätze und lastSeen. Die Anwendung hält nur die laufenden Sandboxen in ihrer lokalen TaskRegistry; Momentaufnahmen, Historie oder Endzustände speichert sie nicht.
- Jede Statusänderung am BatchgenAuftrag ist ein atomarer bedingter Schreibvorgang in der Cloud: Sie gelingt nur, wenn der aktuelle Status passt (siehe erste_phase_zusatz_anforderungen.md, Abschnitt 2.1).

## 1. Start

Die Benutzer:in startet über die REST-API eine Aufgabe. Die API ist mit Spring WebFlux umgesetzt und reicht den Aufruf auf einem Virtual Thread an den TaskManager weiter, damit der Event-Loop nicht blockiert.

Der Start läuft in drei Schritten:

1. **`pendingbox` prüfen:** Nach `COMPLETED` oder `ERROR` ist die `pendingbox` leer; die Prüfung ist eine Absicherung. Ist sie nicht leer, ruft der TaskManager Cloud-API 1 nicht auf und weist den Start mit `409 Conflict` ab. Für den Ursachencode fragt er über Cloud-API 3 nach einem BatchgenAuftrag der Benutzer:in in `RUNNING` oder `CANCELED`: `RUNNING` → `USER_TASK_RUNNING`, `CANCELED` → `CLEANUP_REQUIRED`, keiner → `PENDINGBOX_NOT_EMPTY`.
2. **BatchgenAuftrag anlegen:** Der TaskManager lässt über Cloud-API 1 einen neuen BatchgenAuftrag anlegen. Hat die Benutzer:in bereits einen BatchgenAuftrag in `RUNNING` oder `CANCELED`, lehnt die Cloud das in derselben atomaren Operation ab und liefert dessen Nummer und Status. Der Start wird dann mit `409 Conflict` abgewiesen: `RUNNING` → `USER_TASK_RUNNING`, `CANCELED` → `CLEANUP_REQUIRED`.
3. **Sandbox starten:** Unter der Aufgabennummer baut der TaskManager eine eigene Sandbox auf (Kontext, Status-Pool, Producer- und Consumer-Thread), trägt sie in die lokale TaskRegistry ein und startet Producer und Consumer. Die API antwortet sofort mit `202 Accepted`, dem Aufgabenstatus `RUNNING`, der Aufgabennummer und der Adresse für spätere Statusabfragen.

Ob die Benutzer:in schon eine Aufgabe hat, entscheidet allein die Cloud beim Anlegen. Die Anwendung führt dafür keine eigene Liste; doppelte Startanfragen (Doppelklick, Wiederholung nach einem Timeout des Clients) und Neustarts der Anwendung sind damit abgedeckt.

Ist Cloud-API 1 oder 3 nicht erreichbar, antwortet die API mit `502 Bad Gateway`. Zwei Sonderfälle werden wie ein unerwartetes Ende der Anwendung behandelt (Abschnitt 6): Der Aufruf von Cloud-API 1 läuft in ein Timeout, obwohl die Cloud den BatchgenAuftrag angelegt hat; oder die Sandbox startet nach dem Anlegen nicht. Der BatchgenAuftrag steht dann auf `RUNNING`, ohne dass eine Sandbox ihn bearbeitet.

## 2. Producer – übermitteln

Der Producer arbeitet die `inbox` batchweise ab:

1. Bis zu `batch-size` Dateien in die `pendingbox` verschieben.
2. Die JSON-Daten dieser Dateien zusammen mit der BatchgenAuftrag-Nummer in einer einzigen Anfrage an Cloud-API 1 übermitteln. Alle Batches der Aufgabe gehören zu dem BatchgenAuftrag aus dem Start (Abschnitt 1). Cloud-API 1 antwortet mit JSON-Daten.
3. Die JSON-Antwort von Cloud-API 1 gibt an, welche Dateien erfolgreich in Datenerzeugungsaufträge umgewandelt wurden – jeweils mit TaskId – und welche nicht.
4. Für die erfolgreich umgewandelten Dateien wird die TaskId als Marker gesichert und in den Status-Pool eingetragen.
5. Die nicht umgewandelten Dateien werden direkt in die `errorbox` verschoben.

Weil Cloud-API 1 nicht idempotent ist, wird jede Datei höchstens einmal übermittelt. Nach jedem Batch wartet der Producer, bis der Status-Pool auf `pool-resume-threshold` oder darunter gesunken ist (Rückstau), und liest erst dann die nächste Charge.

## 3. Consumer – Status verfolgen

Parallel zum Producer holt der Consumer in jedem `sweep-interval` die fälligen TaskIds aus dem Status-Pool, fasst sie zu einer einzigen Anfrage zusammen und fragt ihren Status gebündelt bei Cloud-API 2 ab:

| Status | Folge |
|---|---|
| `SUCCESS` | Datei wandert von der `pendingbox` in die `donebox`; der Marker wird gelöscht. |
| `ERROR` | Datei wandert in die `errorbox`; der Marker wird gelöscht, der Fehlerzähler steigt um eins. |
| `PENDING` | Der Eintrag bleibt im Status-Pool und wird nach `poll-interval` erneut geprüft. |

Zusätzlich aktualisiert der Consumer in jedem Durchlauf über Cloud-API 3 das Feld lastSeen des BatchgenAuftrags (Herzschlag). Die Cloud schreibt dabei ihre eigene Uhrzeit, damit unterschiedliche Uhren keine Rolle spielen. Am Herzschlag erkennt CleanMassenanlageAuftrag, ob noch eine Sandbox an dem BatchgenAuftrag arbeitet (Abschnitt 7).

Kann der Consumer lastSeen mehrmals hintereinander nicht aktualisieren (z. B. weil Cloud-API 3 nicht erreichbar ist), beendet er die Aufgabe selbst, und zwar bevor das Herzschlag-Timeout abläuft (z. B. nach 2 fehlgeschlagenen Durchläufen bei einem Timeout von 3 `sweep-interval`). Der BatchgenAuftrag bleibt `RUNNING`; es gilt dasselbe wie bei einem unerwarteten Ende (Abschnitt 6). So ist sicher, dass keine Sandbox mehr arbeitet, sobald lastSeen abgelaufen ist.

## 4. Statusabfrage

Die Benutzer:in kann den Status jederzeit abfragen. Die Antwort stammt vollständig aus der Cloud: Status des BatchgenAuftrags, die Zahl der Datensätze je Status (`SUCCESS`, `ERROR`, `PENDING`) und lastSeen. Weil die Anwendung nichts davon selbst speichert, bleibt die Statusabfrage auch nach einem Neustart der Anwendung unverändert möglich.

Der Status hat die vier Werte der Cloud: `RUNNING`, `CANCELED`, `COMPLETED` und `ERROR`; einen eigenen Status `CANCELLING` gibt es nicht. Ob noch eine Sandbox arbeitet, zeigt lastSeen:

- **`RUNNING` oder `CANCELED`, lastSeen frisch:** Die Sandbox läuft noch bzw. räumt gerade auf.
- **`RUNNING` oder `CANCELED`, lastSeen abgelaufen:** Keine Sandbox arbeitet mehr, etwa nach einem Abbruch von außen (Abschnitt 5) oder einem unerwarteten Ende. Die Benutzer:in kann CleanMassenanlageAuftrag starten.

Zähler, die nur lokal existieren (z. B. fehlgeschlagene Dateiverschiebungen), sind in der Antwort nicht enthalten.

## 5. Abbruch von außen

Phase 1 übernimmt die bestehende Umsetzung: Bricht die Benutzer:in eine Aufgabe über die REST-API ab, stoppt der TaskManager die Sandbox direkt lokal.

- Er findet die Sandbox in seiner TaskRegistry und setzt das Abbruchsignal im Kontext; Producer und Consumer beenden sich daraufhin. Ein laufender Aufruf von Cloud-API 1 wird nicht unterbrochen, weil Cloud-API 1 nicht idempotent ist; die zurückgelieferten TaskIds werden noch als Marker gesichert.
- Dateien, deren Ergebnis noch aussteht, bleiben mit ihrem Marker in der `pendingbox`.
- Die Sandbox wird aus der TaskRegistry entfernt. Ist die Aufgabe lokal nicht bekannt, antwortet die API mit `404 Not Found`.
- Die Cloud wird nicht beschrieben: Der BatchgenAuftrag bleibt `RUNNING`, und lastSeen wird nicht mehr aktualisiert. Danach gilt dasselbe wie bei einem unerwarteten Ende: Ist lastSeen abgelaufen, startet die Benutzer:in CleanMassenanlageAuftrag (Abschnitt 7), das die Reste verteilt und den BatchgenAuftrag auf `ERROR` setzt. Bis dahin wird ein neuer Start mit `USER_TASK_RUNNING` abgewiesen.

Ein Abbruch über den Status des BatchgenAuftrags in der Cloud, wie ihn der Betrieb mit mehreren Instanzen braucht, folgt in Phase 2 (siehe [`zusatz_anforderungen.md`](zusatz_anforderungen.md)).

## 6. Abschluss und Abmeldung

Die Aufgabe endet auf einem der folgenden Wege. Wo die Anwendung den Status des BatchgenAuftrags setzt, geschieht das mit einem bedingten Schreibvorgang über Cloud-API 3, der nur aus `RUNNING` gelingt:

| Ende | Status des BatchgenAuftrags | Reste in der `pendingbox` |
|---|---|---|
| **Normales Ende:** Der Producer ist fertig und der Status-Pool leer | `COMPLETED` | keine |
| **Fehlerschwelle:** `error-threshold` erreicht | `CANCELED` | bleiben; CleanMassenanlageAuftrag räumt auf |
| **Timeout:** `task-timeout` überschritten | `ERROR` | die Anwendung verschiebt sie in die `errorbox`; kein Aufräumen nötig |
| **Abbruch von außen** (Abschnitt 5) | bleibt `RUNNING` | bleiben; CleanMassenanlageAuftrag räumt nach Ablauf von lastSeen auf |
| **Unerwartetes Ende:** Absturz, OOM, Serverausfall | bleibt `RUNNING` | bleiben; CleanMassenanlageAuftrag räumt nach Ablauf von lastSeen auf |

Beim normalen Ende darf einzelnes `ERROR` vorkommen, solange die Fehlerschwelle nicht erreicht ist. Wird ein bedingter Schreibvorgang abgelehnt, etwa weil jemand den Status direkt in der Cloud geändert hat, wiederholt die Anwendung ihn nicht, beendet die Aufgabe und lässt den Status in der Cloud unverändert.

**Abmeldung:** Sobald Producer- und Consumer-Thread ausgelaufen sind, meldet der TaskManager die Sandbox ab und entfernt sie aus seiner TaskRegistry; Threads, Status-Pool und Kontext werden freigegeben. Einen Endzustand legt die Anwendung nicht ab – die Aufgabe bleibt über die Cloud abfragbar (Abschnitt 4).

## 7. Aufräumen mit CleanMassenanlageAuftrag

CleanMassenanlageAuftrag ist ein eigenständiges Programm und gehört nicht zur Anwendung. Es räumt einen BatchgenAuftrag in `RUNNING` oder `CANCELED` auf, sobald keine Sandbox mehr daran arbeitet:

1. **lastSeen frisch:** Das Programm lehnt ab und meldet „Aufgabe läuft noch, bitte später erneut versuchen“. Maßgeblich ist die Uhrzeit der Cloud: Cloud-API 3 liefert die seit lastSeen vergangene Zeit.
2. **`pendingbox` nicht leer:** Das Programm verteilt die Reste nach dem aktuellen Status aus Cloud-API 2 auf `donebox` und `errorbox`, bis die `pendingbox` leer ist, und setzt den BatchgenAuftrag dann auf `ERROR`.
3. **`pendingbox` leer:** Das Programm setzt den BatchgenAuftrag direkt auf `ERROR`.

Lässt sich die `pendingbox` nicht leeren (z. B. weil Datensätze in der Cloud nie einen Endstatus erreichen), wird sie von Hand geleert und das Programm danach erneut gestartet.

Erst wenn der BatchgenAuftrag auf `COMPLETED` oder `ERROR` steht, kann die Benutzer:in eine neue Aufgabe starten. Das Herzschlag-Timeout ist konfigurierbar (z. B. 3 `sweep-interval`); Anwendung und CleanMassenanlageAuftrag verwenden denselben Wert.

## 8. Betrieb in Phase 1

- **Genau eine Instanz:** Der Abbruch von außen (Abschnitt 5) erreicht nur Sandboxen der Instanz, die die Anfrage erhält. Auf denselben Benutzerordnern darf deshalb nur ein Prozess der Anwendung laufen; absichern lässt sich das mit einer Dateisperre (`FileChannel.tryLock()`) oder als Betriebsregel. Anwendung und CleanMassenanlageAuftrag greifen auf dieselben Benutzerordner zu.
- **Neustart und Update:** Weil abgebrochene Aufgaben nicht fortgesetzt werden, würde ein Neustart laufende Aufgaben wie ein unerwartetes Ende beenden. Vor einem geplanten Neustart wird daher zuerst der Start neuer Aufgaben gesperrt (z. B. `503 Service Unavailable` im Wartungsmodus), dann gewartet, bis die TaskRegistry leer ist, und erst dann neu gestartet. Ungeplante Ausfälle lassen sich nicht vermeiden; sie werden wie ein unerwartetes Ende behandelt.
- **Ursachencodes:** Die Codes und die Hinweise für die Oberfläche stehen in erste_phase_zusatz_anforderungen.md, Abschnitt 8.
- **Phase 2:** Was sich beim Betrieb mit mehreren Instanzen ändert (Abbruch über die Cloud, Neustart mehrerer Instanzen, gemeinsames Volume), steht in erste_phase_zusatz_anforderungen.md, Abschnitt 10, und in [`zusatz_anforderungen.md`](zusatz_anforderungen.md).
