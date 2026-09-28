Bitte stellen Sie ein Fake-Backend für den `WebClientCloudClient` bereit, das auf Port 8081 läuft.
1. Es muss in der Lage sein, Anfragen für `SubmittedTask` unter dem durch `config.submitPath()` definierten Pfad entgegenzunehmen.
2. Es muss in der Lage sein, Polling-Anfragen des `StatusConsumer` unter dem durch `config.statusPath()` definierten Pfad zu empfangen (wobei der Anfangsstatus von `SubmittedTask` auf `pending` gesetzt ist) und eine manuelle Statusänderung von außen auf `SUCCESS` oder `ERROR` zu ermöglichen.

Ziel ist es, eine manuelle Simulation und das Testen der Interaktionen zwischen den verschiedenen Modulen der Anwendung zu ermöglichen, um deren erwartungsgemäße Funktion zu verifizieren.
