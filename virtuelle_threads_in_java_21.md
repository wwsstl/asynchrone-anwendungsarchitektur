# Virtuelle Threads in Java 21

## Was sind virtuelle Threads

Virtuelle Threads wurden mit Java 21 als reguläres Feature eingeführt (JEP 444) – leichtgewichtige Threads, die von der JVM verwaltet und geplant werden, nicht vom Betriebssystem. Ein klassischer `Thread` (Plattform-Thread) entspricht direkt einem Betriebssystem-Thread, während ein virtueller Thread auf einer kleinen Anzahl von Plattform-Threads läuft, die als "Carrier-Threads" bezeichnet werden.

## Kernunterschiede

| | Plattform-Thread (Standard) | Virtueller Thread |
|---|---|---|
| Scheduler | Betriebssystem-Kernel | JVM (User-Space-Scheduling) |
| Erstellungskosten | Hoch, meist mehrere Hundert KB bis 1 MB Stack | Sehr gering, Stack wächst dynamisch, anfangs nur wenige Hundert Byte |
| Anzahl | In der Regel wenige Tausend als praktische Grenze | Problemlos mehrere Millionen möglich |
| Verhältnis zu OS-Threads | 1:1 | M:N (viele virtuelle Threads teilen sich wenige Plattform-Threads) |
| Blockierverhalten | Blockiert → belegt den OS-Thread weiter | Blockiert → wird automatisch vom Carrier-Thread "abgekoppelt" (unmount) |

### Der Mechanismus: Mount/Unmount

Ein virtueller Thread wird zur Ausführung auf einem Carrier-Thread "gemountet". Führt er eine blockierende Operation aus (I/O, `Thread.sleep`, Warten auf ein Lock), koppelt die JVM ihn automatisch vom Carrier-Thread ab, sichert den Stack-Zustand im Heap und gibt den Carrier-Thread für andere virtuelle Threads frei. Sobald die Blockierbedingung erfüllt ist, wird er wieder auf einen Carrier-Thread gemountet. Das geschieht transparent – der Code sieht aus wie gewöhnlicher synchroner, blockierender Code.

## Verwendung

```java
// Einzelnen virtuellen Thread starten
Thread.startVirtualThread(() -> {
    System.out.println("Hallo aus virtuellem Thread");
});

// Empfohlen: ExecutorService mit virtuellen Threads
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    for (int i = 0; i < 100_000; i++) {
        executor.submit(() -> {
            Thread.sleep(Duration.ofSeconds(1));
            return "erledigt";
        });
    }
} // wartet automatisch auf alle Tasks und schließt den Executor
```

## Vorteile

1. **Hoher Durchsatz bei Nebenläufigkeit**: Das Modell "ein Thread pro Anfrage" unterstützt Millionen gleichzeitiger Aufgaben – ideal für I/O-lastige Szenarien (Datenbankzugriffe, HTTP-Aufrufe, Microservice-Kommunikation).
2. **Einfaches Programmiermodell**: Kein Umschreiben auf `CompletableFuture` oder reaktive Ansätze (z. B. WebFlux, RxJava) nötig. Synchroner, blockierender Code erreicht von sich aus hohe Nebenläufigkeit – Lesbarkeit, Debugging (Stacktraces, Breakpoints) sind deutlich einfacher als bei reaktiver Programmierung.
3. **Geringer Ressourcenverbrauch**: Erstellung und Kontextwechsel sind viel günstiger als bei Plattform-Threads, auch der GC-Druck sinkt.
4. **Kompatibel mit bestehendem Ökosystem**: `Thread`, `ExecutorService`, `ThreadLocal` (mit Einschränkungen, siehe unten), Debugger und Profiler funktionieren größtenteils direkt weiter.

## Nachteile / Einschränkungen

1. **Ungeeignet für CPU-intensive Aufgaben**: Virtuelle Threads lösen das Problem des "blockierenden Wartens". Bei reinen Rechenaufgaben ohne Blockierpunkte konkurrieren sie weiterhin um die begrenzten CPU-Kerne bzw. Carrier-Threads – es entsteht kein zusätzlicher Parallelisierungsgewinn. Hier ist ein fest dimensionierter Plattform-Thread-Pool oft besser geeignet.
2. **Pinning-Problem bei `synchronized`**: In früheren Versionen konnte ein virtueller Thread, der innerhalb eines `synchronized`-Blocks blockiert, nicht abgekoppelt werden (er "pinnt" den Carrier-Thread fest) – der Carrier-Thread wird dadurch verschwendet. Java 21 hat dies teilweise verbessert, dennoch wird empfohlen, `synchronized` in Kontexten mit virtuellen Threads durch `java.util.concurrent.locks.ReentrantLock` zu ersetzen. Auch blockierende native Methodenaufrufe (JNI) führen zu Pinning.
3. **Problematischer Einsatz von `ThreadLocal`**: Da potenziell sehr viele virtuelle Threads existieren, kann exzessiver Gebrauch von `ThreadLocal` mit großen Objekten den Speicherverbrauch stark erhöhen. Für viele Anwendungsfälle empfiehlt Oracle stattdessen `ScopedValue` (Preview-Feature in Java 21).
4. **Kein Pooling von virtuellen Threads**: Virtuelle Threads sind für den einmaligen Gebrauch konzipiert ("use and discard") und extrem günstig in der Erstellung – sie sollten nicht wie Plattform-Threads in Pools wiederverwendet werden. Konstrukte wie `newFixedThreadPool` passen hier nicht.
5. **Begrenzte Anzahl an Carrier-Threads**: Standardmäßig entspricht die Größe des Carrier-Thread-Pools der Anzahl der CPU-Kerne. Werden viele virtuelle Threads gleichzeitig gepinnt oder führen CPU-intensive Operationen aus, entsteht dennoch ein Engpass.
6. **Tooling noch im Reifeprozess**: Manche APM-Tools und Thread-Dump-Analysewerkzeuge unterstützen die Darstellung und das Sampling von Millionen virtueller Threads noch nicht vollständig.

## Kurzfassung

Virtuelle Threads bringen im Kern das Konzept von Coroutinen/Fasern in die Java-Standardbibliothek und lösen damit das Problem der begrenzten Anzahl von Plattform-Threads sowie deren hoher Kontextwechselkosten in I/O-lastigen Diensten mit hoher Nebenläufigkeit. Sie beschleunigen CPU-intensiven Code nicht, machen aber das einfache "Thread-pro-Anfrage"-Modell wieder praktikabel und effizient.