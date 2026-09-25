Was die aktuelle Architektur betrifft: Enthält jede Sandbox insgesamt zwei virtuelle Threads, und ist es erforderlich, weitere Threads zuzuweisen?

● Kurz: Ja, jede Sandbox hat genau zwei virtuelle Threads. Weitere Threads braucht die aktuelle Architektur nicht.

Was im Code steht

TaskManager.java:113-117 legt pro Task an:
- producer-<taskId> (Thread.ofVirtual()), ausgeführt wird InboxProducer
- consumer-<taskId> (Thread.ofVirtual()), ausgeführt wird StatusConsumer

Sandbox.start() startet genau diese beiden Threads. Andere eigene Threads gibt es pro Sandbox nicht.

Dazu kommen Threads, die sich alle Sandboxen teilen und die nicht pro Sandbox entstehen:
- Carrier-Threads der JVM (ForkJoinPool, etwa so viele wie CPU-Kerne). Auf ihnen laufen alle virtuellen Threads.
- Reactor-Netty-Event-Loop des WebClient. Er erledigt die HTTP-Ein- und Ausgabe für alle San
- Tomcat-Request-Threads für die REST-API. Sie sind ebenfalls virtuell (spring.threads.virtual.enabled: true).

Bei n Benutzern/Tasks sind es also 2n virtuelle Threads. Das kostet praktisch nichts.

Warum keine weiteren Threads nötig sind

1. Die Arbeit wartet fast nur auf I/O. Beide Threads warten die meiste Zeit: auf HTTP-Antworten (join() bzw. get()), auf awaitCancel(sweepInterval) oder auf Dateioperationen. Ein wartender virtueller Thread belegt keinen Carrier-Thread. Mehr Threads bringen hier keinen Durchsatz.
2. Der Producer arbeitet absichtlich nacheinander. Er soll pausieren, bis der Pool auf pool-resume-threshold abgebaut ist (Punkt 1/3 der Anforderungen). Mehrere Producer pro Sandbox würden diesen Rückstau umgehen.
3. Der Pool bleibt klein. Bei batch-size: 4 und pool-resume-threshold: 2 sind höchstens etwa 6 Einträge im Pool. Die status-bulk-size liegt bei 200. Der Consumer braucht also pro Durchlauf höchstens einen Bulk-Aufruf, und paralleles Abfragen hätte nichts zu tun.
4. Weitere Threads würden das Sandbox-Modell verkomplizieren. Der Pool gehört zur Sandbox, und seen im Producer wird ausdrücklich nur vom Producer-Thread benutzt. Mehr Threads pro Sandbox hießen: mehr Synchronisation und ein neues producerFinished-Signal für mehrere Producer.

Wann es sich ändern könnte

- Deutlich größere Batches oder Schwellenwerte, sodass sweep() mehrere Bulk-Chunks nacheinander abarbeiten muss. Auch dann ist die bessere Lösung, die CompletableFutures aus queryStatus gleichzeitig abzuschicken   
  und gemeinsam abzuwarten (allOf). Zusätzliche Threads braucht man dafür nicht.
- Wenn die Anforderung wegfällt, dass der Producer nach jedem Batch wartet. Dann könnte er mehrere submit-Aufrufe gleichzeitig offen halten, ebenfalls asynchron über die Futures statt mit mehr Threads.
- Bei sehr vielen gleichzeitigen Tasks. Der Engpass wären dann das Connection-Pool-Limit des WebClient und die Rate-Limits der Cloud-APIs, nicht die Zahl der Threads.



Fazit: Zwei virtuelle Threads pro Sandbox passen zu den Anforderungen. Sollte später mehr Parallelität nötig sein, würde ich sie über asynchrone Futures lösen und nicht über zusätzliche Threads.



