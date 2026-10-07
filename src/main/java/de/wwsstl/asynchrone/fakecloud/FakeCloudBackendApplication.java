package de.wwsstl.asynchrone.fakecloud;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import de.wwsstl.asynchrone.cloud.JobStatus;
import de.wwsstl.asynchrone.cloud.RecordStatus;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Eigenständige Applikation: Fake-Backend für den {@code WebClientCloudClient}. Sie läuft in einem eigenen Prozess,
 * unabhängig von der Pipeline-Applikation ({@link de.wwsstl.asynchrone.TestdatenPipelineApplication}), und bildet
 * Cloud-API 1 bis 3 so nach, wie es {@code WebClientCloudClient} erwartet, jedoch ohne jede automatische
 * Statuslogik:
 *
 * <ol>
 *   <li>{@code POST <job-path>} legt einen BatchgenAuftrag mit dem Status {@code RUNNING} an (Cloud-API 1).</li>
 *   <li>{@code POST <submit-path>} nimmt Batches zu einem BatchgenAuftrag entgegen und vergibt je Datei eine TaskId;
 *       der Anfangsstatus ist immer {@code PENDING} (Cloud-API 1).</li>
 *   <li>{@code POST <status-path>} beantwortet die Bulk-Statusabfragen des {@code StatusConsumer} mit dem jeweils
 *       hinterlegten Status (Cloud-API 2).</li>
 *   <li>{@code PUT <job-path>/{jobId}/status} setzt den Endzustand, {@code GET <job-path>/{jobId}} liefert Status und
 *       Zahl der Datensätze je Status (Cloud-API 3).</li>
 * </ol>
 *
 * <p>Der Status eines Datensatzes ändert sich <b>nie von selbst</b>. Stattdessen gibt es Admin-Endpunkte, über die er
 * von außen auf {@code SUCCESS} oder {@code ERROR} gesetzt wird und über die sich die Fehlerfälle von Cloud-API 1 aus
 * funktionsweise_sequenz.md, Abschnitt 2, auslösen lassen. So lässt sich das Zusammenspiel der Module der
 * Pipeline-Applikation von Hand oder aus einem Black-Box-Integrationstest ({@code PipelineBlackBoxIT}) durchspielen.
 *
 * <p><b>Endpunkte</b> (Standard-Pfade entsprechen {@code application.yml}):
 * <ul>
 *   <li>{@code POST /jobs} – Cloud-API 1 (BatchgenAuftrag anlegen)</li>
 *   <li>{@code POST /tasks} – Cloud-API 1 (Batch übermitteln)</li>
 *   <li>{@code POST /tasks/status} – Cloud-API 2 (Bulk-Statusabfrage)</li>
 *   <li>{@code GET /jobs/{jobId}}, {@code PUT /jobs/{jobId}/status} – Cloud-API 3</li>
 *   <li>{@code GET /jobs} – alle BatchgenAufträge mit Status und Zahl der Datensätze je Status</li>
 *   <li>{@code GET /tasks} – alle Datensätze mit BatchgenAuftrag, Dateiname, TaskId und Status</li>
 *   <li>{@code POST /tasks/{taskId}/status?status=SUCCESS|ERROR|PENDING} – Status manuell setzen</li>
 *   <li>{@code POST /tasks/reject?fileName=…} – die nächste Übermittlung dieser Datei wird nicht umgewandelt (keine
 *       TaskId)</li>
 *   <li>{@code POST /tasks/next-response?status=…} – die nächste Übermittlung antwortet mit diesem HTTP-Status, ohne
 *       den Batch zu verarbeiten: {@code 503} gilt als eindeutig nicht verarbeitet, {@code 500} als unklar</li>
 * </ul>
 * Ist das Fake-Backend beendet, ist die Cloud für die Pipeline-Applikation nicht erreichbar.
 *
 * <p><b>Start:</b> {@code main}-Methode direkt ausführen oder {@code mvn spring-boot:run@fake-backend}. Optionale
 * Argumente in dieser Reihenfolge: Port (Vorgabe {@code 8081}), Job-Pfad ({@code /jobs}), Submit-Pfad
 * ({@code /tasks}), Status-Pfad ({@code /tasks/status}); sie müssen zu {@code pipeline.cloud.*} der
 * Pipeline-Applikation passen.
 */
public final class FakeCloudBackendApplication {

    private static final class Job {
        private final String jobId;
        private final String userId;
        private final long sequence;
        private volatile JobStatus status = JobStatus.RUNNING;

        Job(String jobId, String userId, long sequence) {
            this.jobId = jobId;
            this.userId = userId;
            this.sequence = sequence;
        }
    }

    private static final class DataRecord {
        private final String taskId;
        private final String jobId;
        private final String fileName;
        private final long sequence;
        private volatile RecordStatus status = RecordStatus.PENDING;

        DataRecord(String taskId, String jobId, String fileName, long sequence) {
            this.taskId = taskId;
            this.jobId = jobId;
            this.fileName = fileName;
            this.sequence = sequence;
        }
    }

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final Map<String, DataRecord> records = new ConcurrentHashMap<>();
    private final AtomicInteger jobSequence = new AtomicInteger();
    private final AtomicLong recordSequence = new AtomicLong();
    /** Dateinamen, deren nächste Übermittlung nicht umgewandelt wird. */
    private final Set<String> rejectNext = ConcurrentHashMap.newKeySet();
    /** HTTP-Status, mit denen die nächsten Übermittlungen ohne Verarbeitung antworten. */
    private final Queue<Integer> nextResponses = new ConcurrentLinkedQueue<>();
    private final String jobPath;
    private final String submitPath;
    private final Pattern jobPattern;
    private final Pattern adminStatusPattern;
    private final HttpServer server;

    public FakeCloudBackendApplication(int port, String jobPath, String submitPath, String statusPath)
            throws IOException {
        this.jobPath = jobPath;
        this.submitPath = submitPath;
        this.jobPattern = Pattern.compile("^" + Pattern.quote(jobPath) + "/([^/]+)(/status)?$");
        this.adminStatusPattern = Pattern.compile("^" + Pattern.quote(submitPath) + "/([^/]+)/status$");
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        // Der spezifischste Pfad gewinnt beim Routing; die Admin-Route liegt unter dem Submit-Pfad und wird im
        // Submit-Handler anhand des Musters "<submit-path>/{taskId}/status" erkannt.
        server.createContext(jobPath, exchange -> handle(exchange, this::handleJobs));
        server.createContext(statusPath, exchange -> handle(exchange, this::handleStatus));
        server.createContext(submitPath, exchange -> handle(exchange, this::handleSubmitOrAdmin));
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    public int port() {
        return server.getAddress().getPort();
    }

    // --- Cloud-API 1 und 3: BatchgenAufträge -------------------------------------------------------------------

    private void handleJobs(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();
        if (jobPath.equals(path)) {
            switch (method) {
                case "POST" -> createJob(exchange);
                case "GET" -> reply(exchange, 200, Map.of("jobs", jobs.values().stream()
                        .sorted(Comparator.comparingLong(job -> job.sequence)).map(this::view).toList()));
                default -> reply(exchange, 405, Map.of("error", "nicht unterstützte Methode"));
            }
            return;
        }
        Matcher matcher = jobPattern.matcher(path);
        Job job = matcher.matches() ? jobs.get(matcher.group(1)) : null;
        if (job == null) {
            reply(exchange, 404, Map.of("error", "unbekannter BatchgenAuftrag: " + path));
        } else if (matcher.group(2) == null && "GET".equals(method)) {
            reply(exchange, 200, view(job));
        } else if (matcher.group(2) != null && "PUT".equals(method)) {
            job.status = JobStatus.valueOf(readJson(exchange).path("status").asString(""));
            System.out.printf("[cloud-api-3] %s -> %s%n", job.jobId, job.status);
            reply(exchange, 200, view(job));
        } else {
            reply(exchange, 405, Map.of("error", "nicht unterstützte Methode"));
        }
    }

    private void createJob(HttpExchange exchange) throws IOException {
        String userId = readJson(exchange).path("userId").asString("");
        if (userId.isBlank()) {
            reply(exchange, 400, Map.of("error", "userId fehlt"));
            return;
        }
        int sequence = jobSequence.incrementAndGet();
        Job job = new Job("BJ-" + sequence, userId, sequence);
        jobs.put(job.jobId, job);
        System.out.printf("[cloud-api-1] BatchgenAuftrag %s fuer %s angelegt (RUNNING)%n", job.jobId, userId);
        reply(exchange, 200, Map.of("jobId", job.jobId));
    }

    private Map<String, Object> view(Job job) {
        Map<RecordStatus, Integer> counts = new EnumMap<>(RecordStatus.class);
        for (RecordStatus status : RecordStatus.values()) {
            counts.put(status, 0);
        }
        records.values().stream().filter(record -> record.jobId.equals(job.jobId))
                .forEach(record -> counts.merge(record.status, 1, Integer::sum));
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("jobId", job.jobId);
        view.put("userId", job.userId);
        view.put("status", job.status.name());
        view.put("counts", counts);
        return view;
    }

    // --- Cloud-API 1: Batch übermitteln -----------------------------------------------------------------------

    private void handleSubmitOrAdmin(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        Matcher admin = adminStatusPattern.matcher(path);
        if (admin.matches()) {
            handleAdminStatusChange(exchange, admin.group(1));
            return;
        }
        if ((submitPath + "/reject").equals(path) || (submitPath + "/next-response").equals(path)) {
            handleAdminSubmitBehavior(exchange, path.endsWith("/reject"));
            return;
        }
        if (!submitPath.equals(path)) {
            reply(exchange, 404, Map.of("error", "unbekannter Pfad: " + path));
            return;
        }
        switch (exchange.getRequestMethod()) {
            case "POST" -> handleSubmit(exchange);
            case "GET" -> handleList(exchange);
            default -> reply(exchange, 405, Map.of("error", "nicht unterstützte Methode"));
        }
    }

    private void handleSubmit(HttpExchange exchange) throws IOException {
        JsonNode request = readJson(exchange);
        String jobId = request.path("jobId").asString("");
        if (!jobs.containsKey(jobId)) {
            reply(exchange, 400, Map.of("error", "unbekannter BatchgenAuftrag: " + jobId));
            return;
        }
        Integer response = nextResponses.poll();
        if (response != null) {
            System.out.printf("[submit] %s: Batch nicht verarbeitet, Antwort %d%n", jobId, response);
            reply(exchange, response, Map.of("error", "vorgegebene Antwort " + response));
            return;
        }
        List<Map<String, String>> tasks = new ArrayList<>();
        for (JsonNode item : request.path("items")) {
            String fileName = item.path("fileName").asString("");
            if (rejectNext.remove(fileName)) {
                System.out.printf("[submit] %s: %s nicht umgewandelt%n", jobId, fileName);
                continue;
            }
            String taskId = UUID.randomUUID().toString();
            records.put(taskId, new DataRecord(taskId, jobId, fileName, recordSequence.incrementAndGet()));
            tasks.add(Map.of("fileName", fileName, "taskId", taskId));
            System.out.printf("[submit] %s: %s -> %s (PENDING)%n", jobId, fileName, taskId);
        }
        reply(exchange, 200, Map.of("tasks", tasks));
    }

    private void handleList(HttpExchange exchange) throws IOException {
        List<Map<String, String>> all = records.values().stream()
                .sorted(Comparator.comparingLong(record -> record.sequence))
                .map(FakeCloudBackendApplication::view)
                .toList();
        reply(exchange, 200, Map.of("tasks", all, "rejectNext", rejectNext.stream().sorted().toList(),
                "nextResponses", List.copyOf(nextResponses)));
    }

    private static Map<String, String> view(DataRecord record) {
        return Map.of("taskId", record.taskId, "jobId", record.jobId, "fileName", record.fileName, "status",
                record.status.name());
    }

    // --- Cloud-API 2: Bulk-Statusabfrage -----------------------------------------------------------------------

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            reply(exchange, 405, Map.of("error", "nicht unterstützte Methode"));
            return;
        }
        List<Map<String, String>> results = new ArrayList<>();
        for (JsonNode id : readJson(exchange).path("taskIds")) {
            DataRecord record = records.get(id.asString(""));
            if (record != null) {
                results.add(Map.of("taskId", record.taskId, "status", record.status.name()));
            }
        }
        reply(exchange, 200, Map.of("results", results));
    }

    // --- Admin: manuelle Statusänderung von außen --------------------------------------------------------------

    private void handleAdminStatusChange(HttpExchange exchange, String taskId) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod()) && !"PUT".equals(exchange.getRequestMethod())) {
            reply(exchange, 405, Map.of("error", "nicht unterstützte Methode"));
            return;
        }
        DataRecord record = records.get(taskId);
        if (record == null) {
            reply(exchange, 404, Map.of("error", "unbekannte taskId: " + taskId));
            return;
        }
        String requested = queryParam(exchange, "status");
        RecordStatus status;
        try {
            status = requested == null ? null : RecordStatus.valueOf(requested.toUpperCase());
        } catch (IllegalArgumentException e) {
            status = null;
        }
        if (status == null) {
            reply(exchange, 400, Map.of("error",
                    "Query-Parameter 'status' muss SUCCESS, ERROR oder PENDING sein, war: " + requested));
            return;
        }
        record.status = status;
        System.out.printf("[admin] %s (%s, %s) -> %s%n", taskId, record.jobId, record.fileName, status);
        reply(exchange, 200, view(record));
    }

    /** Fehlerfälle von Cloud-API 1 für die nächste Übermittlung vorgeben. */
    private void handleAdminSubmitBehavior(HttpExchange exchange, boolean reject) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            reply(exchange, 405, Map.of("error", "nicht unterstützte Methode"));
            return;
        }
        if (reject) {
            String fileName = queryParam(exchange, "fileName");
            if (fileName == null || fileName.isBlank()) {
                reply(exchange, 400, Map.of("error", "Query-Parameter 'fileName' fehlt"));
                return;
            }
            rejectNext.add(fileName);
            System.out.printf("[admin] naechste Uebermittlung von %s wird nicht umgewandelt%n", fileName);
            reply(exchange, 200, Map.of("rejectNext", rejectNext.stream().sorted().toList()));
            return;
        }
        String status = queryParam(exchange, "status");
        int code = status == null ? 0 : Integer.parseInt(status);
        if (code < 400 || code > 599) {
            reply(exchange, 400, Map.of("error", "Query-Parameter 'status' muss ein Fehlerstatus 4xx/5xx sein, war: "
                    + status));
            return;
        }
        nextResponses.add(code);
        System.out.printf("[admin] naechste Uebermittlung antwortet %d%n", code);
        reply(exchange, 200, Map.of("nextResponses", List.copyOf(nextResponses)));
    }

    // --- HTTP --------------------------------------------------------------------------------------------------

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }

    /** Ungültige Eingaben beantwortet der Server mit 400, sonstige Fehler mit 500. */
    private void handle(HttpExchange exchange, Handler handler) throws IOException {
        try {
            handler.handle(exchange);
        } catch (IllegalArgumentException | JacksonException e) {
            reply(exchange, 400, Map.of("error", String.valueOf(e.getMessage())));
        } catch (RuntimeException e) {
            reply(exchange, 500, Map.of("error", e.toString()));
        }
    }

    private JsonNode readJson(HttpExchange exchange) throws IOException {
        return mapper.readTree(exchange.getRequestBody().readAllBytes());
    }

    private static String queryParam(HttpExchange exchange, String name) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return null;
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            String key = eq < 0 ? pair : pair.substring(0, eq);
            if (name.equals(key)) {
                return eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private void reply(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] json = mapper.writeValueAsString(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, json.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(json);
        }
    }

    // --- Standalone-Start ---------------------------------------------------------------------------------------

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8081;
        String jobPath = args.length > 1 ? args[1] : "/jobs";
        String submitPath = args.length > 2 ? args[2] : "/tasks";
        String statusPath = args.length > 3 ? args[3] : "/tasks/status";

        FakeCloudBackendApplication app = new FakeCloudBackendApplication(port, jobPath, submitPath, statusPath);
        app.start();
        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));

        System.out.printf("""
                Fake-Cloud-Backend laeuft auf http://localhost:%1$d
                  POST %2$s                    Cloud-API 1 (BatchgenAuftrag anlegen)
                  POST %3$s                   Cloud-API 1 (Batch uebermitteln)
                  POST %4$s            Cloud-API 2 (Bulk-Statusabfrage)
                  GET  %2$s/{jobId}            Cloud-API 3 (BatchgenAuftrag lesen)
                  PUT  %2$s/{jobId}/status     Cloud-API 3 (Endzustand setzen)
                  GET  %2$s                    alle BatchgenAuftraege auflisten
                  GET  %3$s                   alle Datensaetze auflisten
                  POST %3$s/{taskId}/status?status=SUCCESS|ERROR|PENDING   Status manuell setzen
                  POST %3$s/reject?fileName=...          naechste Uebermittlung der Datei nicht umwandeln
                  POST %3$s/next-response?status=503|500 naechste Uebermittlung nicht verarbeiten

                In der Pipeline-Applikation muss pipeline.cloud.base-url auf http://localhost:%1$d zeigen.
                Beenden mit Strg+C.
                """, app.port(), jobPath, submitPath, statusPath);

        new CountDownLatch(1).await();
    }
}
