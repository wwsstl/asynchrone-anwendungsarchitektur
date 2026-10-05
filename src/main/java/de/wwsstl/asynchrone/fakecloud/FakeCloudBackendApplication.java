package de.wwsstl.asynchrone.fakecloud;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import de.wwsstl.asynchrone.cloud.BatchJobStatus;
import de.wwsstl.asynchrone.cloud.CloudStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Eigenständige Applikation: Fake-Backend für den {@code WebClientCloudClient}
 * (anforderungen_integrationtest.md). Sie läuft in einem eigenen Prozess, unabhängig von der Pipeline-Applikation
 * ({@link de.wwsstl.asynchrone.TestdatenPipelineApplication}), und bildet Cloud-API 1, 2 und 3 so nach, wie es
 * {@code WebClientCloudClient} erwartet — jedoch ohne jede automatische Statuslogik:
 *
 * <ol>
 *   <li>{@code POST <job-path>} legt einen BatchgenAuftrag im Status {@code RUNNING} an (Cloud-API 1).</li>
 *   <li>{@code POST <submit-path>} nimmt Aufträge zu einem BatchgenAuftrag entgegen (Cloud-API 1) und vergibt je
 *       Datei eine TaskId; der Anfangsstatus ist immer {@code PENDING}.</li>
 *   <li>{@code GET <status-path>?taskIds=…} beantwortet die Bulk-Statusabfragen des {@code StatusConsumer}
 *       (Cloud-API 2) mit dem jeweils aktuell hinterlegten Status.</li>
 *   <li>{@code GET <job-path>…} und {@code PUT <job-path>/{jobId}/status} lesen und setzen den Status der
 *       BatchgenAufträge (Cloud-API 3).</li>
 * </ol>
 *
 * <p>Der Status eines Auftrags ändert sich <b>nie von selbst</b> — anders als das automatische Fake
 * ({@code FakeCloud}) in den Integrationstests. Stattdessen stellt diese Applikation einen Admin-Endpunkt bereit,
 * über den der Status eines Auftrags von außen manuell auf {@code SUCCESS} oder {@code ERROR} gesetzt werden kann.
 * Den Status eines BatchgenAuftrags setzt man von Hand über Cloud-API 3 selbst, etwa um einen Abbruch auszulösen
 * oder einen abgebrochenen BatchgenAuftrag zum Fortsetzen wieder auf {@code RUNNING} zu setzen. So lässt sich das
 * Zusammenspiel von Producer, Status-Pool und Consumer der Pipeline-Applikation von Hand durchspielen und
 * beobachten (z. B. Fehlerschwellenwert, Timeout, Abbruch, Fortsetzen, Dateiverschiebung).
 *
 * <p><b>Endpunkte</b> (Standard-Pfade entsprechen {@code application.yml}):
 * <ul>
 *   <li>{@code POST /tasks} — Cloud-API 1 (Auftrag zu einem BatchgenAuftrag übermitteln)</li>
 *   <li>{@code GET /tasks/status?taskIds=…&taskIds=…} — Cloud-API 2 (Bulk-Statusabfrage)</li>
 *   <li>{@code GET /tasks} — listet alle bekannten Aufträge mit Dateiname, TaskId, BatchgenAuftrag und Status</li>
 *   <li>{@code POST /tasks/{taskId}/status?status=SUCCESS|ERROR|PENDING} — setzt den Status manuell</li>
 *   <li>{@code POST /jobs} — Cloud-API 1 (BatchgenAuftrag anlegen)</li>
 *   <li>{@code GET /jobs?userId=…&status=…} — Cloud-API 3 (BatchgenAufträge suchen; ohne Parameter alle)</li>
 *   <li>{@code GET /jobs/{jobId}} — Cloud-API 3 (BatchgenAuftrag lesen)</li>
 *   <li>{@code PUT /jobs/{jobId}/status} mit {@code {"status":"RUNNING|COMPLETED|CANCELLED"}} — Cloud-API 3
 *       (Status setzen, auch von Hand)</li>
 * </ul>
 *
 * <p><b>Start:</b> {@code main}-Methode direkt ausführen, oder
 * {@code mvn -q compile exec:java -Dexec.mainClass=de.wwsstl.asynchrone.fakecloud.FakeCloudBackendApplication}.
 * Optionale Argumente in dieser Reihenfolge: Port (Vorgabe {@code 8081}), Submit-Pfad (Vorgabe {@code /tasks}),
 * Status-Pfad (Vorgabe {@code /tasks/status}), Job-Pfad (Vorgabe {@code /jobs}) — müssen zu
 * {@code pipeline.cloud.base-url}, {@code pipeline.cloud.submit-path}, {@code pipeline.cloud.status-path} und
 * {@code pipeline.cloud.job-path} der Pipeline-Applikation passen.
 */
public final class FakeCloudBackendApplication {

    private static final class Job {
        private final String batchJobId;
        private final String fileName;
        private volatile CloudStatus status = CloudStatus.PENDING;

        Job(String batchJobId, String fileName) {
            this.batchJobId = batchJobId;
            this.fileName = fileName;
        }
    }

    private static final class BatchJob {
        private final String userId;
        private volatile BatchJobStatus status = BatchJobStatus.RUNNING;

        BatchJob(String userId) {
            this.userId = userId;
        }
    }

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final Map<String, BatchJob> batchJobs = new ConcurrentHashMap<>();
    private final AtomicInteger batchJobSequence = new AtomicInteger();
    private final String submitPath;
    private final String statusPath;
    private final String jobPath;
    private final Pattern adminStatusPath;
    private final Pattern batchJobPath;
    private final HttpServer server;

    public FakeCloudBackendApplication(int port, String submitPath, String statusPath, String jobPath)
            throws IOException {
        this.submitPath = submitPath;
        this.statusPath = statusPath;
        this.jobPath = jobPath;
        this.adminStatusPath = Pattern.compile("^" + Pattern.quote(submitPath) + "/([^/]+)/status$");
        this.batchJobPath = Pattern.compile("^" + Pattern.quote(jobPath) + "/([^/]+?)(/status)?$");
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        // Der spezifischste Pfad gewinnt beim Routing; die Admin-Route liegt unter dem Submit-Pfad und wird im
        // Submit-Handler anhand des Musters "<submit-path>/{taskId}/status" erkannt.
        server.createContext(statusPath, this::handleStatus);
        server.createContext(submitPath, this::handleSubmitOrAdmin);
        server.createContext(jobPath, this::handleBatchJobs);
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

    // --- Cloud-API 1: Auftrag übermitteln ---------------------------------------------------------------------

    private void handleSubmitOrAdmin(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            Matcher admin = adminStatusPath.matcher(path);
            if (admin.matches()) {
                handleAdminStatusChange(exchange, admin.group(1));
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
        } catch (RuntimeException e) {
            reply(exchange, 500, Map.of("error", e.toString()));
        }
    }

    private void handleSubmit(HttpExchange exchange) throws IOException {
        JsonNode request = mapper.readTree(exchange.getRequestBody().readAllBytes());
        String batchJobId = request.path("jobId").asString("");
        if (!batchJobs.containsKey(batchJobId)) {
            reply(exchange, 400, Map.of("error", "unbekannter BatchgenAuftrag: " + batchJobId));
            return;
        }
        JsonNode items = request.get("items");
        List<Map<String, String>> tasks = new ArrayList<>();
        if (items != null) {
            for (JsonNode item : items) {
                String taskId = UUID.randomUUID().toString();
                String fileName = item.get("fileName").asString();
                jobs.put(taskId, new Job(batchJobId, fileName));
                tasks.add(Map.of("fileName", fileName, "taskId", taskId));
                System.out.printf("[submit] %s (%s) -> %s (PENDING)%n", fileName, batchJobId, taskId);
            }
        }
        reply(exchange, 200, Map.of("tasks", tasks));
    }

    private void handleList(HttpExchange exchange) throws IOException {
        List<Map<String, String>> all = jobs.entrySet().stream()
                .map(entry -> Map.of("taskId", entry.getKey(), "jobId", entry.getValue().batchJobId, "fileName",
                        entry.getValue().fileName, "status", entry.getValue().status.name()))
                .sorted(Comparator.comparing(m -> m.get("fileName")))
                .toList();
        reply(exchange, 200, Map.of("tasks", all));
    }

    // --- Cloud-API 2: Bulk-Statusabfrage -----------------------------------------------------------------------

    private void handleStatus(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                reply(exchange, 405, Map.of("error", "nicht unterstützte Methode"));
                return;
            }
            List<Map<String, String>> results = new ArrayList<>();
            for (String id : queryParams(exchange, "taskIds")) {
                Job job = jobs.get(id);
                if (job != null) {
                    results.add(Map.of("taskId", id, "status", job.status.name()));
                }
            }
            reply(exchange, 200, Map.of("results", results));
        } catch (RuntimeException e) {
            reply(exchange, 500, Map.of("error", e.toString()));
        }
    }

    // --- Cloud-API 1 und 3: BatchgenAufträge -------------------------------------------------------------------

    private void handleBatchJobs(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            if (jobPath.equals(path)) {
                switch (method) {
                    case "POST" -> createBatchJob(exchange);
                    case "GET" -> listBatchJobs(exchange);
                    default -> reply(exchange, 405, Map.of("error", "nicht unterstützte Methode"));
                }
                return;
            }
            Matcher matcher = batchJobPath.matcher(path);
            if (!matcher.matches()) {
                reply(exchange, 404, Map.of("error", "unbekannter Pfad: " + path));
                return;
            }
            String batchJobId = matcher.group(1);
            BatchJob batchJob = batchJobs.get(batchJobId);
            boolean statusRoute = matcher.group(2) != null;
            if (batchJob == null) {
                reply(exchange, 404, Map.of("error", "unbekannter BatchgenAuftrag: " + batchJobId));
            } else if (!statusRoute && "GET".equals(method)) {
                reply(exchange, 200, batchJobView(batchJobId, batchJob));
            } else if (statusRoute && ("PUT".equals(method) || "POST".equals(method))) {
                setBatchJobStatus(exchange, batchJobId, batchJob);
            } else {
                reply(exchange, 405, Map.of("error", "nicht unterstützte Methode"));
            }
        } catch (RuntimeException e) {
            reply(exchange, 500, Map.of("error", e.toString()));
        }
    }

    private void createBatchJob(HttpExchange exchange) throws IOException {
        String userId = mapper.readTree(exchange.getRequestBody().readAllBytes()).path("userId").asString("");
        if (userId.isEmpty()) {
            reply(exchange, 400, Map.of("error", "userId fehlt"));
            return;
        }
        String batchJobId = "BJ-" + batchJobSequence.incrementAndGet();
        batchJobs.put(batchJobId, new BatchJob(userId));
        System.out.printf("[batchjob] %s fuer %s angelegt (RUNNING)%n", batchJobId, userId);
        reply(exchange, 200, Map.of("jobId", batchJobId));
    }

    private void listBatchJobs(HttpExchange exchange) throws IOException {
        String userId = queryParam(exchange, "userId");
        String status = queryParam(exchange, "status");
        List<Map<String, String>> found = batchJobs.entrySet().stream()
                .filter(entry -> userId == null || userId.equals(entry.getValue().userId))
                .filter(entry -> status == null || status.equalsIgnoreCase(entry.getValue().status.name()))
                .map(entry -> batchJobView(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparing(m -> m.get("jobId")))
                .toList();
        reply(exchange, 200, Map.of("jobs", found));
    }

    private void setBatchJobStatus(HttpExchange exchange, String batchJobId, BatchJob batchJob) throws IOException {
        String requested = mapper.readTree(exchange.getRequestBody().readAllBytes()).path("status").asString("");
        BatchJobStatus status;
        try {
            status = BatchJobStatus.valueOf(requested.toUpperCase());
        } catch (IllegalArgumentException e) {
            reply(exchange, 400, Map.of("error", "status muss RUNNING, COMPLETED oder CANCELLED sein, war: "
                    + requested));
            return;
        }
        batchJob.status = status;
        System.out.printf("[batchjob] %s (%s) -> %s%n", batchJobId, batchJob.userId, status);
        reply(exchange, 200, batchJobView(batchJobId, batchJob));
    }

    private static Map<String, String> batchJobView(String batchJobId, BatchJob batchJob) {
        return Map.of("jobId", batchJobId, "userId", batchJob.userId, "status", batchJob.status.name());
    }

    // --- Admin: manuelle Statusänderung von außen --------------------------------------------------------------

    private void handleAdminStatusChange(HttpExchange exchange, String taskId) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod()) && !"PUT".equals(exchange.getRequestMethod())) {
            reply(exchange, 405, Map.of("error", "nicht unterstützte Methode"));
            return;
        }
        Job job = jobs.get(taskId);
        if (job == null) {
            reply(exchange, 404, Map.of("error", "unbekannte taskId: " + taskId));
            return;
        }
        String requested = queryParam(exchange, "status");
        CloudStatus status;
        try {
            status = requested == null ? null : CloudStatus.valueOf(requested.toUpperCase());
        } catch (IllegalArgumentException e) {
            status = null;
        }
        if (status == null) {
            reply(exchange, 400,
                    Map.of("error", "Query-Parameter 'status' muss SUCCESS, ERROR oder PENDING sein, war: "
                            + requested));
            return;
        }
        job.status = status;
        System.out.printf("[admin] %s (%s) -> %s%n", taskId, job.fileName, status);
        reply(exchange, 200, Map.of("taskId", taskId, "fileName", job.fileName, "status", status.name()));
    }

    private static String queryParam(HttpExchange exchange, String name) {
        List<String> values = queryParams(exchange, name);
        return values.isEmpty() ? null : values.getFirst();
    }

    /** Alle Werte eines (ggf. mehrfach angegebenen) Query-Parameters, z. B. {@code ?taskIds=a&taskIds=b}. */
    private static List<String> queryParams(HttpExchange exchange, String name) {
        String query = exchange.getRequestURI().getRawQuery();
        List<String> values = new ArrayList<>();
        if (query == null) {
            return values;
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            String key = eq < 0 ? pair : pair.substring(0, eq);
            if (name.equals(key)) {
                values.add(eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return values;
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
        String submitPath = args.length > 1 ? args[1] : "/tasks";
        String statusPath = args.length > 2 ? args[2] : "/tasks/status";
        String jobPath = args.length > 3 ? args[3] : "/jobs";

        FakeCloudBackendApplication app = new FakeCloudBackendApplication(port, submitPath, statusPath, jobPath);
        app.start();
        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));

        System.out.printf("""
                Fake-Cloud-Backend laeuft auf http://localhost:%d
                  POST %s              Cloud-API 1 (Auftrag uebermitteln)
                  GET  %s?taskIds=...       Cloud-API 2 (Bulk-Statusabfrage)
                  GET  %s              alle bekannten Auftraege auflisten
                  POST %s/{taskId}/status?status=SUCCESS|ERROR|PENDING   Status manuell setzen
                  POST %s               Cloud-API 1 (BatchgenAuftrag anlegen)
                  GET  %s?userId=...&status=...   Cloud-API 3 (BatchgenAuftraege suchen)
                  GET  %s/{jobId}       Cloud-API 3 (BatchgenAuftrag lesen)
                  PUT  %s/{jobId}/status   Cloud-API 3, Body {"status":"RUNNING|COMPLETED|CANCELLED"}

                In der Pipeline-Applikation muss pipeline.cloud.base-url auf http://localhost:%d zeigen.
                Beenden mit Strg+C.
                """, app.port(), submitPath, statusPath, submitPath, submitPath, jobPath, jobPath, jobPath, jobPath,
                app.port());

        new CountDownLatch(1).await();
    }
}
