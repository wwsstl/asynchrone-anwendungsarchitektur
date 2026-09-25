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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import de.wwsstl.asynchrone.cloud.CloudStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Eigenständige Applikation: Fake-Backend für den {@code WebClientCloudClient}
 * (anforderungen_intergrationtest.md). Sie läuft in einem eigenen Prozess, unabhängig von der Pipeline-Applikation
 * ({@link de.wwsstl.asynchrone.TestdatenPipelineApplication}), und bildet Cloud-API 1 und Cloud-API 2 so nach, wie
 * es {@code WebClientCloudClient} erwartet — jedoch ohne jede automatische Statuslogik:
 *
 * <ol>
 *   <li>{@code POST <submit-path>} nimmt Aufträge entgegen (Cloud-API 1) und vergibt je Datei eine TaskId; der
 *       Anfangsstatus ist immer {@code PENDING}.</li>
 *   <li>{@code GET <status-path>?taskIds=…} beantwortet die Bulk-Statusabfragen des {@code StatusConsumer}
 *       (Cloud-API 2) mit dem jeweils aktuell hinterlegten Status.</li>
 * </ol>
 *
 * <p>Der Status eines Auftrags ändert sich <b>nie von selbst</b> — anders als das automatische Fake
 * ({@code FakeCloud}) in den Integrationstests. Stattdessen stellt diese Applikation einen Admin-Endpunkt bereit,
 * über den der Status eines Auftrags von außen manuell auf {@code SUCCESS} oder {@code ERROR} gesetzt werden kann.
 * So lässt sich das Zusammenspiel von Producer, Status-Pool und Consumer der Pipeline-Applikation von Hand
 * durchspielen und beobachten (z. B. Fehlerschwellenwert, Timeout, Dateiverschiebung).
 *
 * <p><b>Endpunkte</b> (Standard-Pfade entsprechen {@code application.yml}):
 * <ul>
 *   <li>{@code POST /tasks} — Cloud-API 1 (Auftrag übermitteln)</li>
 *   <li>{@code GET /tasks/status?taskIds=…&taskIds=…} — Cloud-API 2 (Bulk-Statusabfrage)</li>
 *   <li>{@code GET /tasks} — listet alle bekannten Aufträge mit Dateiname, TaskId und aktuellem Status</li>
 *   <li>{@code POST /tasks/{taskId}/status?status=SUCCESS|ERROR|PENDING} — setzt den Status manuell</li>
 * </ul>
 *
 * <p><b>Start:</b> {@code main}-Methode direkt ausführen, oder
 * {@code mvn -q compile exec:java -Dexec.mainClass=de.wwsstl.asynchrone.fakecloud.FakeCloudBackendApplication}.
 * Optionale Argumente in dieser Reihenfolge: Port (Vorgabe {@code 8081}), Submit-Pfad (Vorgabe {@code /tasks}),
 * Status-Pfad (Vorgabe {@code /tasks/status}) — müssen zu {@code pipeline.cloud.base-url},
 * {@code pipeline.cloud.submit-path} und {@code pipeline.cloud.status-path} der Pipeline-Applikation passen.
 */
public final class FakeCloudBackendApplication {

    private static final class Job {
        private final String fileName;
        private volatile CloudStatus status = CloudStatus.PENDING;

        Job(String fileName) {
            this.fileName = fileName;
        }
    }

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final String submitPath;
    private final String statusPath;
    private final Pattern adminStatusPath;
    private final HttpServer server;

    public FakeCloudBackendApplication(int port, String submitPath, String statusPath) throws IOException {
        this.submitPath = submitPath;
        this.statusPath = statusPath;
        this.adminStatusPath = Pattern.compile("^" + Pattern.quote(submitPath) + "/([^/]+)/status$");
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        // Der spezifischste Pfad gewinnt beim Routing; die Admin-Route liegt unter dem Submit-Pfad und wird im
        // Submit-Handler anhand des Musters "<submit-path>/{taskId}/status" erkannt.
        server.createContext(statusPath, this::handleStatus);
        server.createContext(submitPath, this::handleSubmitOrAdmin);
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
        JsonNode items = mapper.readTree(exchange.getRequestBody().readAllBytes()).get("items");
        List<Map<String, String>> tasks = new ArrayList<>();
        if (items != null) {
            for (JsonNode item : items) {
                String taskId = UUID.randomUUID().toString();
                String fileName = item.get("fileName").asString();
                jobs.put(taskId, new Job(fileName));
                tasks.add(Map.of("fileName", fileName, "taskId", taskId));
                System.out.printf("[submit] %s -> %s (PENDING)%n", fileName, taskId);
            }
        }
        reply(exchange, 200, Map.of("tasks", tasks));
    }

    private void handleList(HttpExchange exchange) throws IOException {
        List<Map<String, String>> all = jobs.entrySet().stream()
                .map(entry -> Map.of("taskId", entry.getKey(), "fileName", entry.getValue().fileName, "status",
                        entry.getValue().status.name()))
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

        FakeCloudBackendApplication app = new FakeCloudBackendApplication(port, submitPath, statusPath);
        app.start();
        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));

        System.out.printf("""
                Fake-Cloud-Backend laeuft auf http://localhost:%d
                  POST %s              Cloud-API 1 (Auftrag uebermitteln)
                  GET  %s?taskIds=...       Cloud-API 2 (Bulk-Statusabfrage)
                  GET  %s              alle bekannten Auftraege auflisten
                  POST %s/{taskId}/status?status=SUCCESS|ERROR|PENDING   Status manuell setzen

                In der Pipeline-Applikation muss pipeline.cloud.base-url auf http://localhost:%d zeigen.
                Beenden mit Strg+C.
                """, app.port(), submitPath, statusPath, submitPath, submitPath, app.port());

        new CountDownLatch(1).await();
    }
}
