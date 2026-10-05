package de.wwsstl.asynchrone;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Lokaler HTTP-Server, der die Cloud-Dienste nachbildet. Das Verhalten der Aufträge wird über den Dateiinhalt
 * gesteuert:
 * <ul>
 *   <li>Inhalt enthält {@code FAIL} → Cloud meldet {@code ERROR}</li>
 *   <li>Inhalt enthält {@code SLOW} → Cloud meldet {@code PENDING}, bis die Datei per {@link #release} freigegeben
 *       wird; danach {@code SUCCESS}</li>
 *   <li>sonst → {@code PENDING} bei der ersten Abfrage, danach {@code SUCCESS}</li>
 * </ul>
 * BatchgenAufträge (Cloud-API 1 anlegen, Cloud-API 3 lesen und setzen) liegen unter {@code /jobs}; ihr Status lässt
 * sich wie von Hand mit {@link #setJobStatus} ändern.
 */
public final class FakeCloud implements AutoCloseable {

    /** Ein an Cloud-API 2 gerichteter Bulk-Aufruf: die abgefragten Dateinamen. */
    public record StatusCall(List<String> fileNames) {
    }

    private record Job(String fileName, String content, int[] polls) {
    }

    private static final class BatchJob {
        private final String userId;
        private volatile String status = "RUNNING";

        BatchJob(String userId) {
            this.userId = userId;
        }
    }

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final HttpServer server;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final Map<String, BatchJob> batchJobs = new ConcurrentHashMap<>();
    private final AtomicInteger batchJobSequence = new AtomicInteger();
    private final List<Integer> submitBatchSizes = new CopyOnWriteArrayList<>();
    private final List<StatusCall> statusCalls = new CopyOnWriteArrayList<>();
    private final List<String> submittedFileNames = new CopyOnWriteArrayList<>();
    private final Set<String> released = ConcurrentHashMap.newKeySet();
    private final Set<String> unavailableFor = ConcurrentHashMap.newKeySet();

    public FakeCloud() {
        try {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/tasks/status", this::status);
        server.createContext("/tasks", this::submit);
        server.createContext("/jobs", this::batchJobs);
        server.start();
    }

    public String baseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    public List<Integer> submitBatchSizes() {
        return List.copyOf(submitBatchSizes);
    }

    public List<StatusCall> statusCalls() {
        return List.copyOf(statusCalls);
    }

    /** Alle je an Cloud-API 1 übermittelten Dateinamen, in Aufrufreihenfolge (Duplikate = Mehrfachübermittlung). */
    public List<String> submittedFileNames() {
        return List.copyOf(submittedFileNames);
    }

    /** Lässt die {@code SLOW}-Aufträge dieser Dateien ab der nächsten Abfrage mit {@code SUCCESS} enden. */
    public void release(Collection<String> fileNames) {
        released.addAll(fileNames);
    }

    /** Status eines BatchgenAuftrags laut Cloud-API 3; {@code null}, wenn es ihn nicht gibt. */
    public String jobStatus(String jobId) {
        BatchJob job = batchJobs.get(jobId);
        return job == null ? null : job.status;
    }

    /** Setzt den Status eines BatchgenAuftrags wie von Hand (z. B. Fortsetzen: wieder {@code RUNNING}). */
    public void setJobStatus(String jobId, String status) {
        batchJobs.get(jobId).status = status;
    }

    /** Legt einen BatchgenAuftrag im Status {@code RUNNING} an, als wäre er vor einem Absturz entstanden. */
    public String createRunningJob(String userId) {
        String jobId = "BJ-" + batchJobSequence.incrementAndGet();
        batchJobs.put(jobId, new BatchJob(userId));
        return jobId;
    }

    /** Die Nummern aller BatchgenAufträge der Benutzer:in. */
    public List<String> jobsOf(String userId) {
        return batchJobs.entrySet().stream().filter(entry -> entry.getValue().userId.equals(userId))
                .map(Map.Entry::getKey).sorted().toList();
    }

    /** Cloud-API 1 und 3 antworten für diese Benutzer:in mit {@code 503}. */
    public void makeUnavailableFor(String userId) {
        unavailableFor.add(userId);
    }

    private void submit(HttpExchange exchange) throws IOException {
        if (!"/tasks".equals(exchange.getRequestURI().getPath())) {
            reply(exchange, 404, "{}");
            return;
        }
        JsonNode request = mapper.readTree(exchange.getRequestBody().readAllBytes());
        BatchJob batchJob = batchJobs.get(request.path("jobId").asString(""));
        if (batchJob == null) {
            reply(exchange, 400, "{\"error\":\"unbekannter BatchgenAuftrag\"}");
            return;
        }
        JsonNode items = request.get("items");
        submitBatchSizes.add(items.size());
        List<Map<String, String>> tasks = new ArrayList<>();
        for (JsonNode item : items) {
            String taskId = UUID.randomUUID().toString();
            String fileName = item.get("fileName").asString();
            submittedFileNames.add(fileName);
            jobs.put(taskId, new Job(fileName, item.get("content").asString(), new int[1]));
            tasks.add(Map.of("fileName", fileName, "taskId", taskId));
        }
        reply(exchange, 200, mapper.writeValueAsString(Map.of("tasks", tasks)));
    }

    private void status(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            reply(exchange, 405, "{}");
            return;
        }
        List<String> fileNames = new ArrayList<>();
        List<Map<String, String>> results = new ArrayList<>();
        for (String id : queryParams(exchange, "taskIds")) {
            Job job = jobs.get(id);
            fileNames.add(job.fileName());
            results.add(Map.of("taskId", id, "status", statusOf(job, released.contains(job.fileName()))));
        }
        statusCalls.add(new StatusCall(Collections.unmodifiableList(fileNames)));
        reply(exchange, 200, mapper.writeValueAsString(Map.of("results", results)));
    }

    /** {@code POST /jobs}, {@code GET /jobs?userId=&status=}, {@code GET /jobs/{id}}, {@code PUT /jobs/{id}/status}. */
    private void batchJobs(HttpExchange exchange) throws IOException {
        String[] path = exchange.getRequestURI().getPath().substring(1).split("/");
        String method = exchange.getRequestMethod();
        if (path.length == 1 && "POST".equals(method)) {
            String userId = mapper.readTree(exchange.getRequestBody().readAllBytes()).path("userId").asString("");
            if (unavailableFor.contains(userId)) {
                reply(exchange, 503, "{}");
                return;
            }
            reply(exchange, 200, mapper.writeValueAsString(Map.of("jobId", createRunningJob(userId))));
        } else if (path.length == 1 && "GET".equals(method)) {
            String userId = queryParams(exchange, "userId").getFirst();
            if (unavailableFor.contains(userId)) {
                reply(exchange, 503, "{}");
                return;
            }
            List<String> status = queryParams(exchange, "status");
            List<Map<String, String>> found = batchJobs.entrySet().stream()
                    .filter(entry -> entry.getValue().userId.equals(userId))
                    .filter(entry -> status.isEmpty() || status.getFirst().equals(entry.getValue().status))
                    .map(entry -> view(entry.getKey(), entry.getValue()))
                    .toList();
            reply(exchange, 200, mapper.writeValueAsString(Map.of("jobs", found)));
        } else if (path.length == 2 && "GET".equals(method)) {
            BatchJob job = batchJobs.get(path[1]);
            if (job == null) {
                reply(exchange, 404, "{}");
                return;
            }
            reply(exchange, 200, mapper.writeValueAsString(view(path[1], job)));
        } else if (path.length == 3 && "status".equals(path[2]) && "PUT".equals(method)) {
            BatchJob job = batchJobs.get(path[1]);
            if (job == null) {
                reply(exchange, 404, "{}");
                return;
            }
            job.status = mapper.readTree(exchange.getRequestBody().readAllBytes()).path("status").asString();
            reply(exchange, 200, mapper.writeValueAsString(view(path[1], job)));
        } else {
            reply(exchange, 404, "{}");
        }
    }

    private static Map<String, String> view(String jobId, BatchJob job) {
        return Map.of("jobId", jobId, "userId", job.userId, "status", job.status);
    }

    /** Alle Werte eines Query-Parameters, z. B. {@code ?taskIds=a&taskIds=b}. */
    private static List<String> queryParams(HttpExchange exchange, String name) {
        String query = exchange.getRequestURI().getRawQuery();
        List<String> values = new ArrayList<>();
        if (query == null) {
            return values;
        }
        for (String pair : query.split("&")) {
            if (pair.startsWith(name + "=")) {
                values.add(URLDecoder.decode(pair.substring(name.length() + 1), StandardCharsets.UTF_8));
            }
        }
        return values;
    }

    private static String statusOf(Job job, boolean released) {
        if (job.content().contains("SLOW")) {
            return released ? "SUCCESS" : "PENDING";
        }
        int poll;
        synchronized (job) {
            poll = ++job.polls()[0];
        }
        if (poll < 2) {
            return "PENDING";
        }
        return job.content().contains("FAIL") ? "ERROR" : "SUCCESS";
    }

    private static void reply(HttpExchange exchange, int status, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
