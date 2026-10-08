package de.wwsstl.asynchrone;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import de.wwsstl.asynchrone.cloud.JobStatus;
import de.wwsstl.asynchrone.cloud.RecordStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Lokaler HTTP-Server, der die Cloud-Dienste nach dem Vertrag von {@code WebClientCloudClient} nachbildet. Das
 * Verhalten eines Datensatzes steuert der Dateiinhalt:
 * <ul>
 *   <li>{@code REJECT}: Cloud-API 1 wandelt die Datei nicht um (keine TaskId)</li>
 *   <li>{@code FAIL}: Cloud-API 2 meldet beim ersten Mal {@code PENDING}, danach {@code ERROR}</li>
 *   <li>{@code SLOW}: {@code PENDING}, bis die Datei mit {@link #release} freigegeben wird, danach {@code SUCCESS}</li>
 *   <li>sonst: beim ersten Mal {@code PENDING}, danach {@code SUCCESS}</li>
 * </ul>
 * Mit {@link #nextSubmitsFor} lassen sich Fehler von Cloud-API 1 für eine Benutzer:in vorgeben.
 */
public final class FakeCloud implements AutoCloseable {

    /** Vorgegebenes Verhalten des nächsten Aufrufs von Cloud-API 1 (Batch übermitteln). */
    public sealed interface SubmitBehavior {
        /** Antwortet mit diesem HTTP-Status, ohne den Batch zu verarbeiten. */
        record Respond(int status) implements SubmitBehavior {
        }

        /** Verarbeitet den Batch, antwortet aber erst nach dieser Verzögerung. */
        record Delay(Duration delay) implements SubmitBehavior {
        }
    }

    /** Ein Aufruf von Cloud-API 2: die abgefragten Dateinamen. */
    public record StatusCall(List<String> fileNames) {
    }

    private static final class Job {
        private final String userId;
        private volatile JobStatus status = JobStatus.RUNNING;

        Job(String userId) {
            this.userId = userId;
        }
    }

    private static final class DataRecord {
        private final String jobId;
        private final String fileName;
        private final String content;
        private final AtomicInteger polls = new AtomicInteger();
        private volatile RecordStatus status = RecordStatus.PENDING;

        DataRecord(String jobId, String fileName, String content) {
            this.jobId = jobId;
            this.fileName = fileName;
            this.content = content;
        }
    }

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final HttpServer server;
    private final AtomicInteger jobSequence = new AtomicInteger();
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final Map<String, DataRecord> records = new ConcurrentHashMap<>();
    private final Map<String, Queue<SubmitBehavior>> submitBehaviors = new ConcurrentHashMap<>();
    private final Set<String> unavailableUsers = ConcurrentHashMap.newKeySet();
    private final Map<String, Duration> jobCreationDelays = new ConcurrentHashMap<>();
    private final Set<String> released = ConcurrentHashMap.newKeySet();
    private final List<String> submittedFileNames = new CopyOnWriteArrayList<>();
    private final List<Integer> submitBatchSizes = new CopyOnWriteArrayList<>();
    private final List<StatusCall> statusCalls = new CopyOnWriteArrayList<>();
    private final AtomicBoolean jobStatusWritesFail = new AtomicBoolean();

    public FakeCloud() {
        try {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/jobs", this::jobs);
        server.createContext("/tasks/status", this::status);
        server.createContext("/tasks", this::submit);
        server.start();
    }

    public String baseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    // --- Steuerung und Auswertung für Tests ------------------------------------------------------------------

    /** Lässt die {@code SLOW}-Datensätze dieser Dateien ab der nächsten Abfrage mit {@code SUCCESS} enden. */
    public void release(Collection<String> fileNames) {
        released.addAll(fileNames);
    }

    /** Cloud-API 1 (BatchgenAuftrag anlegen) antwortet für diese Benutzer:in mit 503. */
    public void makeUnavailableFor(String userId) {
        unavailableUsers.add(userId);
    }

    /** Cloud-API 1 (BatchgenAuftrag anlegen) legt ihn für diese Benutzer:in an, antwortet aber erst verzögert. */
    public void delayJobCreationFor(String userId, Duration delay) {
        jobCreationDelays.put(userId, delay);
    }

    /** Die nächsten Aufrufe von Cloud-API 1 (Batch übermitteln) dieser Benutzer:in verhalten sich wie angegeben. */
    public void nextSubmitsFor(String userId, SubmitBehavior... behaviors) {
        submitBehaviors.computeIfAbsent(userId, user -> new ConcurrentLinkedQueue<>()).addAll(List.of(behaviors));
    }

    /** Cloud-API 3 (Endzustand setzen) antwortet mit 503. */
    public void failJobStatusWrites(boolean fail) {
        jobStatusWritesFail.set(fail);
    }

    /** Status des BatchgenAuftrags; {@code null}, wenn es ihn nicht gibt. */
    public JobStatus jobStatus(String jobId) {
        Job job = jobs.get(jobId);
        return job == null ? null : job.status;
    }

    /** Die Nummern aller BatchgenAufträge der Benutzer:in. */
    public List<String> jobsOf(String userId) {
        return jobs.entrySet().stream().filter(entry -> entry.getValue().userId.equals(userId))
                .map(Map.Entry::getKey).sorted().toList();
    }

    /** Alle von Cloud-API 1 verarbeiteten Dateinamen in Aufrufreihenfolge (Duplikate = doppelte Übermittlung). */
    public List<String> submittedFileNames() {
        return List.copyOf(submittedFileNames);
    }

    public List<Integer> submitBatchSizes() {
        return List.copyOf(submitBatchSizes);
    }

    public List<StatusCall> statusCalls() {
        return List.copyOf(statusCalls);
    }

    // --- Cloud-API 1 und 3: BatchgenAufträge -----------------------------------------------------------------

    private void jobs(HttpExchange exchange) throws IOException {
        String[] path = exchange.getRequestURI().getPath().substring(1).split("/");
        String method = exchange.getRequestMethod();
        if (path.length == 1 && "POST".equals(method)) {
            String userId = mapper.readTree(exchange.getRequestBody().readAllBytes()).path("userId").asString("");
            if (unavailableUsers.contains(userId)) {
                reply(exchange, 503, Map.of());
                return;
            }
            String jobId = "BJ-" + jobSequence.incrementAndGet();
            jobs.put(jobId, new Job(userId));
            Duration delay = jobCreationDelays.get(userId);
            if (delay != null) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            reply(exchange, 200, Map.of("jobId", jobId));
            return;
        }
        Job job = path.length >= 2 ? jobs.get(path[1]) : null;
        if (job == null) {
            reply(exchange, 404, Map.of());
        } else if (path.length == 2 && "GET".equals(method)) {
            reply(exchange, 200, view(path[1], job));
        } else if (path.length == 3 && "status".equals(path[2]) && "PUT".equals(method)) {
            if (jobStatusWritesFail.get()) {
                reply(exchange, 503, Map.of());
                return;
            }
            String status = mapper.readTree(exchange.getRequestBody().readAllBytes()).path("status").asString();
            job.status = JobStatus.valueOf(status);
            reply(exchange, 200, view(path[1], job));
        } else {
            reply(exchange, 404, Map.of());
        }
    }

    private Map<String, Object> view(String jobId, Job job) {
        Map<RecordStatus, Integer> counts = new EnumMap<>(RecordStatus.class);
        for (RecordStatus status : RecordStatus.values()) {
            counts.put(status, 0);
        }
        records.values().stream().filter(record -> record.jobId.equals(jobId))
                .forEach(record -> counts.merge(record.status, 1, Integer::sum));
        return Map.of("jobId", jobId, "userId", job.userId, "status", job.status.name(), "counts", counts);
    }

    // --- Cloud-API 1: Batch übermitteln ----------------------------------------------------------------------

    private void submit(HttpExchange exchange) throws IOException {
        if (!"/tasks".equals(exchange.getRequestURI().getPath()) || !"POST".equals(exchange.getRequestMethod())) {
            reply(exchange, 404, Map.of());
            return;
        }
        JsonNode request = mapper.readTree(exchange.getRequestBody().readAllBytes());
        String jobId = request.path("jobId").asString("");
        Job job = jobs.get(jobId);
        if (job == null) {
            reply(exchange, 400, Map.of("error", "unbekannter BatchgenAuftrag " + jobId));
            return;
        }
        SubmitBehavior behavior = nextBehavior(job.userId);
        if (behavior instanceof SubmitBehavior.Respond respond) {
            reply(exchange, respond.status(), Map.of());
            return;
        }

        JsonNode items = request.path("items");
        submitBatchSizes.add(items.size());
        List<Map<String, String>> tasks = new ArrayList<>();
        for (JsonNode item : items) {
            String fileName = item.path("fileName").asString();
            String content = item.path("content").asString();
            submittedFileNames.add(fileName);
            if (content.contains("REJECT")) {
                continue;
            }
            String taskId = UUID.randomUUID().toString();
            records.put(taskId, new DataRecord(jobId, fileName, content));
            tasks.add(Map.of("fileName", fileName, "taskId", taskId));
        }
        if (behavior instanceof SubmitBehavior.Delay delay) {
            try {
                Thread.sleep(delay.delay());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        reply(exchange, 200, Map.of("tasks", tasks));
    }

    private SubmitBehavior nextBehavior(String userId) {
        Queue<SubmitBehavior> queue = submitBehaviors.get(userId);
        return queue == null ? null : queue.poll();
    }

    // --- Cloud-API 2: Bulk-Statusabfrage ---------------------------------------------------------------------

    private void status(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            reply(exchange, 405, Map.of());
            return;
        }
        JsonNode taskIds = mapper.readTree(exchange.getRequestBody().readAllBytes()).path("taskIds");
        List<String> fileNames = new ArrayList<>();
        List<Map<String, String>> results = new ArrayList<>();
        for (JsonNode node : taskIds) {
            String taskId = node.asString();
            DataRecord record = records.get(taskId);
            if (record == null) {
                continue;
            }
            fileNames.add(record.fileName);
            results.add(Map.of("taskId", taskId, "status", advance(record).name()));
        }
        statusCalls.add(new StatusCall(List.copyOf(fileNames)));
        reply(exchange, 200, Map.of("results", results));
    }

    /** Die Cloud aktualisiert den Status ihrer Datensätze selbst; hier geschieht das bei jeder Abfrage. */
    private RecordStatus advance(DataRecord record) {
        int poll = record.polls.incrementAndGet();
        if (record.content.contains("SLOW")) {
            record.status = released.contains(record.fileName) ? RecordStatus.SUCCESS : RecordStatus.PENDING;
        } else if (poll >= 2) {
            record.status = record.content.contains("FAIL") ? RecordStatus.ERROR : RecordStatus.SUCCESS;
        }
        return record.status;
    }

    private void reply(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] json = mapper.writeValueAsString(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, json.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(json);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
