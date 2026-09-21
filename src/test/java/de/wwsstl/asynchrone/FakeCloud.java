package de.wwsstl.asynchrone;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Lokaler HTTP-Server, der die beiden Cloud-Dienste nachbildet. Das Verhalten wird über den Dateiinhalt
 * gesteuert:
 * <ul>
 *   <li>Inhalt enthält {@code FAIL} → Cloud meldet {@code ERROR}</li>
 *   <li>Inhalt enthält {@code SLOW} → Cloud meldet dauerhaft {@code PENDING}</li>
 *   <li>sonst → {@code PENDING} bei der ersten Abfrage, danach {@code SUCCESS}</li>
 * </ul>
 */
public final class FakeCloud implements AutoCloseable {

    /** Ein an Cloud-API 2 gerichteter Bulk-Aufruf: die abgefragten Dateinamen. */
    public record StatusCall(List<String> fileNames) {
    }

    private record Job(String fileName, String content, int[] polls) {
    }

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final HttpServer server;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final List<Integer> submitBatchSizes = new CopyOnWriteArrayList<>();
    private final List<StatusCall> statusCalls = new CopyOnWriteArrayList<>();

    public FakeCloud() {
        try {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/tasks/status", this::status);
        server.createContext("/tasks", this::submit);
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

    private void submit(HttpExchange exchange) throws IOException {
        if (!"/tasks".equals(exchange.getRequestURI().getPath())) {
            reply(exchange, 404, "{}");
            return;
        }
        JsonNode items = mapper.readTree(exchange.getRequestBody().readAllBytes()).get("items");
        submitBatchSizes.add(items.size());
        List<Map<String, String>> tasks = new ArrayList<>();
        for (JsonNode item : items) {
            String taskId = UUID.randomUUID().toString();
            String fileName = item.get("fileName").asString();
            jobs.put(taskId, new Job(fileName, item.get("content").asString(), new int[1]));
            tasks.add(Map.of("fileName", fileName, "taskId", taskId));
        }
        reply(exchange, 200, mapper.writeValueAsString(Map.of("tasks", tasks)));
    }

    private void status(HttpExchange exchange) throws IOException {
        JsonNode ids = mapper.readTree(exchange.getRequestBody().readAllBytes()).get("taskIds");
        List<String> fileNames = new ArrayList<>();
        List<Map<String, String>> results = new ArrayList<>();
        for (JsonNode id : ids) {
            Job job = jobs.get(id.asString());
            fileNames.add(job.fileName());
            results.add(Map.of("taskId", id.asString(), "status", statusOf(job)));
        }
        statusCalls.add(new StatusCall(Collections.unmodifiableList(fileNames)));
        reply(exchange, 200, mapper.writeValueAsString(Map.of("results", results)));
    }

    private static String statusOf(Job job) {
        if (job.content().contains("SLOW")) {
            return "PENDING";
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
