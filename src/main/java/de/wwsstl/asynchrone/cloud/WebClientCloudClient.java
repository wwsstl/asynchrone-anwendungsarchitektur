package de.wwsstl.asynchrone.cloud;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import de.wwsstl.asynchrone.config.PipelineProperties;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * {@link CloudClient} auf Basis des nicht-blockierenden {@link WebClient}; JSON erzeugt und liest Jackson.
 *
 * <p>Angenommener Vertrag der Cloud-Dienste (die Pfade sind über {@code pipeline.cloud.*} einstellbar):
 * <ul>
 *   <li>Cloud-API 1, BatchgenAuftrag anlegen: {@code POST job-path} mit {@code {"userId"}} → {@code {"jobId"}}</li>
 *   <li>Cloud-API 1, Batch übermitteln: {@code POST submit-path} mit
 *       {@code {"jobId","items":[{"fileName","content"}]}} → {@code {"tasks":[{"fileName","taskId"}]}};
 *       Dateien ohne Eintrag in {@code tasks} wurden nicht umgewandelt</li>
 *   <li>Cloud-API 2: {@code POST status-path} mit {@code {"taskIds":[…]}} →
 *       {@code {"results":[{"taskId","status":"SUCCESS|ERROR|PENDING"}]}}</li>
 *   <li>Cloud-API 3, Endzustand setzen: {@code PUT job-path/{jobId}/status} mit {@code {"status"}}</li>
 *   <li>Cloud-API 3, BatchgenAuftrag lesen: {@code GET job-path/{jobId}} →
 *       {@code {"jobId","userId","status","counts":{"SUCCESS":n,"ERROR":n,"PENDING":n}}}; {@code 404}, wenn es ihn
 *       nicht gibt</li>
 * </ul>
 *
 * <p>Cloud-API 1 ist nicht idempotent und wird nie wiederholt. Die Aufrufe von Cloud-API 2 und 3 werden bei
 * vorübergehenden Fehlern bis zu {@code pipeline.cloud.retries} Mal wiederholt.
 */
public class WebClientCloudClient implements CloudClient {

    private static final Duration RETRY_BACKOFF = Duration.ofMillis(200);

    private final WebClient webClient;
    private final PipelineProperties.Cloud config;

    public WebClientCloudClient(WebClient webClient, PipelineProperties.Cloud config) {
        this.webClient = webClient;
        this.config = config;
    }

    @Override
    public CompletableFuture<String> createBatchJob(String userId) {
        return webClient.post()
                .uri(config.jobPath())
                .bodyValue(new CreateJobRequest(userId))
                .retrieve()
                .bodyToMono(CreateJobResponse.class)
                .timeout(config.submitTimeout())
                .map(response -> Objects.requireNonNull(response.jobId(), "Cloud-API 1 lieferte keine jobId"))
                .onErrorMap(error -> new SubmitFailedException(classify(error), error))
                .toFuture();
    }

    @Override
    public CompletableFuture<List<SubmittedFile>> submit(String jobId, List<TestdataItem> items) {
        return webClient.post()
                .uri(config.submitPath())
                .bodyValue(new SubmitRequest(jobId, items))
                .retrieve()
                .bodyToMono(SubmitResponse.class)
                .timeout(config.submitTimeout())
                .map(response -> response.tasks() == null ? List.<SubmittedFile>of() : response.tasks().stream()
                        .filter(entry -> entry.fileName() != null && entry.taskId() != null && !entry.taskId().isBlank())
                        .map(entry -> new SubmittedFile(entry.fileName(), new TaskId(entry.taskId())))
                        .toList())
                .onErrorMap(error -> new SubmitFailedException(classify(error), error))
                .toFuture();
    }

    @Override
    public CompletableFuture<List<RecordStatusResult>> queryStatus(List<TaskId> taskIds) {
        Mono<StatusResponse> call = webClient.post()
                .uri(config.statusPath())
                .bodyValue(new StatusRequest(taskIds.stream().map(TaskId::value).toList()))
                .retrieve()
                .bodyToMono(StatusResponse.class)
                .timeout(config.statusTimeout());
        return withRetry(call)
                .map(response -> response.results() == null ? List.<RecordStatusResult>of() : response.results().stream()
                        .filter(entry -> entry.taskId() != null && entry.status() != null)
                        .map(entry -> new RecordStatusResult(new TaskId(entry.taskId()), entry.status()))
                        .toList())
                .toFuture();
    }

    @Override
    public CompletableFuture<Void> setJobStatus(String jobId, JobStatus status) {
        Mono<Void> call = webClient.put()
                .uri(config.jobPath() + "/{jobId}/status", jobId)
                .bodyValue(new JobStatusRequest(status))
                .retrieve()
                .toBodilessEntity()
                .timeout(config.statusTimeout())
                .then();
        return withRetry(call).toFuture();
    }

    @Override
    public CompletableFuture<Optional<BatchJob>> batchJob(String jobId) {
        Mono<JobResponse> call = webClient.get()
                .uri(config.jobPath() + "/{jobId}", jobId)
                .retrieve()
                .bodyToMono(JobResponse.class)
                .timeout(config.statusTimeout());
        return withRetry(call)
                .map(response -> Optional.of(toBatchJob(response)))
                .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.just(Optional.empty()))
                .toFuture();
    }

    private static BatchJob toBatchJob(JobResponse response) {
        Map<RecordStatus, Integer> counts = response.counts() == null ? Map.of() : response.counts();
        return new BatchJob(response.jobId(), response.userId(), response.status(),
                counts.getOrDefault(RecordStatus.SUCCESS, 0), counts.getOrDefault(RecordStatus.ERROR, 0),
                counts.getOrDefault(RecordStatus.PENDING, 0));
    }

    /**
     * Eindeutig nicht verarbeitet ist eine Anfrage an Cloud-API 1 nur, wenn die Verbindung gar nicht zustande kam oder
     * Cloud-API 1 sie abgelehnt hat (4xx, 503). In allen anderen Fällen (Timeout, abgebrochene Verbindung, sonstige
     * 5xx, unlesbare Antwort) kann Cloud-API 1 den BatchgenAuftrag bzw. den Batch bereits angelegt haben.
     */
    static SubmitFailedException.Kind classify(Throwable error) {
        if (error instanceof WebClientResponseException response) {
            boolean rejected = response.getStatusCode().is4xxClientError()
                    || response.getStatusCode().value() == HttpStatus.SERVICE_UNAVAILABLE.value();
            return rejected ? SubmitFailedException.Kind.NOT_PROCESSED : SubmitFailedException.Kind.UNKNOWN;
        }
        if (error instanceof WebClientRequestException && isConnectFailure(error)) {
            return SubmitFailedException.Kind.NOT_PROCESSED;
        }
        return SubmitFailedException.Kind.UNKNOWN;
    }

    private static boolean isConnectFailure(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof ConnectException || cause instanceof UnknownHostException
                    || cause instanceof NoRouteToHostException) {
                return true;
            }
        }
        return false;
    }

    private <T> Mono<T> withRetry(Mono<T> call) {
        if (config.retries() == 0) {
            return call;
        }
        return call.retryWhen(Retry.backoff(config.retries(), RETRY_BACKOFF)
                .filter(WebClientCloudClient::isTransient)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure()));
    }

    /** Nur Netzwerk-, Timeout- und 5xx-Fehler lohnen eine Wiederholung. */
    private static boolean isTransient(Throwable error) {
        if (error instanceof WebClientResponseException response) {
            return response.getStatusCode().is5xxServerError();
        }
        return error instanceof WebClientRequestException || error instanceof TimeoutException
                || error instanceof IOException;
    }

    record CreateJobRequest(String userId) {
    }

    record CreateJobResponse(String jobId) {
    }

    record SubmitRequest(String jobId, List<TestdataItem> items) {
    }

    record SubmitResponse(List<SubmitEntry> tasks) {
    }

    record SubmitEntry(String fileName, String taskId) {
    }

    record StatusRequest(List<String> taskIds) {
    }

    record StatusResponse(List<StatusEntry> results) {
    }

    record StatusEntry(String taskId, RecordStatus status) {
    }

    record JobStatusRequest(JobStatus status) {
    }

    record JobResponse(String jobId, String userId, JobStatus status, Map<RecordStatus, Integer> counts) {
    }
}
