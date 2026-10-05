package de.wwsstl.asynchrone.cloud;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.pool.TaskId;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * {@link CloudClient} auf Basis von {@link WebClient}. JSON (Jackson 3) wird über die Codecs von Spring erzeugt
 * und gelesen.
 *
 * <p>Angenommener Vertrag der Cloud-Dienste (die Spezifikation liegt nicht vor):
 * <ul>
 *   <li>API 1, BatchgenAuftrag anlegen: {@code POST job-path} mit {@code {"userId"}} → {@code {"jobId"}}</li>
 *   <li>API 1, Batch übermitteln: {@code POST submit-path} mit
 *       {@code {"jobId","items":[{"fileName","content"}]}} → {@code {"tasks":[{"fileName","taskId"}]}}</li>
 *   <li>API 2: {@code GET status-path?taskIds=…&taskIds=…} →
 *       {@code {"results":[{"taskId","status":"SUCCESS|ERROR|PENDING"}]}}</li>
 *   <li>API 3, laufenden BatchgenAuftrag suchen: {@code GET job-path?userId=…&status=RUNNING} →
 *       {@code {"jobs":[{"jobId","userId","status"}]}}</li>
 *   <li>API 3, BatchgenAuftrag lesen: {@code GET job-path/{jobId}} → {@code {"jobId","userId","status"}};
 *       {@code 404}, wenn es ihn nicht gibt</li>
 *   <li>API 3, Status setzen: {@code PUT job-path/{jobId}/status} mit
 *       {@code {"status":"RUNNING|COMPLETED|CANCELLED"}}</li>
 * </ul>
 * Anlegen und Übermitteln sind nicht idempotent und verwenden {@code submit-timeout}/{@code submit-retries}; die
 * Aufrufe von API 3 verwenden wie API 2 {@code status-timeout}/{@code status-retries}.
 */
public class WebClientCloudClient implements CloudClient {

    private static final Duration RETRY_BACKOFF = Duration.ofMillis(500);

    private final WebClient webClient;
    private final PipelineProperties.Cloud config;

    public WebClientCloudClient(WebClient webClient, PipelineProperties.Cloud config) {
        this.webClient = webClient;
        this.config = config;
    }

    @Override
    public CompletableFuture<BatchJobId> createBatchJob(String userId) {
        Mono<JobCreated> call = webClient.post()
                .uri(config.jobPath())
                .bodyValue(new CreateJobRequest(userId))
                .retrieve()
                .bodyToMono(JobCreated.class)
                .timeout(config.submitTimeout());
        return withRetry(call, config.submitRetries())
                .map(response -> new BatchJobId(response.jobId()))
                .toFuture();
    }

    @Override
    public CompletableFuture<List<SubmittedTask>> submit(BatchJobId job, List<TestdataItem> items) {
        Mono<SubmitResponse> call = webClient.post()
                .uri(config.submitPath())
                .bodyValue(new SubmitRequest(job.value(), items))
                .retrieve()
                .bodyToMono(SubmitResponse.class)
                .timeout(config.submitTimeout());
        return withRetry(call, config.submitRetries())
                .map(response -> response.tasks() == null ? List.<SubmittedTask>of() : response.tasks().stream()
                        .map(entry -> new SubmittedTask(entry.fileName(), new TaskId(entry.taskId())))
                        .toList())
                .toFuture();
    }

    @Override
    public CompletableFuture<List<TaskStatusResult>> queryStatus(List<TaskId> taskIds) {
        Mono<StatusResponse> call = webClient.get()
                .uri(builder -> builder.path(config.statusPath())
                        .queryParam("taskIds", taskIds.stream().map(TaskId::value).toArray())
                        .build())
                .retrieve()
                .bodyToMono(StatusResponse.class)
                .timeout(config.statusTimeout());
        return withRetry(call, config.statusRetries())
                .map(response -> response.results() == null ? List.<TaskStatusResult>of() : response.results().stream()
                        .map(entry -> new TaskStatusResult(new TaskId(entry.taskId()), entry.status()))
                        .toList())
                .toFuture();
    }

    @Override
    public CompletableFuture<Optional<BatchJobId>> findRunningBatchJob(String userId) {
        Mono<JobList> call = webClient.get()
                .uri(builder -> builder.path(config.jobPath())
                        .queryParam("userId", userId)
                        .queryParam("status", BatchJobStatus.RUNNING)
                        .build())
                .retrieve()
                .bodyToMono(JobList.class)
                .timeout(config.statusTimeout());
        return withRetry(call, config.statusRetries())
                .map(response -> response.jobs() == null ? Optional.<BatchJobId>empty() : response.jobs().stream()
                        .filter(entry -> userId.equals(entry.userId()) && entry.status() == BatchJobStatus.RUNNING)
                        .map(entry -> new BatchJobId(entry.jobId()))
                        .findFirst())
                .toFuture();
    }

    @Override
    public CompletableFuture<Optional<BatchJob>> batchJob(BatchJobId job) {
        Mono<JobEntry> call = webClient.get()
                .uri(config.jobPath() + "/{jobId}", job.value())
                .retrieve()
                .bodyToMono(JobEntry.class)
                .timeout(config.statusTimeout());
        return withRetry(call, config.statusRetries())
                .map(entry -> Optional.of(new BatchJob(new BatchJobId(entry.jobId()), entry.userId(), entry.status())))
                .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.just(Optional.empty()))
                .toFuture();
    }

    @Override
    public CompletableFuture<Void> setBatchJobStatus(BatchJobId job, BatchJobStatus status) {
        Mono<Void> call = webClient.put()
                .uri(config.jobPath() + "/{jobId}/status", job.value())
                .bodyValue(new JobStatusRequest(status))
                .retrieve()
                .toBodilessEntity()
                .timeout(config.statusTimeout())
                .then();
        return withRetry(call, config.statusRetries()).toFuture();
    }

    private static <T> Mono<T> withRetry(Mono<T> call, int retries) {
        if (retries == 0) {
            return call;
        }
        return call.retryWhen(Retry.backoff(retries, RETRY_BACKOFF)
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

    record JobCreated(String jobId) {
    }

    record SubmitRequest(String jobId, List<TestdataItem> items) {
    }

    record SubmitResponse(List<SubmitEntry> tasks) {
    }

    record SubmitEntry(String fileName, String taskId) {
    }

    record StatusResponse(List<StatusEntry> results) {
    }

    record StatusEntry(String taskId, CloudStatus status) {
    }

    record JobList(List<JobEntry> jobs) {
    }

    record JobEntry(String jobId, String userId, BatchJobStatus status) {
    }

    record JobStatusRequest(BatchJobStatus status) {
    }
}
