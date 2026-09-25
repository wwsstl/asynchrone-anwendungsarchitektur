package de.wwsstl.asynchrone.cloud;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
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
 *   <li>API 1: {@code POST submit-path} mit {@code {"items":[{"fileName","content"}]}} →
 *       {@code {"tasks":[{"fileName","taskId"}]}}</li>
 *   <li>API 2: {@code GET status-path?taskIds=…&taskIds=…} →
 *       {@code {"results":[{"taskId","status":"SUCCESS|ERROR|PENDING"}]}}</li>
 * </ul>
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
    public CompletableFuture<List<SubmittedTask>> submit(List<TestdataItem> items) {
        Mono<SubmitResponse> call = webClient.post()
                .uri(config.submitPath())
                .bodyValue(new SubmitRequest(items))
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

    record SubmitRequest(List<TestdataItem> items) {
    }

    record SubmitResponse(List<SubmitEntry> tasks) {
    }

    record SubmitEntry(String fileName, String taskId) {
    }

    record StatusResponse(List<StatusEntry> results) {
    }

    record StatusEntry(String taskId, CloudStatus status) {
    }
}
