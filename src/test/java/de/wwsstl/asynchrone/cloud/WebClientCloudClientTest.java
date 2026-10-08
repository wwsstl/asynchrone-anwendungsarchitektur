package de.wwsstl.asynchrone.cloud;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import de.wwsstl.asynchrone.FakeCloud;
import de.wwsstl.asynchrone.FakeCloud.SubmitBehavior;
import de.wwsstl.asynchrone.TestProperties;
import de.wwsstl.asynchrone.config.PipelineProperties;

/** Der Vertrag mit den Cloud-Diensten und die Einordnung der Fehler von Cloud-API 1. */
class WebClientCloudClientTest {

    private static final FakeCloud cloud = new FakeCloud();

    private final CloudClient client = client(URI.create(cloud.baseUrl()), TestProperties.cloud(URI.create(cloud.baseUrl())));

    @AfterAll
    static void stop() {
        cloud.close();
    }

    @Test
    void submitReturnsTaskIdsOfConvertedFilesOnly() throws Exception {
        String jobId = get(client.createBatchJob("conv"));

        List<SubmittedFile> submitted = get(client.submit(jobId,
                List.of(new TestdataItem("a.json", "ok"), new TestdataItem("b.json", "REJECT"))));

        assertThat(submitted).extracting(SubmittedFile::fileName).containsExactly("a.json");
    }

    @Test
    void statusAndJobFollowTheCloud() throws Exception {
        String jobId = get(client.createBatchJob("flow"));
        TaskId taskId = get(client.submit(jobId, List.of(new TestdataItem("a.json", "ok")))).getFirst().taskId();

        assertThat(get(client.queryStatus(List.of(taskId))))
                .containsExactly(new RecordStatusResult(taskId, RecordStatus.PENDING));
        assertThat(get(client.queryStatus(List.of(taskId))))
                .containsExactly(new RecordStatusResult(taskId, RecordStatus.SUCCESS));

        get(client.setJobStatus(jobId, JobStatus.COMPLETED));

        assertThat(get(client.batchJob(jobId)))
                .contains(new BatchJob(jobId, "flow", JobStatus.COMPLETED, 1, 0, 0));
        assertThat(get(client.batchJob("BJ-unknown"))).isEqualTo(Optional.empty());
    }

    @Test
    void rejectedOrUnavailableSubmitWasNotProcessed() throws Exception {
        String jobId = get(client.createBatchJob("rejected"));
        cloud.nextSubmitsFor("rejected", new SubmitBehavior.Respond(503), new SubmitBehavior.Respond(400));

        assertSubmitFails(client, jobId, SubmitFailedException.Kind.NOT_PROCESSED);
        assertSubmitFails(client, jobId, SubmitFailedException.Kind.NOT_PROCESSED);
    }

    @Test
    void serverErrorOrTimeoutMakesSubmitResultUnclear() throws Exception {
        String jobId = get(client.createBatchJob("unclear"));
        cloud.nextSubmitsFor("unclear", new SubmitBehavior.Respond(500),
                new SubmitBehavior.Delay(Duration.ofSeconds(2)));

        assertSubmitFails(client, jobId, SubmitFailedException.Kind.UNKNOWN);
        assertSubmitFails(client, jobId, SubmitFailedException.Kind.UNKNOWN);
        // Der verzögerte Batch wurde trotzdem verarbeitet: genau deshalb darf er nicht wiederholt werden.
        assertThat(cloud.submittedFileNames()).contains("probe.json");
    }

    @Test
    void refusedConnectionMeansNotProcessed() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        URI unreachable = URI.create("http://127.0.0.1:" + closedPort);
        PipelineProperties.Cloud base = TestProperties.cloud(unreachable);
        CloudClient offline = client(unreachable, new PipelineProperties.Cloud(unreachable, base.jobPath(),
                base.submitPath(), base.statusPath(), Duration.ofSeconds(20), base.statusTimeout(), 0));

        assertSubmitFails(offline, "BJ-1", SubmitFailedException.Kind.NOT_PROCESSED);
        assertFails(offline.createBatchJob("anna"), SubmitFailedException.Kind.NOT_PROCESSED);
    }

    @Test
    void createJobWithoutResponseIsUnclear() {
        cloud.delayJobCreationFor("slowjob", Duration.ofSeconds(2));

        assertFails(client.createBatchJob("slowjob"), SubmitFailedException.Kind.UNKNOWN);
        // Der BatchgenAuftrag wurde trotzdem angelegt: genau deshalb ist das Ergebnis unklar.
        assertThat(cloud.jobsOf("slowjob")).hasSize(1);
    }

    @Test
    void failedEndStateWriteIsReportedAfterRetries() throws Exception {
        String jobId = get(client.createBatchJob("nowrite"));
        cloud.failJobStatusWrites(true);
        try {
            assertThatThrownBy(() -> get(client.setJobStatus(jobId, JobStatus.ERROR)))
                    .isInstanceOf(ExecutionException.class);
        } finally {
            cloud.failJobStatusWrites(false);
        }
        assertThat(cloud.jobStatus(jobId)).isEqualTo(JobStatus.RUNNING);
    }

    private static void assertSubmitFails(CloudClient client, String jobId, SubmitFailedException.Kind kind) {
        assertFails(client.submit(jobId, List.of(new TestdataItem("probe.json", "ok"))), kind);
    }

    private static void assertFails(CompletableFuture<?> call, SubmitFailedException.Kind kind) {
        assertThatThrownBy(() -> get(call))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(SubmitFailedException.class, e -> assertThat(e.kind()).isEqualTo(kind));
    }

    private static CloudClient client(URI baseUrl, PipelineProperties.Cloud config) {
        return new WebClientCloudClient(WebClient.builder().baseUrl(baseUrl.toString()).build(), config);
    }

    private static <T> T get(CompletableFuture<T> future) throws Exception {
        return future.get(30, TimeUnit.SECONDS);
    }
}
