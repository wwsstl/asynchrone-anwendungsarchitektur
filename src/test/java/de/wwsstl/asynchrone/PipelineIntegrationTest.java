package de.wwsstl.asynchrone;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import de.wwsstl.asynchrone.FakeCloud.SubmitBehavior;
import de.wwsstl.asynchrone.cloud.JobStatus;
import de.wwsstl.asynchrone.task.TaskSnapshot;
import de.wwsstl.asynchrone.task.TaskState;
import de.wwsstl.asynchrone.task.TaskStatus;

/** Die Abläufe aus funktionsweise_sequenz.md, Ende zu Ende über die REST-API. */
class PipelineIntegrationTest extends PipelineTestSupport {

    // --- 1. Start und 6. Abschluss ---------------------------------------------------------------------------

    @Test
    void completesAllFilesAndWritesCompleted() {
        api.dropFiles("happy", 10, "ok");

        PipelineApi.Response response = api.startResponse("happy");

        assertThat(response.status()).isEqualTo(202);
        String taskNumber = (String) response.body().get("taskNumber");
        assertThat(cloud.jobsOf("happy")).containsExactly(taskNumber);
        assertThat(response.location()).isEqualTo("/api/users/happy/tasks/" + taskNumber);
        assertThat(response.body().get("state")).isEqualTo("RUNNING");

        TaskStatus status = awaitEnd("happy", taskNumber, JobStatus.COMPLETED);

        assertThat(status.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 10, 0, 0));
        assertThat(api.names("happy", "donebox")).hasSize(10);
        assertThat(api.names("happy", "inbox")).isEmpty();
        assertThat(api.names("happy", "pendingbox")).isEmpty();
        assertThat(api.markers("happy")).isEmpty();
        assertThat(api.names("happy", "errorbox")).isEmpty();
        assertThat(submittedBy("happy")).hasSize(10).doesNotHaveDuplicates();
        assertThat(cloud.submitBatchSizes()).allSatisfy(size -> assertThat(size).isBetween(1, 4));
    }

    @Test
    void rejectedAndFailedFilesGoToErrorbox() {
        api.drop("mixed", "mixed-1.json", "ok");
        api.drop("mixed", "mixed-2.json", "ok");
        api.drop("mixed", "mixed-3.json", "ok");
        api.drop("mixed", "mixed-4.json", "REJECT");
        api.drop("mixed", "mixed-5.json", "FAIL");

        String taskNumber = api.start("mixed").taskNumber();
        TaskStatus status = awaitEnd("mixed", taskNumber, JobStatus.COMPLETED);

        // Die nicht umgewandelte Datei ist kein Datensatz in der Cloud.
        assertThat(status.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 3, 1, 0));
        assertThat(api.names("mixed", "donebox")).containsExactly("mixed-1.json", "mixed-2.json", "mixed-3.json");
        assertThat(api.names("mixed", "errorbox")).containsExactly("mixed-4.json", "mixed-5.json");
        assertThat(api.names("mixed", "pendingbox")).isEmpty();
        assertThat(api.markers("mixed")).isEmpty();
    }

    @Test
    void secondStartOfSameUserIsRejectedWhileRunning() {
        api.drop("busy", "busy-1.json", "SLOW");
        String taskNumber = api.start("busy").taskNumber();

        PipelineApi.Response second = api.startResponse("busy");

        assertThat(second.status()).isEqualTo(409);
        assertThat(second.code()).isEqualTo("USER_TASK_RUNNING");
        assertThat(cloud.jobsOf("busy")).containsExactly(taskNumber);

        cloud.release(List.of("busy-1.json"));
        awaitEnd("busy", taskNumber, JobStatus.COMPLETED);

        // Nach der Abmeldung ist die Benutzer:in wieder frei; eine leere inbox endet sofort mit COMPLETED.
        String next = api.start("busy").taskNumber();
        assertThat(next).isNotEqualTo(taskNumber);
        assertThat(awaitEnd("busy", next, JobStatus.COMPLETED).cloud())
                .isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 0, 0, 0));
    }

    @Test
    void startIsRejectedWhileCloudIsUnavailable() {
        api.dropFiles("down", 2, "ok");
        cloud.makeUnavailableFor("down");

        for (int attempt = 0; attempt < 2; attempt++) {
            PipelineApi.Response response = api.startResponse("down");
            assertThat(response.status()).isEqualTo(502);
            assertThat(response.code()).isEqualTo("CLOUD_UNAVAILABLE");
        }
        assertThat(cloud.jobsOf("down")).isEmpty();
        assertThat(api.names("down", "inbox")).hasSize(2);
        assertThat(api.names("down", "pendingbox")).isEmpty();
    }

    // --- 2. Producer: Fehler von Cloud-API 1 -----------------------------------------------------------------

    @Test
    void batchThatWasNotProcessedIsSubmittedAgain() {
        api.dropFiles("retry", 3, "ok");
        cloud.nextSubmitsFor("retry", new SubmitBehavior.Respond(503), new SubmitBehavior.Respond(503));

        String taskNumber = api.start("retry").taskNumber();
        TaskStatus status = awaitEnd("retry", taskNumber, JobStatus.COMPLETED);

        assertThat(status.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 3, 0, 0));
        assertThat(submittedBy("retry")).hasSize(3).doesNotHaveDuplicates();
        assertThat(api.names("retry", "donebox")).hasSize(3);
        assertThat(api.names("retry", "pendingbox")).isEmpty();
    }

    @Test
    void filesWithUnclearSubmitResultStayInPendingboxWithoutMarker() {
        api.dropFiles("unclear", 2, "ok");
        // Cloud-API 1 verarbeitet den Batch, antwortet aber erst nach dem Timeout des Clients (1 s).
        cloud.nextSubmitsFor("unclear", new SubmitBehavior.Delay(Duration.ofSeconds(2)));

        String taskNumber = api.start("unclear").taskNumber();
        TaskStatus status = awaitEnd("unclear", taskNumber, JobStatus.COMPLETED);

        // Die Datensätze gibt es in der Cloud, die Anwendung kennt ihre TaskIds aber nicht und übermittelt nicht erneut.
        assertThat(status.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 0, 0, 2));
        assertThat(submittedBy("unclear")).hasSize(2).doesNotHaveDuplicates();
        assertThat(api.names("unclear", "pendingbox")).containsExactly("unclear-0.json", "unclear-1.json");
        assertThat(api.markers("unclear")).isEmpty();

        PipelineApi.Response next = api.startResponse("unclear");
        assertThat(next.status()).isEqualTo(409);
        assertThat(next.code()).isEqualTo("PENDINGBOX_NOT_EMPTY");
    }

    // --- 3. Consumer und 6. Fehlerschwelle -------------------------------------------------------------------

    @Test
    void errorThresholdEndsTaskAndLeavesOpenFilesInPendingbox() {
        // Genau ein Batch: ein nicht umgewandelter, zwei fehlerhafte und ein unbeendeter Datensatz; Schwelle 3.
        api.drop("errors", "errors-1.json", "REJECT");
        api.drop("errors", "errors-2.json", "FAIL");
        api.drop("errors", "errors-3.json", "FAIL");
        api.drop("errors", "errors-4.json", "SLOW");

        String taskNumber = api.start("errors").taskNumber();
        TaskStatus status = awaitEnd("errors", taskNumber, JobStatus.ERROR);

        assertThat(status.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.ERROR, 0, 2, 1));
        assertThat(api.names("errors", "errorbox")).containsExactly("errors-1.json", "errors-2.json", "errors-3.json");
        assertThat(api.names("errors", "pendingbox")).containsExactly("errors-4.json");
        assertThat(api.markers("errors")).containsExactly("errors-4.json");

        PipelineApi.Response next = api.startResponse("errors");
        assertThat(next.status()).isEqualTo(409);
        assertThat(next.code()).isEqualTo("PENDINGBOX_NOT_EMPTY");

        // Erst nachdem die Benutzer:in die Restdateien bearbeitet hat, ist ein neuer Start möglich.
        api.clearPendingbox("errors");
        String fresh = api.start("errors").taskNumber();
        awaitEnd("errors", fresh, JobStatus.COMPLETED);
    }

    // --- 4. Statusabfrage ------------------------------------------------------------------------------------

    @Test
    void statusShowsCloudAndLocalStateWhileRunning() {
        api.drop("watch", "watch-1.json", "SLOW");
        api.drop("watch", "watch-2.json", "SLOW");
        String taskNumber = api.start("watch").taskNumber();
        awaitPending("watch", taskNumber, 2);

        TaskStatus running = api.status("watch", taskNumber);

        assertThat(running.taskNumber()).isEqualTo(taskNumber);
        assertThat(running.userId()).isEqualTo("watch");
        assertThat(running.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.RUNNING, 0, 0, 2));
        TaskSnapshot local = running.local();
        assertThat(local.state()).isEqualTo(TaskState.RUNNING);
        assertThat(local.submitted()).isEqualTo(2);
        assertThat(local.succeeded()).isZero();
        assertThat(local.finishedAt()).isNull();

        cloud.release(List.of("watch-1.json", "watch-2.json"));
        TaskStatus done = awaitEnd("watch", taskNumber, JobStatus.COMPLETED);
        assertThat(done.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 2, 0, 0));
    }

    @Test
    void endStateThatCannotBeWrittenLeavesJobRunning() {
        api.dropFiles("nowrite", 1, "ok");
        cloud.failJobStatusWrites(true);
        try {
            String taskNumber = api.start("nowrite").taskNumber();

            eventually().untilAsserted(() -> {
                TaskStatus status = api.status("nowrite", taskNumber);
                assertThat(status.local()).isNull();
                assertThat(status.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.RUNNING, 1, 0, 0));
            });
            assertThat(api.names("nowrite", "donebox")).hasSize(1);
        } finally {
            cloud.failJobStatusWrites(false);
        }
    }

    // --- 5. Abbruch von außen --------------------------------------------------------------------------------

    @Test
    void cancelEndsTaskWithTerminatedAndLeavesFiles() {
        // Sechs unbeendete Datensätze: Nach dem ersten Batch wartet der Producer, weil der Status-Pool voll ist.
        api.dropFiles("cancel", 6, "SLOW");
        String taskNumber = api.start("cancel").taskNumber();
        awaitPending("cancel", taskNumber, 4);

        PipelineApi.Response response = api.cancel("cancel", taskNumber);

        assertThat(response.status()).isEqualTo(202);
        assertThat(response.body().get("state")).isEqualTo("TERMINATED");
        TaskStatus status = awaitEnd("cancel", taskNumber, JobStatus.TERMINATED);
        assertThat(status.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.TERMINATED, 0, 0, 4));
        assertThat(api.names("cancel", "inbox")).hasSize(2);
        assertThat(api.names("cancel", "pendingbox")).hasSize(4);
        assertThat(api.markers("cancel")).isEqualTo(api.names("cancel", "pendingbox"));

        // Nach der Abmeldung kennt die TaskRegistry die Aufgabe nicht mehr.
        assertThat(api.cancel("cancel", taskNumber).status()).isEqualTo(404);
        PipelineApi.Response next = api.startResponse("cancel");
        assertThat(next.status()).isEqualTo(409);
        assertThat(next.code()).isEqualTo("PENDINGBOX_NOT_EMPTY");
    }

    @Test
    void sandboxesOfDifferentUsersAreIsolated() {
        api.dropFiles("isoa", 5, "ok");
        api.dropFiles("isob", 3, "SLOW");
        String taskA = api.start("isoa").taskNumber();
        String taskB = api.start("isob").taskNumber();
        awaitPending("isob", taskB, 3);

        api.cancel("isob", taskB);

        awaitEnd("isob", taskB, JobStatus.TERMINATED);
        assertThat(awaitEnd("isoa", taskA, JobStatus.COMPLETED).cloud())
                .isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 5, 0, 0));
        assertThat(api.names("isoa", "donebox")).hasSize(5);
        assertThat(api.names("isob", "pendingbox")).hasSize(3);
        // Jede Sandbox fragt nur ihre eigenen TaskIds ab, höchstens status-bulk-size (3) je Anfrage.
        assertThat(cloud.statusCalls()).isNotEmpty().allSatisfy(call -> {
            assertThat(call.fileNames()).hasSizeBetween(1, 3);
            assertThat(call.fileNames().stream().map(PipelineTestSupport::userOf).collect(Collectors.toSet()))
                    .hasSize(1);
        });
    }

    // --- REST-Vertrag ----------------------------------------------------------------------------------------

    @Test
    void invalidIdsAreRejected() {
        PipelineApi.Response badUser = api.startResponse("-bad");
        assertThat(badUser.status()).isEqualTo(400);
        assertThat(badUser.code()).isEqualTo("INVALID_USER_ID");

        PipelineApi.Response badTask = api.statusResponse("someone", "bad!nr");
        assertThat(badTask.status()).isEqualTo(400);
        assertThat(badTask.code()).isEqualTo("INVALID_TASK_NUMBER");
    }

    @Test
    void unknownOrForeignTasksAreNotFound() {
        PipelineApi.Response unknown = api.statusResponse("nobody", "BJ-999999");
        assertThat(unknown.status()).isEqualTo(404);
        assertThat(unknown.code()).isEqualTo("TASK_NOT_FOUND");
        assertThat(api.cancel("nobody", "BJ-999999").status()).isEqualTo(404);

        api.drop("owner", "owner-1.json", "SLOW");
        String taskNumber = api.start("owner").taskNumber();
        assertThat(api.statusResponse("intruder", taskNumber).status()).isEqualTo(404);
        assertThat(api.cancel("intruder", taskNumber).status()).isEqualTo(404);
        assertThat(api.status("owner", taskNumber).local().state()).isEqualTo(TaskState.RUNNING);

        api.cancel("owner", taskNumber);
        awaitEnd("owner", taskNumber, JobStatus.TERMINATED);
    }
}
