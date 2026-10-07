package de.wwsstl.asynchrone;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

import de.wwsstl.asynchrone.cloud.JobStatus;
import de.wwsstl.asynchrone.task.TaskStatus;

/** Ende durch Überschreiten der maximalen Laufzeit (funktionsweise_sequenz.md, Abschnitt 6). */
@TestPropertySource(properties = "pipeline.task-timeout=1500ms")
class PipelineTimeoutIntegrationTest extends PipelineTestSupport {

    @Test
    void timeoutEndsTaskAndLeavesOpenFilesInPendingbox() {
        api.drop("late", "late-1.json", "SLOW");
        api.drop("late", "late-2.json", "SLOW");

        String taskNumber = api.start("late").taskNumber();
        TaskStatus status = awaitEnd("late", taskNumber, JobStatus.TIMEOUT);

        assertThat(status.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.TIMEOUT, 0, 0, 2));
        assertThat(api.names("late", "pendingbox")).containsExactly("late-1.json", "late-2.json");
        assertThat(api.markers("late")).containsExactly("late-1.json", "late-2.json");
        assertThat(api.names("late", "errorbox")).isEmpty();

        PipelineApi.Response next = api.startResponse("late");
        assertThat(next.status()).isEqualTo(409);
        assertThat(next.code()).isEqualTo("PENDINGBOX_NOT_EMPTY");
    }
}
