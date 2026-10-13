package de.wwsstl.asynchrone.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import de.wwsstl.asynchrone.cloud.JobType;

/** Prüfung von Art und Name des BatchgenAuftrags aus der Startanfrage. */
class JobDefinitionTest {

    @Test
    void acceptsKnownTypeAndStripsTheName() {
        assertThat(JobDefinition.of("VERTRAGSDATEN", "  Testlauf Oktober "))
                .isEqualTo(new JobDefinition(JobType.VERTRAGSDATEN, "Testlauf Oktober"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "   ")
    void missingNameDefaultsToTheType(String jobName) {
        assertThat(JobDefinition.of("KUNDENDATEN", jobName))
                .isEqualTo(new JobDefinition(JobType.KUNDENDATEN, "KUNDENDATEN"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"kundendaten", "UNBEKANNT", " KUNDENDATEN"})
    void rejectsMissingOrUnknownType(String jobType) {
        assertThatThrownBy(() -> JobDefinition.of(jobType, "Testlauf"))
                .isInstanceOf(InvalidJobTypeException.class)
                .hasMessageContaining("erlaubt: KUNDENDATEN, VERTRAGSDATEN");
    }

    @Test
    void nameMayHaveAtMostTheMaximumLength() {
        String longest = "x".repeat(JobDefinition.MAX_JOB_NAME_LENGTH);

        assertThat(JobDefinition.of("KUNDENDATEN", longest).jobName()).isEqualTo(longest);
        assertThatThrownBy(() -> JobDefinition.of("KUNDENDATEN", longest + "x"))
                .isInstanceOf(InvalidJobNameException.class)
                .hasMessage("jobName ist zu lang: 201 Zeichen (höchstens 200)");
    }
}
