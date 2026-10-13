package de.wwsstl.asynchrone.task;

import java.util.Arrays;

import de.wwsstl.asynchrone.cloud.JobType;

/**
 * Art und Name des BatchgenAuftrags einer Aufgabe, wie sie beim Start angegeben werden.
 *
 * @param jobType Art des BatchgenAuftrags
 * @param jobName Name des BatchgenAuftrags; ohne Angabe der Name der Art (z. B. {@code KUNDENDATEN})
 */
public record JobDefinition(JobType jobType, String jobName) {

    /** Höchstlänge von {@code jobName}. */
    public static final int MAX_JOB_NAME_LENGTH = 200;

    /**
     * Prüft die Angaben einer Startanfrage. Ein fehlender, leerer oder nur aus Leerzeichen bestehender {@code jobName}
     * gilt als nicht angegeben und wird durch den Namen der Art ersetzt; sonst werden Leerzeichen am Rand entfernt.
     *
     * @throws InvalidJobTypeException wenn {@code jobType} fehlt oder keine der erlaubten Arten ist
     * @throws InvalidJobNameException wenn {@code jobName} länger als {@value #MAX_JOB_NAME_LENGTH} Zeichen ist
     */
    public static JobDefinition of(String jobType, String jobName) {
        JobType type = Arrays.stream(JobType.values())
                .filter(candidate -> candidate.name().equals(jobType))
                .findFirst()
                .orElseThrow(() -> new InvalidJobTypeException(jobType));
        if (jobName == null || jobName.isBlank()) {
            return new JobDefinition(type, type.name());
        }
        String name = jobName.strip();
        if (name.length() > MAX_JOB_NAME_LENGTH) {
            throw new InvalidJobNameException(name.length(), MAX_JOB_NAME_LENGTH);
        }
        return new JobDefinition(type, name);
    }
}
