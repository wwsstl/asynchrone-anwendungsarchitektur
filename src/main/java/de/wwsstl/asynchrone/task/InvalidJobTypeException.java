package de.wwsstl.asynchrone.task;

import java.util.Arrays;

import de.wwsstl.asynchrone.cloud.JobType;

/** {@code jobType} fehlt in der Startanfrage oder ist keine der erlaubten Arten. */
public class InvalidJobTypeException extends RuntimeException {

    public InvalidJobTypeException(String jobType) {
        super((jobType == null ? "jobType fehlt" : "Ungültiger jobType: '" + jobType + "'") + " (erlaubt: "
                + String.join(", ", Arrays.stream(JobType.values()).map(Enum::name).toList()) + ")");
    }
}
