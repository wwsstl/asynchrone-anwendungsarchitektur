package de.wwsstl.asynchrone;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Asynchrone Testdaten-Pipeline (docs/architect/funktionsweise_sequenz.md): je Aufgabe eine Sandbox aus Producer,
 * Status-Pool und Consumer, die Dateien eines Benutzers über die Cloud-Dienste erzeugen lässt.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class TestdatenPipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestdatenPipelineApplication.class, args);
    }
}
