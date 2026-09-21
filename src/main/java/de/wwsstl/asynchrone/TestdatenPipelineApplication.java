package de.wwsstl.asynchrone;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class TestdatenPipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestdatenPipelineApplication.class, args);
    }
}
