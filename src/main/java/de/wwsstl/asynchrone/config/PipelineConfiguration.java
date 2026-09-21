package de.wwsstl.asynchrone.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.WebClientCloudClient;

@Configuration(proxyBeanMethods = false)
public class PipelineConfiguration {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    CloudClient cloudClient(WebClient.Builder builder, PipelineProperties properties) {
        WebClient webClient = builder.baseUrl(properties.cloud().baseUrl().toString()).build();
        return new WebClientCloudClient(webClient, properties.cloud());
    }
}
