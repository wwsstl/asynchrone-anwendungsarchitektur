package de.wwsstl.asynchrone.config;

import java.time.Clock;
import java.util.concurrent.Executors;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.WebClientCloudClient;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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

    /**
     * Scheduler für die blockierenden Aufrufe des {@code TaskManager} aus der reaktiven REST-API (Dateisystem,
     * Sperren, Warten auf auslaufende Threads): je Aufruf ein Virtual Thread, sodass der Netty-Event-Loop nie
     * blockiert.
     */
    @Bean(destroyMethod = "dispose")
    Scheduler taskManagerScheduler() {
        return Schedulers.fromExecutorService(Executors.newVirtualThreadPerTaskExecutor(), "task-manager");
    }
}
