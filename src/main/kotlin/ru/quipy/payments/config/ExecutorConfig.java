package ru.quipy.payments.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import liquibase.repackaged.org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ExecutorConfig {

    @Bean
    public ExecutorService executorService(MeterRegistry meterRegistry) {
        ExecutorService executorService = Executors.newFixedThreadPool(2048, new BasicThreadFactory.Builder()
            .namingPattern("payment-exec-%d").priority(Thread.MAX_PRIORITY).build());
        ExecutorServiceMetrics.monitor(meterRegistry, executorService, "payment-exec");
        return executorService;
    }
}
