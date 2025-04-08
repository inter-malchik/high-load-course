package ru.quipy.payments.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import liquibase.repackaged.org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ExecutorConfig {

    @Bean
    @Qualifier("payment")
    public ExecutorService executorServicePayment(MeterRegistry meterRegistry) {
        ExecutorService executorService = Executors.newFixedThreadPool(1024, new BasicThreadFactory.Builder()
            .namingPattern("payment-exec-%d").priority(Thread.MAX_PRIORITY).build());
        ExecutorServiceMetrics.monitor(meterRegistry, executorService, "payment-exec");
        return executorService;
    }

    @Bean
    @Qualifier("http")
    public ExecutorService executorServiceHttp(MeterRegistry meterRegistry) {
        ExecutorService executorService = Executors.newFixedThreadPool(2048, new BasicThreadFactory.Builder()
                .namingPattern("http-exec-%d").priority(Thread.MAX_PRIORITY).build());
        ExecutorServiceMetrics.monitor(meterRegistry, executorService, "http-exec");
        return executorService;
    }
}
