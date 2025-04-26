package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import org.eclipse.jetty.http.HttpStatus
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration.ofMillis
import java.util.UUID
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.function.Supplier


private const val paymentUrl = "http://localhost:1234/external/process"

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val rateLimiter = TokenBucketRateLimiter(
        rate = rateLimitPerSec,
        window = 1005,
        bucketMaxCapacity = rateLimitPerSec,
        timeUnit = TimeUnit.MILLISECONDS
    )

    private val semaphore = Semaphore(parallelRequests)
    private val retryTimeout = ofMillis(100)
    private val retryExecutor = Executors.newScheduledThreadPool(8)

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        semaphore.acquire()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        try {
            logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            // Фиксируем факт отправки платежа независимо от результата.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), ofMillis(now() - paymentStartedAt))
            }

            val localRetry = buildRetryConfig(deadline, transactionId)
            val httpCallSupplier = buildHttpSupplier(transactionId, paymentId, amount)

            Retry.decorateCompletionStage(
                localRetry,
                retryExecutor,
                httpCallSupplier
            ).get()
                .thenApply { response ->
                    try {
                        val readValue = mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        readValue.code = response.statusCode()
                        readValue
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "result code: ${response.statusCode()}, reason: ${response.body()}"
                        )
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            e.message,
                            response.statusCode()
                        )
                    }
                }
                .thenApply { response ->
                    paymentESService.update(paymentId) {
                        it.logProcessing(response.result, now(), transactionId, reason = response.message)
                    }
                }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            semaphore.release()
        }
    }

    private fun buildRetryConfig(deadline: Long, transactionId: UUID?): Retry {
        val localRetryConfig = RetryConfig.custom<ExternalSysResponse>()
            .maxAttempts(5)
            .waitDuration(retryTimeout)
            .retryOnResult { response: ExternalSysResponse ->
                now() < deadline && (!response.result || response.code == HttpStatus.TOO_MANY_REQUESTS_429)
            }
            .retryOnException { exception ->
                (now() < deadline) && (exception is SocketTimeoutException || exception is InterruptedIOException)
            }
            .failAfterMaxAttempts(true)
            .build()

        return Retry.of("payment-${transactionId}", localRetryConfig)
    }

    private fun buildHttpSupplier(
        transactionId: UUID?,
        paymentId: UUID,
        amount: Int
    ): Supplier<CompletionStage<HttpResponse<String>>> {
        val httpRequest = HttpRequest
            .newBuilder()
            .uri(URI("$paymentUrl?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val httpCallSupplier = Supplier<CompletionStage<HttpResponse<String>>> {
            rateLimiter.tickBlocking()
            httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
        }
        return httpCallSupplier
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()