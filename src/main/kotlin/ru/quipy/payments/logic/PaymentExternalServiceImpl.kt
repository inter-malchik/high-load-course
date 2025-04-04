package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.eclipse.jetty.http.HttpStatus
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration.ofMillis
import java.time.Duration.ofSeconds
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


private const val paymentUrl = "http://localhost:1234/external/process"

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private var CALL_TIMEOUT_MULT = 2.5

    private val client = OkHttpClient.Builder()
        .callTimeout((requestAverageProcessingTime.toMillis() * CALL_TIMEOUT_MULT).toLong(), TimeUnit.MILLISECONDS)
        .build()
    private val rateLimiter = LeakingBucketRateLimiter(rateLimitPerSec.toLong(), ofSeconds(1), rateLimitPerSec)
    private val semaphore = Semaphore(parallelRequests)
    private val retryTimeout = ofSeconds(1)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        semaphore.acquire()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        try {
            rateLimiter.tickBlocking()
            logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            // Фиксируем факт отправки платежа независимо от результата.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), ofMillis(now() - paymentStartedAt))
            }

            val request = Request.Builder().run {
                url("$paymentUrl?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val localRetryConfig = RetryConfig.custom<ExternalSysResponse>()
                .maxAttempts(5)
                .waitDuration(retryTimeout)
                .retryOnResult { response: ExternalSysResponse ->
                    now() <= deadline && (!response.result || response.code == HttpStatus.TOO_MANY_REQUESTS_429)
                }
                .retryOnException { exception ->
                    (exception is SocketTimeoutException || exception is InterruptedIOException) && (now() <= deadline)
                }
                .failAfterMaxAttempts(true)
                .build()

            val localRetry = Retry.of("payment-${transactionId}", localRetryConfig)

            val decoratedCall = Retry.decorateCallable(localRetry) {
                client.newCall(request).execute().use { response ->
                    val body = try {
                        var readValue = mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        readValue.code = response.code
                        readValue
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "result code: ${response.code}, reason: ${response.body?.string()}"
                        )
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message,
                            response.code)
                    }
                    logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                "succeeded: ${body.result}, message: ${body.message}"
                    )
                    body
                }
            }

            val result: ExternalSysResponse = decoratedCall.call()

            paymentESService.update(paymentId) {
                it.logProcessing(result.result, now(), transactionId, reason = result.message)
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


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()