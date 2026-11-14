package com.v2ray.ang.service

import android.content.Context
import com.v2ray.ang.core.CoreConfigManager
import com.v2ray.ang.core.CoreNativeManager
import com.v2ray.ang.dto.RealPingEvent
import com.v2ray.ang.enums.EConfigType
import com.v2ray.ang.extension.isComplexType
import com.v2ray.ang.extension.isNotNullEmpty
import com.v2ray.ang.handler.MmkvManager
import com.v2ray.ang.handler.SettingsManager
import com.v2ray.ang.handler.SpeedtestManager
import com.v2ray.ang.util.JsonUtil
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext

internal object RealPingExecutionLimiter {
    private val serializedMutex = Mutex()

    suspend fun <T> run(configType: EConfigType, configContent: String, block: () -> T): T {
        return if (requiresSerialization(configType, configContent)) {
            serializedMutex.withLock { block() }
        } else {
            block()
        }
    }

    /**
     * Custom WireGuard profiles reproduced a native `close of closed channel` panic (SIGABRT)
     * when several probe cores overlapped (#6077), so they stay serialized globally across
     * batches. Other custom profiles are trimmed to outbounds like generated ones and run
     * with the configured concurrency.
     */
    fun requiresSerialization(configType: EConfigType, configContent: String): Boolean {
        if (configType != EConfigType.CUSTOM) return false
        val outbounds = JsonUtil.parseString(configContent)
            ?.get("outbounds")?.takeIf { it.isJsonArray }?.asJsonArray
            ?: return true
        return outbounds.any { elem ->
            elem.isJsonObject && elem.asJsonObject.get("protocol")
                ?.takeIf { it.isJsonPrimitive && it.asJsonPrimitive.isString }
                ?.asString.equals("wireguard", ignoreCase = true)
        }
    }
}

internal object RealPingStageRunner {
    /**
     * Run [probes] with at most [maxParallel] at once and return the first non-negative delay,
     * or null when every probe fails.
     *
     * Once one probe succeeds, probes that have not started are cancelled. A native probe that
     * is already running cannot be interrupted; it finishes in the background under [parent]
     * and its result is ignored. A throwing probe counts as a failed probe.
     */
    suspend fun firstSuccess(
        parent: Job,
        context: CoroutineContext,
        probes: List<suspend () -> Long>,
        maxParallel: Int
    ): Long? {
        if (probes.isEmpty()) return null
        val result = CompletableDeferred<Long?>()
        val remaining = AtomicInteger(probes.size)
        val permits = Semaphore(maxParallel)
        val stageJob = SupervisorJob(parent)
        val stageScope = CoroutineScope(stageJob + context)
        probes.forEach { probe ->
            stageScope.launch {
                try {
                    val delay = try {
                        permits.withPermit { probe() }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (_: Throwable) {
                        // A probe failure must not escape to the uncaught-exception handler.
                        -1L
                    }
                    if (delay >= 0) {
                        result.complete(delay)
                    }
                } finally {
                    if (remaining.decrementAndGet() == 0) {
                        result.complete(null)
                    }
                }
            }
        }
        return try {
            result.await()
        } finally {
            stageJob.cancel()
        }
    }
}

/**
 * Worker that runs a batch of real-ping tests independently.
 * Each batch owns its own CoroutineScope/dispatcher and can be cancelled separately.
 */
class RealPingWorkerService(
    private val context: Context,
    private val guids: List<String>,
    private val onlyTcp: Boolean = false,
    private val onEvent: (RealPingEvent) -> Unit = {}
) {
    private val job = SupervisorJob()
    private val concurrency = SettingsManager.getRealPingConcurrency()
    private val dispatcher = Executors.newFixedThreadPool(if (onlyTcp) concurrency * 2 else concurrency).asCoroutineDispatcher()
    private val scope = CoroutineScope(job + dispatcher + CoroutineName("RealPingBatchWorker"))

    private val runningCount = AtomicInteger(0)
    private val totalCount = AtomicInteger(0)

    fun start() {
        val jobs = guids.map { guid ->
            totalCount.incrementAndGet()
            scope.launch {
                runningCount.incrementAndGet()
                try {
                    val result = if (onlyTcp) startTcping(guid) else startRealPing(guid)
                    if (scope.isActive) {
                        onEvent(RealPingEvent.Result(guid, result))
                    }
                } catch (_: Throwable) {
                    // ignore
                } finally {
                    val count = totalCount.decrementAndGet()
                    val left = runningCount.decrementAndGet()
                    if (scope.isActive) {
                        onEvent(RealPingEvent.Progress("$left / $count"))
                    }
                }
            }
        }

        scope.launch {
            try {
                joinAll(*jobs.toTypedArray())
                if (isActive) {
                    onEvent(RealPingEvent.Finish("0"))
                }
            } catch (_: CancellationException) {
                // If cancelled, don't send finish event to avoid confusion
            } finally {
                close()
            }
        }
    }

    fun cancel() {
        job.cancel()
    }

    private fun close() {
        try {
            dispatcher.close()
        } catch (_: Throwable) {
            // ignore
        }
    }

    private suspend fun startRealPing(guid: String): Long {
        val retFailure = -1L

        val config = MmkvManager.decodeServerConfig(guid) ?: return retFailure
        if (!config.configType.isComplexType()
            && config.configType != EConfigType.HYSTERIA2
            && config.configType != EConfigType.WIREGUARD
            && config.alpn?.startsWith("h3") != true
            && config.server.isNotNullEmpty()
            && config.serverPort?.toIntOrNull() != null
        ) {
            val url = config.server.orEmpty()
            val port = config.serverPort.orEmpty().toInt()
            val tcpTime = SpeedtestManager.socketConnectTime(url, port, 1000)
            if (tcpTime <= -1L) {
                return retFailure
            }
        }

        val stages = CoreConfigManager.getV2rayConfigStages4Speedtest(context, guid) ?: return retFailure
        val testUrl = SettingsManager.getDelayTestUrl()
        for (stage in stages) {
            val delay = probeStage(stage, config.configType, testUrl)
            if (delay != null) {
                return delay
            }
        }
        return retFailure
    }

    private suspend fun probeStage(stage: List<String>, configType: EConfigType, testUrl: String): Long? {
        val probes = stage.map { content ->
            suspend {
                RealPingExecutionLimiter.run(configType, content) {
                    CoreNativeManager.measureOutboundDelay(content, testUrl)
                }
            }
        }
        return RealPingStageRunner.firstSuccess(job, dispatcher, probes, MAX_PARALLEL_PROBES_PER_PROFILE)
    }

    private fun startTcping(guid: String): Long {
        val retFailure = -1L

        val config = MmkvManager.decodeServerConfig(guid) ?: return retFailure
        if (!config.configType.isComplexType()
            && config.configType != EConfigType.HYSTERIA2
            && config.configType != EConfigType.WIREGUARD
            && config.alpn?.split(',')?.all { it.trim().startsWith("h3") } != true
            && config.server.isNotNullEmpty()
            && config.serverPort?.toIntOrNull() != null
        ) {
            val url = config.server.orEmpty()
            val port = config.serverPort.orEmpty().toInt()
            val tcpTime = SpeedtestManager.socketConnectTime(url, port, 1000)

            return tcpTime
        }

        return retFailure
    }

    private companion object {
        const val MAX_PARALLEL_PROBES_PER_PROFILE = 4
    }
}
