package com.v2ray.ang.service

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class RealPingStageRunnerTest {

    @Test
    fun returnsFirstSuccessWithoutWaitingForSlowProbes() {
        runBlocking {
            val parent = SupervisorJob()
            val release = CountDownLatch(1)
            val probes: List<suspend () -> Long> = listOf(
                { release.await(5, TimeUnit.SECONDS); -1L },
                { 42L },
            )

            val started = System.nanoTime()
            val result = RealPingStageRunner.firstSuccess(parent, Dispatchers.IO, probes, 4)
            val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
            release.countDown()
            parent.cancel()

            assertEquals(42L, result)
            assertTrue(elapsedMs < 3000, "waited ${elapsedMs}ms for a slow probe")
        }
    }

    @Test
    fun returnsNullWhenEveryProbeFails() {
        runBlocking {
            val parent = SupervisorJob()
            val probes: List<suspend () -> Long> = List(5) { { -1L } }

            assertNull(RealPingStageRunner.firstSuccess(parent, Dispatchers.IO, probes, 2))
            parent.cancel()
        }
    }

    @Test
    fun throwingProbeDoesNotBlockTheStage() {
        runBlocking {
            val parent = SupervisorJob()
            val probes: List<suspend () -> Long> = listOf(
                { throw IllegalStateException("probe failed") },
                { -1L },
                { 7L },
            )

            assertEquals(7L, RealPingStageRunner.firstSuccess(parent, Dispatchers.IO, probes, 1))
            parent.cancel()
        }
    }

    @Test
    fun limitsParallelProbes() {
        runBlocking {
            val parent = SupervisorJob()
            val active = AtomicInteger(0)
            val maxActive = AtomicInteger(0)
            val probes: List<suspend () -> Long> = List(8) {
                {
                    val current = active.incrementAndGet()
                    maxActive.accumulateAndGet(current, ::maxOf)
                    Thread.sleep(20)
                    active.decrementAndGet()
                    -1L
                }
            }

            assertNull(RealPingStageRunner.firstSuccess(parent, Dispatchers.IO, probes, 3))
            parent.cancel()
            assertTrue(maxActive.get() <= 3, "max parallel probes was ${maxActive.get()}")
        }
    }
}
