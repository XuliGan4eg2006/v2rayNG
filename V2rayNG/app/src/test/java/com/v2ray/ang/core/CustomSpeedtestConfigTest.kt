package com.v2ray.ang.core

import com.google.gson.JsonParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CustomSpeedtestConfigTest {

    @Test
    fun keepsOnlyLogAndOutboundsWithoutMux() {
        val raw = """
            {
              "log": {"loglevel": "debug", "access": "/x"},
              "inbounds": [{"tag": "socks", "port": 10808, "protocol": "socks"}],
              "dns": {"servers": ["1.1.1.1"]},
              "routing": {"rules": [{"outboundTag": "direct", "domain": ["geosite:cn"]}]},
              "observatory": {"subjectSelector": ["proxy"]},
              "outbounds": [
                {"tag": "proxy", "protocol": "vless", "mux": {"enabled": true},
                 "settings": {"vnext": [{"address": "a.example", "port": 443}]}},
                {"tag": "direct", "protocol": "freedom"}
              ]
            }
        """.trimIndent()

        val trimmed = CoreConfigManager.trimCustomConfigForSpeedtest(raw, "warning")!!
        val json = JsonParser.parseString(trimmed).asJsonObject

        assertEquals(setOf("log", "outbounds"), json.keySet())
        assertEquals("warning", json.getAsJsonObject("log").get("loglevel").asString)
        val outbounds = json.getAsJsonArray("outbounds")
        assertEquals(2, outbounds.size())
        val proxy = outbounds[0].asJsonObject
        assertEquals("proxy", proxy.get("tag").asString)
        assertFalse(proxy.has("mux"))
        assertTrue(proxy.has("settings"))
        assertEquals("direct", outbounds[1].asJsonObject.get("tag").asString)
    }

    @Test
    fun rejectsConfigWithoutOutbounds() {
        assertNull(CoreConfigManager.trimCustomConfigForSpeedtest("""{"inbounds": []}""", "warning"))
        assertNull(CoreConfigManager.trimCustomConfigForSpeedtest("""{"outbounds": []}""", "warning"))
    }

    @Test
    fun plainConfigProbesFirstOutboundOnly() {
        val raw = """
            {
              "inbounds": [{"tag": "socks", "protocol": "socks"}],
              "routing": {"rules": [{"type": "field", "domain": ["example.org"], "outboundTag": "direct"}]},
              "outbounds": [{"tag": "proxy", "protocol": "vless"}, {"tag": "direct", "protocol": "freedom"}]
            }
        """.trimIndent()

        assertEquals(listOf(listOf("proxy")), firstTags(raw))
    }

    @Test
    fun defaultRuleSelectsNonFirstOutbound() {
        val raw = """
            {
              "inbounds": [{"tag": "socks", "protocol": "socks"}],
              "routing": {"rules": [{"type": "field", "network": "tcp,udp", "outboundTag": "main"}]},
              "outbounds": [{"tag": "direct", "protocol": "freedom"}, {"tag": "main", "protocol": "vless"}]
            }
        """.trimIndent()

        assertEquals(listOf(listOf("main")), firstTags(raw))
    }

    @Test
    fun balancerChainThroughLoopbacksBecomesOrderedStages() {
        val raw = """
            {
              "inbounds": [{"tag": "socks", "protocol": "socks"}, {"tag": "http", "protocol": "http"}],
              "routing": {
                "rules": [
                  {"type": "field", "inboundTag": ["socks", "http"], "balancerTag": "CHEAP-ONLY"},
                  {"type": "field", "inboundTag": ["REROUTE-HIGH"], "balancerTag": "HIGH-ONLY"},
                  {"type": "field", "domain": ["example.org"], "outboundTag": "direct"}
                ],
                "balancers": [
                  {"tag": "CHEAP-ONLY", "selector": ["cheap"], "strategy": {"type": "leastPing"}, "fallbackTag": "LOOP-TO-HIGH"},
                  {"tag": "HIGH-ONLY", "selector": ["high"], "strategy": {"type": "leastLoad"}, "fallbackTag": "direct"}
                ]
              },
              "outbounds": [
                {"tag": "cheap", "protocol": "vless"},
                {"tag": "cheap-2", "protocol": "vless", "mux": {"enabled": true}},
                {"tag": "high", "protocol": "vless"},
                {"tag": "high-2", "protocol": "vless"},
                {"tag": "direct", "protocol": "freedom"},
                {"tag": "block", "protocol": "blackhole"},
                {"tag": "LOOP-TO-HIGH", "protocol": "loopback", "settings": {"inboundTag": "REROUTE-HIGH"}}
              ]
            }
        """.trimIndent()

        // The direct fallback is not probed, so an unreachable chain reports failure.
        assertEquals(
            listOf(listOf("cheap", "cheap-2"), listOf("high", "high-2")),
            firstTags(raw)
        )
        val probe = JsonParser.parseString(CoreConfigManager.planCustomSpeedtest(raw, "warning")!![0][1]).asJsonObject
        val outbounds = probe.getAsJsonArray("outbounds")
        assertEquals(7, outbounds.size())
        assertFalse(outbounds[0].asJsonObject.has("mux"))
    }

    @Test
    fun balancerFallbackCycleTerminates() {
        val raw = """
            {
              "inbounds": [{"tag": "socks", "protocol": "socks"}],
              "routing": {
                "rules": [
                  {"type": "field", "inboundTag": ["socks", "again"], "balancerTag": "B"}
                ],
                "balancers": [{"tag": "B", "selector": ["p"], "fallbackTag": "loop"}]
              },
              "outbounds": [
                {"tag": "p1", "protocol": "vless"},
                {"tag": "loop", "protocol": "loopback", "settings": {"inboundTag": "again"}}
              ]
            }
        """.trimIndent()

        assertEquals(listOf(listOf("p1")), firstTags(raw))
    }

    private fun firstTags(raw: String): List<List<String>> =
        CoreConfigManager.planCustomSpeedtest(raw, "warning")!!.map { stage ->
            stage.map { config ->
                JsonParser.parseString(config).asJsonObject.getAsJsonArray("outbounds")[0]
                    .asJsonObject.get("tag").asString
            }
        }
}
