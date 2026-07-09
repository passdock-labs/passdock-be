package com.cyjoon68.passdock

import jakarta.validation.constraints.NotBlank
import java.time.Instant
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@CrossOrigin(origins = ["*"])
@RequestMapping("/api")
class RiskController(private val service: RiskService) {
    @PostMapping("/login-events")
    fun ingest(@RequestBody request: LoginEventRequest): RiskAlert = service.ingest(request)

    @GetMapping("/risk-alerts")
    fun alerts(): List<RiskAlert> = service.alerts()

    @GetMapping("/risk-rules")
    fun rules(): List<RiskRule> = service.rules()

    @PatchMapping("/risk-alerts/{id}/status")
    fun updateStatus(@PathVariable id: String, @RequestBody request: AlertStatusRequest): RiskAlert = service.updateStatus(id, request.status)
}

@Service
class RiskService(private val eventPublisher: RiskEventPublisher = NoopRiskEventPublisher()) {
    private val alerts = mutableListOf(
        RiskAlert("ra-1", "HIGH", "new device and repeated failure", "OPEN", Instant.parse("2026-07-08T10:20:00Z")),
        RiskAlert("ra-2", "MEDIUM", "region changed after passkey reset", "ACKED", Instant.parse("2026-07-08T10:12:00Z")),
    )
    private val rules = listOf(
        RiskRule("rr-1", "new-device-repeat-failure", "HIGH", enabled = true),
        RiskRule("rr-2", "region-change-after-reset", "MEDIUM", enabled = true),
    )
    private val evaluators = mapOf<String, (LoginEventRequest) -> Boolean>(
        "new-device-repeat-failure" to { request -> request.result == "FAILURE" && request.deviceChanged },
        "region-change-after-reset" to { request -> request.result == "RESET" && request.region != "KR" },
    )

    fun ingest(request: LoginEventRequest): RiskAlert {
        eventPublisher.publish(request)
        val severity = rules.firstOrNull { rule -> rule.enabled && evaluators[rule.name]?.invoke(request) == true }?.severity ?: "LOW"
        val alert = RiskAlert("ra-${alerts.size + 1}", severity, "passkey ${request.result.lowercase()} from ${request.region}", "OPEN", Instant.now())
        alerts += alert
        return alert
    }

    fun alerts(): List<RiskAlert> = alerts.toList()

    fun rules(): List<RiskRule> = rules

    fun updateStatus(id: String, status: String): RiskAlert {
        require(status in setOf("OPEN", "ACKED", "RESOLVED")) { "unsupported alert status: $status" }
        val index = alerts.indexOfFirst { it.id == id }
        require(index >= 0) { "risk alert not found: $id" }
        val updated = alerts[index].copy(status = status)
        alerts[index] = updated
        return updated
    }
}

interface RiskEventPublisher {
    fun publish(request: LoginEventRequest)
}

class NoopRiskEventPublisher : RiskEventPublisher {
    override fun publish(request: LoginEventRequest) = Unit
}

@Component
class KafkaRiskEventPublisher(private val kafkaTemplate: KafkaTemplate<String, LoginEventRequest>) : RiskEventPublisher {
    override fun publish(request: LoginEventRequest) {
        kafkaTemplate.send("passdock.login-events", request.userHash, request)
    }
}

@Configuration
class KafkaProducerConfig {
    @Bean
    fun loginEventKafkaTemplate(
        @Value("\${spring.kafka.bootstrap-servers:localhost:9092}") bootstrapServers: String,
    ): KafkaTemplate<String, LoginEventRequest> {
        val properties = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java,
            JsonSerializer.ADD_TYPE_INFO_HEADERS to false,
        )

        return KafkaTemplate(DefaultKafkaProducerFactory(properties))
    }
}

data class LoginEventRequest(
    @field:NotBlank val userHash: String,
    @field:NotBlank val deviceId: String,
    @field:NotBlank val region: String,
    @field:NotBlank val result: String,
    val deviceChanged: Boolean,
)

data class RiskAlert(val id: String, val severity: String, val reason: String, val status: String, val createdAt: Instant)
data class RiskRule(val id: String, val name: String, val severity: String, val enabled: Boolean)
data class AlertStatusRequest(val status: String)
