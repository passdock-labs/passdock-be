package com.cyjoon68.passdock

import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
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
    fun ingest(@Valid @RequestBody request: LoginEventRequest): LoginEventIngestResult = service.ingest(request)

    @GetMapping("/login-events")
    fun events(): List<LoginEventRecord> = service.events()

    @GetMapping("/risk-alerts")
    fun alerts(): List<RiskAlert> = service.alerts()

    @GetMapping("/risk-rules")
    fun rules(): List<RiskRule> = service.rules()

    @PatchMapping("/risk-alerts/{id}/status")
    fun updateStatus(@PathVariable id: String, @RequestBody request: AlertStatusRequest): RiskAlert = service.updateStatus(id, request.status)
}

@Service
class RiskService(
    private val store: RiskStore = InMemoryRiskStore(),
    private val eventPublisher: RiskEventPublisher = NoopRiskEventPublisher(),
) {
    private val evaluators = mapOf<String, (LoginEventRequest) -> Boolean>(
        "new-device-repeat-failure" to { request -> request.result == "FAILURE" && request.deviceChanged },
        "region-change-after-reset" to { request -> request.result == "RESET" && request.region != "KR" },
    )

    fun ingest(request: LoginEventRequest): LoginEventIngestResult {
        eventPublisher.publish(request)
        val event = store.saveEvent(request)
        val matchedRule = store.rules().firstOrNull { rule ->
            rule.enabled && evaluators[rule.name]?.invoke(request) == true
        }
        val alert = matchedRule?.let { rule ->
            store.saveAlert(event.id, rule, alertReason(request, rule))
        }
        return LoginEventIngestResult(event, alert)
    }

    fun events(): List<LoginEventRecord> = store.events()

    fun alerts(): List<RiskAlert> = store.alerts()

    fun rules(): List<RiskRule> = store.rules()

    fun updateStatus(id: String, status: String): RiskAlert {
        require(status in setOf("OPEN", "ACKED", "RESOLVED")) { "unsupported alert status: $status" }
        return store.updateAlertStatus(id, status)
    }

    private fun alertReason(request: LoginEventRequest, rule: RiskRule): String =
        when (rule.name) {
            "new-device-repeat-failure" -> "${request.userHash} failed passkey login on a changed device"
            "region-change-after-reset" -> "${request.userHash} reset passkey outside the primary region"
            else -> "${request.userHash} matched ${rule.name}"
        }
}

interface RiskStore {
    fun saveEvent(request: LoginEventRequest): LoginEventRecord
    fun events(): List<LoginEventRecord>
    fun alerts(): List<RiskAlert>
    fun rules(): List<RiskRule>
    fun saveAlert(loginEventId: String, rule: RiskRule, reason: String): RiskAlert
    fun updateAlertStatus(id: String, status: String): RiskAlert
}

class InMemoryRiskStore : RiskStore {
    private val eventRecords = mutableListOf<LoginEventRecord>()
    private val alertRecords = mutableListOf<RiskAlert>()
    private val ruleRecords = listOf(
        RiskRule("6ad2c9a9-61f2-45da-a2c2-4ec2a0f4f5f1", "new-device-repeat-failure", "HIGH", enabled = true),
        RiskRule("d2e4c2bc-0104-4a89-bb86-e02a892cc7f6", "region-change-after-reset", "MEDIUM", enabled = true),
    )

    override fun saveEvent(request: LoginEventRequest): LoginEventRecord {
        val event = LoginEventRecord(
            id = UUID.randomUUID().toString(),
            userHash = request.userHash,
            deviceId = request.deviceId,
            region = request.region,
            result = request.result,
            deviceChanged = request.deviceChanged,
            createdAt = Instant.now(),
        )
        eventRecords += event
        return event
    }

    override fun events(): List<LoginEventRecord> = eventRecords.sortedByDescending { it.createdAt }

    override fun alerts(): List<RiskAlert> = alertRecords.sortedByDescending { it.createdAt }

    override fun rules(): List<RiskRule> = ruleRecords

    override fun saveAlert(loginEventId: String, rule: RiskRule, reason: String): RiskAlert {
        val alert = RiskAlert(
            id = UUID.randomUUID().toString(),
            loginEventId = loginEventId,
            ruleName = rule.name,
            severity = rule.severity,
            reason = reason,
            status = "OPEN",
            createdAt = Instant.now(),
        )
        alertRecords += alert
        return alert
    }

    override fun updateAlertStatus(id: String, status: String): RiskAlert {
        val index = alertRecords.indexOfFirst { it.id == id }
        require(index >= 0) { "risk alert not found: $id" }
        val updated = alertRecords[index].copy(status = status)
        alertRecords[index] = updated
        return updated
    }
}

@Repository
class JdbcRiskStore(private val jdbcClient: JdbcClient) : RiskStore {
    override fun saveEvent(request: LoginEventRequest): LoginEventRecord {
        val id = UUID.randomUUID()
        val createdAt = Instant.now()
        jdbcClient.sql(
            """
            insert into login_event (id, user_hash, device_id, region, result, device_changed, created_at)
            values (:id, :userHash, :deviceId, :region, :result, :deviceChanged, :createdAt)
            """.trimIndent(),
        )
            .param("id", id)
            .param("userHash", request.userHash)
            .param("deviceId", request.deviceId)
            .param("region", request.region)
            .param("result", request.result)
            .param("deviceChanged", request.deviceChanged)
            .param("createdAt", Timestamp.from(createdAt))
            .update()

        return LoginEventRecord(id.toString(), request.userHash, request.deviceId, request.region, request.result, request.deviceChanged, createdAt)
    }

    override fun events(): List<LoginEventRecord> =
        jdbcClient.sql(
            """
            select id, user_hash, device_id, region, result, device_changed, created_at
            from login_event
            order by created_at desc, id desc
            limit 50
            """.trimIndent(),
        ).query(::mapEvent).list()

    override fun alerts(): List<RiskAlert> =
        jdbcClient.sql(
            """
            select a.id, a.login_event_id, r.name as rule_name, a.severity, a.reason, a.status, a.created_at
            from risk_alert a
            join risk_rule r on r.id = a.rule_id
            order by a.created_at desc, a.id desc
            limit 50
            """.trimIndent(),
        ).query(::mapAlert).list()

    override fun rules(): List<RiskRule> =
        jdbcClient.sql(
            """
            select id, name, severity, enabled
            from risk_rule
            order by name asc
            """.trimIndent(),
        ).query(::mapRule).list()

    override fun saveAlert(loginEventId: String, rule: RiskRule, reason: String): RiskAlert {
        val id = UUID.randomUUID()
        val createdAt = Instant.now()
        jdbcClient.sql(
            """
            insert into risk_alert (id, login_event_id, rule_id, severity, reason, status, created_at)
            values (:id, :loginEventId, :ruleId, :severity, :reason, 'OPEN', :createdAt)
            """.trimIndent(),
        )
            .param("id", id)
            .param("loginEventId", UUID.fromString(loginEventId))
            .param("ruleId", UUID.fromString(rule.id))
            .param("severity", rule.severity)
            .param("reason", reason)
            .param("createdAt", Timestamp.from(createdAt))
            .update()

        return RiskAlert(id.toString(), loginEventId, rule.name, rule.severity, reason, "OPEN", createdAt)
    }

    override fun updateAlertStatus(id: String, status: String): RiskAlert {
        val updated = jdbcClient.sql(
            """
            update risk_alert
            set status = :status
            where id = :id
            returning id
            """.trimIndent(),
        )
            .param("status", status)
            .param("id", UUID.fromString(id))
            .query(UUID::class.java)
            .optional()

        require(updated.isPresent) { "risk alert not found: $id" }
        return alerts().first { it.id == id }
    }

    private fun mapEvent(rs: ResultSet, rowNumber: Int): LoginEventRecord =
        LoginEventRecord(
            id = rs.getObject("id", UUID::class.java).toString(),
            userHash = rs.getString("user_hash"),
            deviceId = rs.getString("device_id"),
            region = rs.getString("region"),
            result = rs.getString("result"),
            deviceChanged = rs.getBoolean("device_changed"),
            createdAt = rs.getTimestamp("created_at").toInstant(),
        )

    private fun mapAlert(rs: ResultSet, rowNumber: Int): RiskAlert =
        RiskAlert(
            id = rs.getObject("id", UUID::class.java).toString(),
            loginEventId = rs.getObject("login_event_id", UUID::class.java).toString(),
            ruleName = rs.getString("rule_name"),
            severity = rs.getString("severity"),
            reason = rs.getString("reason"),
            status = rs.getString("status"),
            createdAt = rs.getTimestamp("created_at").toInstant(),
        )

    private fun mapRule(rs: ResultSet, rowNumber: Int): RiskRule =
        RiskRule(
            id = rs.getObject("id", UUID::class.java).toString(),
            name = rs.getString("name"),
            severity = rs.getString("severity"),
            enabled = rs.getBoolean("enabled"),
        )
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

data class LoginEventIngestResult(val event: LoginEventRecord, val alert: RiskAlert?)
data class LoginEventRecord(
    val id: String,
    val userHash: String,
    val deviceId: String,
    val region: String,
    val result: String,
    val deviceChanged: Boolean,
    val createdAt: Instant,
)
data class RiskAlert(
    val id: String,
    val loginEventId: String,
    val ruleName: String,
    val severity: String,
    val reason: String,
    val status: String,
    val createdAt: Instant,
)
data class RiskRule(val id: String, val name: String, val severity: String, val enabled: Boolean)
data class AlertStatusRequest(val status: String)
