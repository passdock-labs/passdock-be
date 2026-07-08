package com.cyjoon68.passdock

import jakarta.validation.constraints.NotBlank
import java.time.Instant
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
class RiskController(private val service: RiskService = RiskService()) {
    @PostMapping("/login-events")
    fun ingest(@RequestBody request: LoginEventRequest): RiskAlert = service.ingest(request)

    @GetMapping("/risk-alerts")
    fun alerts(): List<RiskAlert> = service.alerts()

    @GetMapping("/risk-rules")
    fun rules(): List<RiskRule> = service.rules()

    @PatchMapping("/risk-alerts/{id}/status")
    fun updateStatus(@PathVariable id: String, @RequestBody request: AlertStatusRequest): RiskAlert = service.updateStatus(id, request.status)
}

class RiskService {
    private val alerts = mutableListOf(
        RiskAlert("ra-1", "HIGH", "new device and repeated failure", "OPEN", Instant.parse("2026-07-08T10:20:00Z")),
        RiskAlert("ra-2", "MEDIUM", "region changed after passkey reset", "ACKED", Instant.parse("2026-07-08T10:12:00Z")),
    )
    private val rules = listOf(
        RiskRule("rr-1", "new-device-repeat-failure", "HIGH", enabled = true),
        RiskRule("rr-2", "region-change-after-reset", "MEDIUM", enabled = true),
    )

    fun ingest(request: LoginEventRequest): RiskAlert {
        val severity = if (request.result == "FAILURE" && request.deviceChanged) "HIGH" else "LOW"
        val alert = RiskAlert("ra-${alerts.size + 1}", severity, "passkey ${request.result.lowercase()} from ${request.region}", "OPEN", Instant.now())
        alerts += alert
        return alert
    }

    fun alerts(): List<RiskAlert> = alerts.toList()

    fun rules(): List<RiskRule> = rules

    fun updateStatus(id: String, status: String): RiskAlert {
        val index = alerts.indexOfFirst { it.id == id }
        require(index >= 0) { "risk alert not found: $id" }
        val updated = alerts[index].copy(status = status)
        alerts[index] = updated
        return updated
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
