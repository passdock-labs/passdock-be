package com.cyjoon68.passdock

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class PassdockBeApplicationTests {

	@Test
	fun createsHighRiskAlertForChangedDeviceFailure() {
		val service = RiskService()
		val alert = service.ingest(LoginEventRequest("user-a", "device-b", "KR", "FAILURE", deviceChanged = true)).alert

		assertEquals("HIGH", alert?.severity)
		assertEquals("OPEN", alert?.status)
	}

	@Test
	fun createsMediumRiskAlertForRegionReset() {
		val service = RiskService()
		val alert = service.ingest(LoginEventRequest("user-a", "device-b", "US", "RESET", deviceChanged = false)).alert

		assertEquals("MEDIUM", alert?.severity)
	}

	@Test
	fun storesLoginEventWithoutAlertWhenRiskRuleDoesNotMatch() {
		val service = RiskService()
		val result = service.ingest(LoginEventRequest("user-a", "device-a", "KR", "SUCCESS", deviceChanged = false))

		assertEquals(null, result.alert)
		assertEquals(1, service.events().size)
	}

	@Test
	fun updatesRiskAlertStatusWithSupportedLifecycleState() {
		val service = RiskService()
		val alert = service.ingest(LoginEventRequest("user-a", "device-b", "KR", "FAILURE", deviceChanged = true)).alert
			?: error("expected risk alert")
		val acked = service.updateStatus(alert.id, "ACKED", "담당자 확인 중")
		val resolved = service.updateStatus(alert.id, "RESOLVED", "사용자 확인 후 해결")

		assertEquals("ACKED", acked.status)
		assertEquals("RESOLVED", resolved.status)
	}

}
