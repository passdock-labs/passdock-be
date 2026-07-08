package com.cyjoon68.passdock

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class PassdockBeApplicationTests {

	@Test
	fun createsHighRiskAlertForChangedDeviceFailure() {
		val service = RiskService()
		val alert = service.ingest(LoginEventRequest("user-a", "device-b", "KR", "FAILURE", deviceChanged = true))

		assertEquals("HIGH", alert.severity)
		assertEquals("OPEN", alert.status)
	}

	@Test
	fun createsMediumRiskAlertForRegionReset() {
		val service = RiskService()
		val alert = service.ingest(LoginEventRequest("user-a", "device-b", "US", "RESET", deviceChanged = false))

		assertEquals("MEDIUM", alert.severity)
	}

}
