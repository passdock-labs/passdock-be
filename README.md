# passdock-be

Kotlin Spring Boot MVC API for PassDock authentication risk events and alerts.

## Stack

- Kotlin
- Spring Boot MVC
- PostgreSQL schema + Flyway migration
- Kafka dependency
- Prometheus actuator endpoint
- Docker
- GitHub Actions CI

## API Scope

- Login event ingestion.
- Risk rule listing.
- Risk alert creation.
- Alert status update.
- Prometheus metrics endpoint.

## Risk Evaluation

- Enabled rules are evaluated through a rule-name strategy map.
- Matching rules create alerts with the configured severity.
- Tests cover changed-device failures and region reset alerts.
