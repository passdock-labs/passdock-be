# passdock-be

PassDock의 인증 이벤트와 위험 알림을 처리하는 Kotlin Spring Boot MVC API입니다.

## 기술 스택

- Kotlin
- Spring Boot MVC
- PostgreSQL schema + Flyway migration
- Kafka dependency
- Prometheus actuator endpoint
- Docker
- GitHub Actions CI

## API 범위

- 로그인 이벤트 수집
- 위험 규칙 목록 조회
- 위험 알림 생성
- 알림 상태 변경
- Prometheus metrics endpoint

## 위험 평가

- 활성화된 규칙은 rule-name strategy map으로 평가합니다.
- 매칭된 규칙은 설정된 severity로 알림을 생성합니다.
- 기기 변경 실패와 지역 변경 reset 알림 테스트를 포함합니다.
