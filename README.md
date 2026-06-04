# NO24 - Concert Ticketing System

대규모 동시 접속을 처리하는 콘서트 티켓팅 시스템입니다. 대기열 관리, 좌석 예약, 결제 처리를 마이크로서비스 아키텍처로 구현했습니다.

---

## 아키텍처 개요

```
[Client Browser]
       │
       │ SSE (대기열 순위 실시간 수신)
       ▼
  ┌─────────────┐        ┌─────────────┐
  │  API Server  │◄──────►│  API Server  │  (replica x2)
  │    (Go)      │        │    (Go)      │
  └──────┬──────┘        └──────┬──────┘
         │                      │
         ▼                      ▼
  ┌─────────────────────────────────────┐
  │              Kafka                  │
  │  - wait-queue-events (토큰 브로드캐스트) │
  │  - ticketing.payment.requested      │
  └──────────────┬──────────────────────┘
                 │
         ┌───────┴───────┐
         ▼               ▼
  ┌─────────────┐  ┌─────────────┐
  │    Redis    │  │    MySQL    │
  │  (대기열/락)  │  │  (이벤트 DB) │
  └─────────────┘  └─────────────┘
         ▲
         │
  ┌──────┴──────┐        ┌────────────────┐
  │  Ticketing  │◄──────►│   Ticketing    │  (replica x2)
  │  Server     │        │   Server       │
  │  (Java)     │        │   (Java)       │
  └─────────────┘        └────────────────┘
         │
         │ SSE (결제 결과 실시간 수신)
         ▼
  [Client Browser]
```

---

## 서비스 구성 (Monorepo)

| 서비스 | 언어/프레임워크 | 역할 |
|---|---|---|
| `api-server` | Go 1.25 + Echo v5 | 대기열 관리, 순번 발급, JWT 토큰 발행 |
| `ticketing-server` | Java 21 + Spring Boot 4.0 | 좌석 예약, 결제 처리, 푸시 알림 |

---

## 기술 스택

### Infrastructure
- **Kafka** — 서비스 간 이벤트 스트리밍, 다중 replica 간 SSE 토큰 브로드캐스트
- **Redis** — 대기열 Sorted Set, 분산 락, 좌석 예약 상태 캐싱
- **MySQL** — 이벤트(공연) 정보 영구 저장
- **Kubernetes** — 전체 인프라 오케스트레이션

### API Server (Go)
- Echo v5 — HTTP 라우터
- `encoding/json` + SSE — 실시간 순위 스트리밍
- Kafka Go client — 토큰 브로드캐스트
- Redis Sorted Set — 대기열 FIFO 관리
- JWT 발급 — 대기열 통과 유저에게 10분 유효 토큰 발행

### Ticketing Server (Java)
- Spring Security + JWT 필터 — Bearer 토큰 인증
- Redisson — Redis 기반 분산 락 (좌석별 RLock)
- Spring Kafka — 결제 이벤트 Producer/Consumer
- SSE Emitter — 결제 결과 실시간 푸시

---

## 주요 기능 및 흐름

### 1. 대기열 입장 (API Server)
```
GET /enter?event_id={event_id}&user_id={user_id}
```
- SSE 연결 수립 후 Redis Sorted Set(`waiting:{event_id}`)에 유저 등록
- 3초마다 현재 순위를 SSE로 전송
- **2-Tier 순위 조회 최적화:**
  - Tier 1: 상위 100명 또는 상위 1% → Redis 직접 조회
  - Tier 2: 나머지 → 1분 주기 인메모리 스냅샷 활용 (대규모 트래픽 시 Redis 부하 절감)

### 2. 대기열 처리 및 토큰 발급
- `waitingQueueWorker`가 FIFO 방식으로 유저를 팝
- `user_id` + `event_id` 클레임이 담긴 JWT 토큰(10분 유효) 생성
- SSE로 직접 전달, 또는 Kafka(`wait-queue-events`) 브로드캐스트를 통해 다른 replica 경유 전달

### 3. 좌석 예약 (Ticketing Server)
```
POST /api/seats/reserve
Authorization: Bearer {jwt_token}
Body: {"seatId": "A1"}
```
- JWT에서 `user_id` 추출 후 SecurityContext에 설정
- Redisson `tryLock(0, 0, SECONDS)` — 이미 선점된 좌석은 즉시 409 반환
- 성공 시 Redis MapCache에 10분간 예약 상태 저장
- `reservationId`(UUID) 반환

### 4. 결제 처리 (Ticketing Server)
```
POST /api/payments/request
Authorization: Bearer {jwt_token}
Body: {"reservationId": "uuid", "amount": 50000}

GET /api/notifications/subscribe   ← SSE 연결 (결제 결과 수신)
Authorization: Bearer {jwt_token}
```
- `PaymentRequestedEvent`를 Kafka 토픽(`ticketing.payment.requested`)에 발행
- `PaymentProcessor`가 컨슈밍 후 처리
- 결과를 SSE Emitter로 해당 유저에게 푸시

---

## API Reference

### API Server (Port 80)

| Method | Path | 설명 |
|---|---|---|
| `GET` | `/enter` | 대기열 입장 + SSE 연결 (`?event_id=&user_id=`) |

**SSE 이벤트 예시:**
```json
{"seq": 42, "token": "<jwt_token>"}
```

### Ticketing Server (Port 81)

| Method | Path | 인증 | 설명 |
|---|---|---|---|
| `POST` | `/api/seats/reserve` | Bearer JWT | 좌석 예약 |
| `POST` | `/api/payments/request` | Bearer JWT | 결제 요청 |
| `GET` | `/api/notifications/subscribe` | Bearer JWT | 결제 결과 SSE 구독 |

**좌석 예약 응답:**
```json
{
  "success": true,
  "message": "Seat reserved successfully",
  "data": {
    "reservationId": "550e8400-e29b-41d4-a716-446655440000",
    "seatId": "A1"
  }
}
```

---

## 데이터베이스 스키마

```sql
CREATE TABLE events (
    id        INT PRIMARY KEY,
    name      VARCHAR(50) UNIQUE,          -- e.g. "bts_2026_seoul"
    status    ENUM('A', 'N'),              -- Active / Inactive
    start_at  TIMESTAMP,
    end_at    TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

---

## 인프라 구성 (Kubernetes)

```
infra/
├── kafka-service.yaml          # Kafka StatefulSet
├── redis-service.yaml          # Redis Deployment + PVC (1Gi)
├── mysql-service.yaml          # MySQL StatefulSet + PVC (5Gi)
├── api-server/
│   └── deployment.yaml         # replicas: 2, LoadBalancer:80
└── ticketing-server/
    └── deployment.yaml         # replicas: 2, LoadBalancer:81
```

| 컴포넌트 | 타입 | 포트 | 스토리지 |
|---|---|---|---|
| api-server | Deployment (x2) | 80 → 8080 | - |
| ticketing-server | Deployment (x2) | 81 → 8081 | - |
| Kafka | StatefulSet (x1) | 9092 | - |
| Redis | Deployment | 6379 | 1Gi PVC |
| MySQL | StatefulSet | 3306 | 5Gi PVC |

---

## 프로젝트 구조

```
no24/
├── api-server/
│   ├── cmd/main.go                     # 엔트리포인트
│   ├── internal/
│   │   ├── mq/client.go               # Kafka 클라이언트
│   │   ├── queue/
│   │   │   ├── repository.go          # Redis 대기열 CRUD
│   │   │   ├── snapshot.go            # 인메모리 스냅샷 캐시
│   │   │   └── events.go              # 이벤트 로딩 (MySQL)
│   │   ├── service/service.go         # 대기열 처리 비즈니스 로직
│   │   ├── sse/sse.go                 # SSE Hub (goroutine 기반)
│   │   └── model/                     # 데이터 모델
│   ├── no24_api_Dockerfile
│   └── go.mod
│
├── ticketing-server/ticketing/src/main/java/com/ticketing/ticketing/
│   ├── controller/TicketingController.java
│   ├── seat/SeatReservationService.java
│   ├── payment/
│   │   ├── PaymentProducer.java
│   │   └── PaymentProcessor.java
│   ├── auth/
│   │   ├── JwtAuthenticationFilter.java
│   │   └── JwtTokenProvider.java
│   ├── notification/PushNotificationService.java
│   └── config/
│       ├── KafkaConfig.java
│       ├── RedissonConfig.java
│       └── SecurityConfig.java
│
├── database_schema/schema.sql
├── infra/
└── .github/workflows/
    ├── docker-push.yml             # CI/CD (조건부 빌드)
    └── security-audit.yml          # PR 시 보안 감사
```

---

## CI/CD

GitHub Actions를 통한 자동 빌드 및 배포:

| 커밋 메시지 트리거 | 동작 |
|---|---|
| `[build-api]` | API Server Docker 이미지 빌드 & 푸시 |
| `[build-tkt]` | Ticketing Server Docker 이미지 빌드 & 푸시 |

- 빌드 완료 후 `infra/` 내 deployment manifest의 이미지 태그를 커밋 SHA로 자동 업데이트
- PR 생성 시 `security-audit.yml`이 AI 기반 보안 감사 실행

---

## 로컬 개발 환경 설정

### 의존성
- Go 1.25+
- Java 21+
- Docker & Kubernetes (kubectl)

### 인프라 실행
```bash
kubectl apply -f infra/kafka-service.yaml
kubectl apply -f infra/redis-service.yaml
kubectl apply -f infra/mysql-service.yaml
```

### API Server 실행
```bash
cd api-server
go run cmd/main.go
```

### Ticketing Server 실행
```bash
cd ticketing-server/ticketing
./gradlew bootRun
```

---

## 부하 테스트

`ticketing-server/src/main/resources/static/stress-client.html`에서 브라우저 기반 스트레스 테스트를 실행할 수 있습니다.
