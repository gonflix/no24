import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Counter } from "k6/metrics";

// ── 설정 ────────────────────────────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://192.168.1.201:81";

// api-server에서 발급받은 RS256 JWT를 여기에 붙여넣기
const JWT_TOKEN = __ENV.JWT_TOKEN || "REPLACE_ME";

// ── 커스텀 메트릭 ────────────────────────────────────────────────────────────
// 결제요청 POST ~ 상태 200 수신까지의 실제 처리 시간 (폴링 대기 포함)
const paymentE2eDuration = new Trend("payment_e2e_ms", true);

// 결과별 카운터
const cntCompleted = new Counter("payment_completed"); // 결제 완료 (성공+실패 모두)
const cntTimeout = new Counter("payment_timeout"); // 30초 이내 응답 없음
const cntReserveFail = new Counter("reserve_fail"); // 좌석 예약 자체 실패

// ── 부하 시나리오 ────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    // 시나리오 A: 일정 동시 사용자 유지 (처리량/TPS 측정용)
    steady_load: {
      executor: "constant-vus",
      vus: 50,
      duration: "60s",
    },

    // 시나리오 B: 단계별 부하 증가 (병목 지점 탐색용)
    // steady_load 대신 아래 주석을 해제하면 ramp-up 시나리오로 전환
    // ramp_up: {
    //   executor: 'ramping-vus',
    //   startVUs: 0,
    //   stages: [
    //     { duration: '20s', target: 20  },  // 워밍업
    //     { duration: '30s', target: 100 },  // 부하 증가
    //     { duration: '10s', target: 0   },  // 정리
    //   ],
    // },
  },

  thresholds: {
    // P95 end-to-end 처리시간 10초 이내
    payment_e2e_ms: ["p(95)<10000"],
    // HTTP 오류율 — reserve 제외, 결제 요청/폴링만 체크
    "http_req_failed{name:payment_request}": ["rate<0.05"],
    "http_req_failed{name:payment_status}": ["rate<0.05"],
    // 타임아웃 0건 목표 (초과 시 경고만)
    payment_timeout: ["count<1"],
  },
};

// ── 공통 헤더 ─────────────────────────────────────────────────────────────────
const headers = {
  "Content-Type": "application/json",
  Authorization: `Bearer ${JWT_TOKEN}`,
};

// ── 메인 시나리오: VU 1명이 반복 실행 ────────────────────────────────────────
export default function () {
  const seatId = `S-${__VU}-${__ITER}`;

  // ── Step 1: 좌석 예약 (측정 제외 — 준비 단계) ────────────────────────────
  const reserveRes = http.post(
    `${BASE_URL}/api/seats/reserve`,
    JSON.stringify({ seatId }),
    { headers, tags: { name: "reserve" } },
  );

  if (reserveRes.status !== 200) {
    cntReserveFail.add(1);
    sleep(0.5);
    return;
  }

  const reservationId = reserveRes.json("data.reservationId");
  if (!reservationId) {
    cntReserveFail.add(1);
    sleep(0.5);
    return;
  }

  // ── Step 2: 결제 요청 → 202 Accepted  [측정 시작] ────────────────────────
  const payStart = Date.now();

  const payReqRes = http.post(
    `${BASE_URL}/api/payments/request`,
    JSON.stringify({ reservationId, amount: 50000 }),
    { headers, tags: { name: "payment_request" } },
  );

  const payReqOk = check(payReqRes, {
    "payment_request: 202 Accepted": (r) => r.status === 202,
  });

  if (!payReqOk) {
    sleep(0.5);
    return;
  }

  // ── Step 3: 결제 상태 폴링 (1초 간격, 최대 30초)  [측정 종료] ─────────────
  const POLL_INTERVAL_S = 1;
  const TIMEOUT_MS = 30_000;
  let done = false;

  while (Date.now() - payStart < TIMEOUT_MS) {
    const statusRes = http.get(
      `${BASE_URL}/api/payments/status/${reservationId}`,
      { headers, tags: { name: "payment_status" } },
    );

    check(statusRes, {
      "payment_status: 200 or 202": (r) => r.status === 200 || r.status === 202,
    });

    if (statusRes.status === 200) {
      // 결제 POST 직전 ~ 완료 폴링까지 전체 시간
      paymentE2eDuration.add(Date.now() - payStart);
      cntCompleted.add(1);
      done = true;
      break;
    }

    sleep(POLL_INTERVAL_S);
  }

  if (!done) {
    cntTimeout.add(1);
  }
}

// ── 테스트 종료 후 요약 출력 ──────────────────────────────────────────────────
export function handleSummary(data) {
  const dur = data.metrics["payment_e2e_ms"];
  const completed = data.metrics["payment_completed"]?.values?.count ?? 0;
  const timeout = data.metrics["payment_timeout"]?.values?.count ?? 0;
  const reserve = data.metrics["reserve_fail"]?.values?.count ?? 0;
  const totalIter = data.metrics["iterations"]?.values?.count ?? 0;
  const durationS = (data.state?.testRunDurationMs ?? 60000) / 1000;
  const paymentTps = (completed / durationS).toFixed(2);

  const summary = [
    "══════════════════════════════════════════════",
    "  Payment Load Test Summary",
    "  (측정 범위: 결제 요청 POST ~ 폴링 완료)",
    "══════════════════════════════════════════════",
    `  Test Duration    : ${durationS.toFixed(1)}s`,
    `  Iterations       : ${totalIter}`,
    `  Completed        : ${completed}`,
    `  Payment TPS      : ${paymentTps} req/s`,
    `  Timeout (>30s)   : ${timeout}`,
    `  Reserve fail     : ${reserve}  (측정 제외)`,
    "",
    `  E2E Latency  (결제 POST → 폴링 200 수신)`,
    `    avg : ${ms(dur?.values?.avg)}`,
    `    p50 : ${ms(dur?.values?.["p(50)"] ?? dur?.values?.med)}`,
    `    p90 : ${ms(dur?.values?.["p(90)"])}`,
    `    p95 : ${ms(dur?.values?.["p(95)"])}`,
    `    p99 : ${ms(dur?.values?.["p(99)"])}`,
    "══════════════════════════════════════════════",
  ].join("\n");

  console.log(summary);

  return {
    stdout: summary,
    "k6/summary.json": JSON.stringify(data, null, 2),
  };
}

function ms(val) {
  if (val == null) return "N/A";
  return `${(val / 1000).toFixed(2)}s`;
}
