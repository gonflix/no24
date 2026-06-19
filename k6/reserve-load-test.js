import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Counter } from "k6/metrics";
import exec from "k6/execution";

// ── 설정 ────────────────────────────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://192.168.1.201:81";

// api-server(큐 서버)에서 발급받은 RS256 JWT를 여기에 붙여넣기
const JWT_TOKEN = __ENV.JWT_TOKEN || "REPLACE_ME";

// 좌석 총 수 (1~1000: event=1, 1001~2000: event=2, 2001~3000: event=3)
const TOTAL_SEATS = 3000;

// ── 커스텀 메트릭 ────────────────────────────────────────────────────────────
const reserveDurationMs = new Trend("reserve_duration_ms", true);
const cntReserveOk = new Counter("reserve_ok"); // 좌석 점유 성공
const cntReserveFail = new Counter("reserve_fail"); // 좌석 점유 실패

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
    //   executor: "ramping-vus",
    //   startVUs: 0,
    //   stages: [
    //     { duration: "20s", target: 20 }, // 워밍업
    //     { duration: "30s", target: 100 }, // 부하 증가
    //     { duration: "10s", target: 0 }, // 정리
    //   ],
    // },
  },

  thresholds: {
    // P95 좌석 점유 응답시간 3초 이내
    reserve_duration_ms: ["p(95)<3000"],
    // HTTP 오류율 5% 미만 (4xx/5xx)
    http_req_failed: ["rate<0.05"],
    // 점유 실패 0건 목표 (초과 시 경고만)
    reserve_fail: ["count<1"],
  },
};

// ── 공통 헤더 ─────────────────────────────────────────────────────────────────
const headers = {
  "Content-Type": "application/json",
  Authorization: `Bearer ${JWT_TOKEN}`,
};

// ── seatId → eventId 변환 ────────────────────────────────────────────────────
function getEventId(seatId) {
  if (seatId <= 1000) return 1;
  if (seatId <= 2000) return 2;
  return 3;
}

// ── 메인 시나리오: VU 1명이 반복 실행 ────────────────────────────────────────
export default function () {
  // 전체 이터레이션 순번(0-based)으로 좌석번호를 순차 배정, 3000개 순환
  const seatId = (exec.scenario.iterationInTest % TOTAL_SEATS) + 1;
  // [충돌 시나리오] 10개 좌석(1~10)에 요청 집중 — 좌석 경합/충돌 한계 테스트용
  // const seatId = (exec.scenario.iterationInTest % 10) + 1;
  const eventId = getEventId(seatId);

  const start = Date.now();
  const res = http.post(
    `${BASE_URL}/api/seats/reserve`,
    JSON.stringify({ eventId, seatId }),
    { headers, tags: { name: "reserve" } },
  );
  const elapsed = Date.now() - start;

  // ── 상태 판단은 check와 분리 ──────────────────────────────
  const statusOk = res.status === 200;
  const reservationId = statusOk ? res.json("data.reservationEid") : null;
  const bodyOk = reservationId != null;
  const ok = statusOk && bodyOk;

  // check는 리포팅 전용으로만 사용
  check(res, {
    "reserve: 200 OK": () => statusOk,
    "reserve: reservationId exists": () => bodyOk,
  });

  // ── 레이턴시는 성공/실패 무관하게 항상 기록 ──────────────
  reserveDurationMs.add(elapsed);

  // ── 카운터는 명시적으로 분리 ─────────────────────────────
  if (ok) {
    cntReserveOk.add(1);
  } else {
    cntReserveFail.add(1);
    // 실패 시 디버깅용 로그 (VU 적을 때만 켜기)
    // console.log(`FAIL status=${res.status} body=${res.body.substring(0, 200)}`);
    // sleep(0.5);
  }
}

// ── 테스트 종료 후 요약 출력 ──────────────────────────────────────────────────
export function handleSummary(data) {
  const dur = data.metrics["reserve_duration_ms"];
  const reserveOk = data.metrics["reserve_ok"]?.values?.count ?? 0;
  const reserveFail = data.metrics["reserve_fail"]?.values?.count ?? 0;
  const totalIter = data.metrics["iterations"]?.values?.count ?? 0;

  const summary = [
    "══════════════════════════════════════════════",
    "  Reserve Load Test Summary",
    "══════════════════════════════════════════════",
    `  Iterations   : ${totalIter}`,
    `  Reserve OK   : ${reserveOk}`,
    `  Reserve Fail : ${reserveFail}`,
    "",
    `  Reserve Latency (Per Request)`,
    `    avg : ${ms(dur?.values?.avg)}`,
    `    p50 : ${ms(dur?.values?.["p(50)"] ?? dur?.values?.med)}`,
    `    p90 : ${ms(dur?.values?.["p(90)"])}`,
    `    p95 : ${ms(dur?.values?.["p(95)"])}`,
    `    p99 : ${ms(dur?.values?.["p(99)"])}`,
    "══════════════════════════════════════════════",
  ].join("\n");

  console.log(summary);
  return { stdout: summary, "k6/summary.json": JSON.stringify(data, null, 2) };
}

function ms(val) {
  if (val == null) return "N/A";
  if (val < 1000) return `${val.toFixed(2)}ms`; // 1초 미만은 ms로
  return `${(val / 1000).toFixed(2)}s`; // 1초 이상은 s로
}
