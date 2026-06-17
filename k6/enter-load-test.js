import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// ── 설정 ────────────────────────────────────────────────────────────────────
const QUEUE_BASE_URL  = __ENV.QUEUE_BASE_URL  || 'http://192.168.1.201:80';
const EVENT_ID        = __ENV.EVENT_ID        || 'bts_2026_seoul';
const USER_ID_START   = parseInt(__ENV.USER_ID_START  || '1000001', 10);
const QUEUE_TIMEOUT_S = parseInt(__ENV.QUEUE_TIMEOUT_S || '60',     10);

// ── 커스텀 메트릭 ────────────────────────────────────────────────────────────
// 큐 진입 ~ JWT 수신까지 실제 대기 시간
const queueWaitMs  = new Trend('queue_wait_ms', true);
const cntTokenOk   = new Counter('token_ok');    // JWT 정상 수신
const cntTokenFail = new Counter('token_fail');  // 타임아웃 or 파싱 실패

// ── 부하 시나리오 ────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    // 시나리오 A: 일정 동시 사용자 유지 (처리량/TPS 측정용)
    steady_load: {
      executor: 'constant-vus',
      vus: 50,
      duration: '60s',
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
    // P95 대기시간 60초 이내
    queue_wait_ms:   ['p(95)<60000'],
    // HTTP 오류율 5% 미만
    http_req_failed: ['rate<0.05'],
    // JWT 수신 실패 0건 목표 (초과 시 경고)
    token_fail:      ['count<1'],
  },
};

// ── SSE 응답 바디에서 JWT 추출 ────────────────────────────────────────────────
// k6에는 네이티브 EventSource가 없으므로 http.get()으로 SSE 스트림 전체를 수신한 뒤
// 텍스트를 파싱한다. 큐 서버가 JWT 전송 후 커넥션을 닫는 구조일 때 동작한다.
function extractJwtFromSseBody(body) {
  if (!body) return null;
  for (const line of body.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed.startsWith('data:')) continue;
    const data = trimmed.slice(5).trim();
    if (!data) continue;
    try {
      const obj = JSON.parse(data);
      const d   = obj?.data || obj || {};
      const jwt = d.jwt || d.token || d.accessToken || d.jwtToken;
      if (jwt) return jwt;
    } catch (_) {
      // JSON이 아니면 JWT 3-part 형식 확인
      if (data.split('.').length === 3) return data;
    }
  }
  return null;
}

// ── 메인 시나리오: VU 1명이 반복 실행 ────────────────────────────────────────
export default function () {
  // VU × ITER 조합으로 고유한 user_id 생성 (충돌 방지)
  const userId = USER_ID_START + (__VU - 1) * 10000 + __ITER;
  const url    = `${QUEUE_BASE_URL}/enter?event_id=${EVENT_ID}&user_id=${userId}`;

  const start = Date.now();
  const res   = http.get(url, {
    headers: {
      Accept:          'text/event-stream',
      'Cache-Control': 'no-cache',
    },
    // QUEUE_TIMEOUT_S 경과 후 서버 미종료 시 k6 자체 타임아웃으로 차단
    timeout: `${QUEUE_TIMEOUT_S + 5}s`,
    tags:    { name: 'queue_enter' },
  });
  const elapsed = Date.now() - start;

  check(res, { 'queue_enter: 200': (r) => r.status === 200 });

  if (res.status !== 200) {
    cntTokenFail.add(1);
    return;
  }

  const jwt = extractJwtFromSseBody(res.body);
  if (jwt) {
    queueWaitMs.add(elapsed);
    cntTokenOk.add(1);
    check(jwt, { 'jwt: 3-part token': (t) => t.split('.').length === 3 });
  } else {
    cntTokenFail.add(1);
  }
}

// ── 테스트 종료 후 요약 출력 ──────────────────────────────────────────────────
export function handleSummary(data) {
  const dur       = data.metrics['queue_wait_ms'];
  const tokenOk   = data.metrics['token_ok']?.values?.count   ?? 0;
  const tokenFail = data.metrics['token_fail']?.values?.count  ?? 0;
  const totalIter = data.metrics['iterations']?.values?.count  ?? 0;

  const summary = [
    '══════════════════════════════════════════════',
    '  Enter (Queue SSE) Load Test Summary',
    '══════════════════════════════════════════════',
    `  Iterations   : ${totalIter}`,
    `  Token OK     : ${tokenOk}`,
    `  Token Fail   : ${tokenFail}`,
    '',
    `  Queue Wait Latency`,
    `    avg : ${ms(dur?.values?.avg)}`,
    `    p50 : ${ms(dur?.values?.['p(50)'] ?? dur?.values?.med)}`,
    `    p90 : ${ms(dur?.values?.['p(90)'])}`,
    `    p95 : ${ms(dur?.values?.['p(95)'])}`,
    `    p99 : ${ms(dur?.values?.['p(99)'])}`,
    '══════════════════════════════════════════════',
  ].join('\n');

  console.log(summary);
  return { stdout: summary, 'k6/summary.json': JSON.stringify(data, null, 2) };
}

function ms(val) {
  if (val == null) return 'N/A';
  return `${(val / 1000).toFixed(2)}s`;
}
