# k6 + InfluxDB + Grafana 부하 테스트 환경 구축 가이드

## 📋 목차
1. [개요](#개요)
2. [아키텍처](#아키텍처)
3. [Step 1: docker-compose.yml 설정](#step-1-docker-composeyml-설정)
4. [Step 2: 디렉토리 구조 생성](#step-2-디렉토리-구조-생성)
5. [Step 3: k6 테스트 스크립트 작성](#step-3-k6-테스트-스크립트-작성)
6. [Step 4: Grafana 프로비저닝 설정](#step-4-grafana-프로비저닝-설정)
7. [Step 5: 테스트 실행](#step-5-테스트-실행)
8. [Step 6: Grafana 대시보드 구성](#step-6-grafana-대시보드-구성)
9. [트러블슈팅](#트러블슈팅)

---

## 개요

### 사용 도구
- **k6**: 부하 테스트 도구
- **InfluxDB 1.8**: 시계열 데이터베이스 (k6 메트릭 저장)
- **Grafana**: 시각화 대시보드

### 데이터 흐름
```
k6 → InfluxDB → Grafana
```

---

## 아키텍처

```
┌─────────────┐
│     k6      │ (부하 테스트 실행)
└──────┬──────┘
       │ 메트릭 전송
       ↓
┌─────────────┐
│  InfluxDB   │ (시계열 데이터 저장)
└──────┬──────┘
       │ 쿼리
       ↓
┌─────────────┐
│   Grafana   │ (시각화)
└─────────────┘
```

---

## Step 1: docker-compose.yml 설정

### 1.1 네트워크 정의

docker-compose.yml 최상단에 네트워크 정의:

```yaml
networks:
  k6:        # k6와 InfluxDB 통신용
  grafana:   # Grafana와 InfluxDB 통신용
```

### 1.2 InfluxDB 서비스 추가

```yaml
services:
  influxdb:
    image: influxdb:1.8
    container_name: ecommerce-influxdb
    networks:
      - k6
      - grafana
    ports:
      - "8086:8086"
    environment:
      - INFLUXDB_DB=k6  # 기본 데이터베이스 이름
    restart: unless-stopped
```

**설명:**
- `influxdb:1.8`: InfluxDB 1.x 버전 (k6와 호환성 좋음)
- `INFLUXDB_DB=k6`: 초기 데이터베이스 자동 생성
- 두 개의 네트워크에 연결 (k6, grafana)

### 1.3 Grafana 서비스 추가

```yaml
  grafana:
    image: grafana/grafana:9.3.8
    container_name: ecommerce-grafana
    networks:
      - grafana
    ports:
      - "3000:3000"
    environment:
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Admin
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_BASIC_ENABLED=false
    volumes:
      - ./grafana:/etc/grafana/provisioning/
    restart: unless-stopped
```

**설명:**
- `GF_AUTH_ANONYMOUS_ENABLED=true`: 로그인 없이 접속 가능
- `./grafana:/etc/grafana/provisioning/`: 자동 설정 파일 마운트

### 1.4 k6 서비스 추가

```yaml
  k6:
    image: grafana/k6:latest
    container_name: ecommerce-k6
    networks:
      - k6
    ports:
      - "6565:6565"
    environment:
      - K6_OUT=influxdb=http://influxdb:8086/k6_test
    volumes:
      - ./k6/scripts/load-test.js:/scripts/load-test.js
    entrypoint: ["sh", "-c", "sleep 5 && k6 run /scripts/load-test.js"]
    profiles:
      - k6
```

**설명:**
- `K6_OUT`: InfluxDB 출력 설정 (데이터베이스 이름: k6_test)
- `profiles: - k6`: 기본 실행에서 제외 (명시적으로 실행할 때만)
- `sleep 5`: InfluxDB가 준비될 때까지 대기

---

## Step 2: 디렉토리 구조 생성

프로젝트 루트에서 다음 디렉토리 생성:

```bash
mkdir -p k6/scripts
mkdir -p grafana/datasources
mkdir -p grafana/dashboards
```

**디렉토리 구조:**
```
프로젝트루트/
├── k6/
│   └── scripts/
│       └── load-test.js
├── grafana/
│   ├── datasources/
│   │   └── influxdb.yml
│   └── dashboards/
│       └── dashboard.yml
└── docker-compose.yml
```

---

## Step 3: k6 테스트 스크립트 작성

### 3.1 기본 구조

`k6/scripts/load-test.js` 파일 생성:

```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// 커스텀 메트릭 정의
const errorRate = new Rate('errors');
const apiDuration = new Trend('api_duration');

// 테스트 옵션
export const options = {
  scenarios: {
    load_test: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 500,
      stages: [
        { duration: '30s', target: 50 },   // 30초간 50 RPS
        { duration: '1m', target: 100 },   // 1분간 100 RPS
        { duration: '30s', target: 0 },    // 30초간 쿨다운
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<500', 'p(99)<1000'],
    api_duration: ['p(95)<300', 'p(99)<600'],
    http_req_failed: ['rate<0.05'],
    errors: ['rate<0.05'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8080';

export default function () {
  // API 호출 예시
  const res = http.get(`${BASE_URL}/api/products`);

  // 메트릭 기록
  apiDuration.add(res.timings.duration);

  // 응답 검증
  const success = check(res, {
    'status is 200': (r) => r.status === 200,
  });

  if (!success) {
    errorRate.add(1);
  }

  sleep(1);
}
```

### 3.2 주요 구성 요소

#### a) 메트릭 정의
```javascript
import { Rate, Trend } from 'k6/metrics';

const errorRate = new Rate('errors');        // 에러율
const apiDuration = new Trend('api_duration'); // 응답 시간
```

**메트릭 타입:**
- `Rate`: 비율 (성공/실패율)
- `Trend`: 시간 측정 (p50, p95, p99 등)
- `Counter`: 카운터
- `Gauge`: 게이지 (현재 값)

#### b) 실행 모드 (executor)

**ramping-arrival-rate** (RPS 기반):
```javascript
{
  executor: 'ramping-arrival-rate',
  startRate: 10,      // 시작 RPS
  timeUnit: '1s',     // 시간 단위
  preAllocatedVUs: 50,
  maxVUs: 500,
  stages: [
    { duration: '1m', target: 100 },  // 1분간 100 RPS로 증가
  ],
}
```

**ramping-vus** (VU 기반):
```javascript
{
  executor: 'ramping-vus',
  startVUs: 0,
  stages: [
    { duration: '1m', target: 50 },   // 1분간 50명 VU로 증가
  ],
}
```

#### c) Thresholds (성공 기준)
```javascript
thresholds: {
  http_req_duration: ['p(95)<500'],     // p95 < 500ms
  http_req_failed: ['rate<0.05'],       // 실패율 < 5%
}
```

---

## Step 4: Grafana 프로비저닝 설정

### 4.1 InfluxDB 데이터 소스 자동 설정

`grafana/datasources/influxdb.yml` 파일 생성:

```yaml
apiVersion: 1

datasources:
  - name: k6_test
    type: influxdb
    access: proxy
    url: http://influxdb:8086
    database: k6_test
    isDefault: true
    editable: true
```

**설명:**
- `url: http://influxdb:8086`: Docker 네트워크 내부 주소 사용
- `database: k6_test`: k6가 데이터를 저장하는 DB 이름과 일치

### 4.2 대시보드 프로비저닝 설정

`grafana/dashboards/dashboard.yml` 파일 생성:

```yaml
apiVersion: 1

providers:
  - name: 'default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards
```

---

## Step 5: 테스트 실행

### 5.1 인프라 시작

```bash
# InfluxDB와 Grafana 시작
docker-compose up -d influxdb grafana
```

### 5.2 애플리케이션 준비

애플리케이션이 실행 중인지 확인:
```bash
curl http://localhost:8080/api/health
```

### 5.3 k6 테스트 실행

```bash
# k6 프로필로 실행
docker-compose --profile k6 up k6

# 또는 백그라운드 실행
docker-compose --profile k6 up -d k6

# 로그 확인
docker logs -f ecommerce-k6
```

### 5.4 테스트 중지

```bash
docker-compose stop k6
```

---

## Step 6: Grafana 대시보드 구성

### 6.1 Grafana 접속

브라우저에서 `http://localhost:3000` 접속

### 6.2 새 대시보드 생성

1. 좌측 메뉴 → **Dashboards** → **New Dashboard**
2. **Add visualization** 클릭
3. Data source: **k6_test** 선택

### 6.3 주요 패널 쿼리

#### Panel 1: RPS (초당 요청 수)
```sql
SELECT mean("value")
FROM "http_reqs"
WHERE $timeFilter
GROUP BY time(1s) fill(null)
```

#### Panel 2: p95 응답 시간
```sql
SELECT percentile("value", 95)
FROM "http_req_duration"
WHERE $timeFilter
GROUP BY time(5s) fill(null)
```

#### Panel 3: p99 응답 시간
```sql
SELECT percentile("value", 99)
FROM "http_req_duration"
WHERE $timeFilter
GROUP BY time(5s) fill(null)
```

#### Panel 4: 에러율
```sql
SELECT mean("value") * 100
FROM "errors"
WHERE $timeFilter
GROUP BY time(5s) fill(null)
```

#### Panel 5: HTTP 실패율
```sql
SELECT mean("value") * 100
FROM "http_req_failed"
WHERE $timeFilter
GROUP BY time(5s) fill(null)
```

#### Panel 6: Active VUs
```sql
SELECT mean("value")
FROM "vus"
WHERE $timeFilter
GROUP BY time(1s) fill(null)
```

#### Panel 7: 커스텀 메트릭 (api_duration p99)
```sql
SELECT percentile("value", 99)
FROM "api_duration"
WHERE $timeFilter
GROUP BY time(5s) fill(null)
```

### 6.4 패널 설정 팁

**시간 범위:**
- 우측 상단에서 시간 범위 조정 (예: Last 15 minutes)

**Refresh:**
- Auto-refresh 설정 (예: 5s)

**Unit 설정:**
- 응답 시간: `milliseconds (ms)`
- 에러율: `percent (0-100)`
- RPS: `requests/sec`

---

## Step 7: 고급 설정

### 7.1 여러 테스트 시나리오 관리

#### 시나리오별 서비스 분리

`docker-compose.yml`:
```yaml
  k6-scenario1:
    image: grafana/k6:latest
    container_name: k6-scenario1
    networks:
      - k6
    environment:
      - K6_OUT=influxdb=http://influxdb:8086/scenario1
    volumes:
      - ./k6/scripts/scenario1.js:/scripts/test.js
    entrypoint: ["sh", "-c", "sleep 5 && k6 run /scripts/test.js"]
    profiles:
      - scenario1

  k6-scenario2:
    image: grafana/k6:latest
    container_name: k6-scenario2
    networks:
      - k6
    environment:
      - K6_OUT=influxdb=http://influxdb:8086/scenario2
    volumes:
      - ./k6/scripts/scenario2.js:/scripts/test.js
    entrypoint: ["sh", "-c", "sleep 5 && k6 run /scripts/test.js"]
    profiles:
      - scenario2
```

실행:
```bash
docker-compose --profile scenario1 up k6-scenario1
docker-compose --profile scenario2 up k6-scenario2
```

### 7.2 환경 변수 활용

k6 스크립트에서:
```javascript
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TARGET_RPS = __ENV.TARGET_RPS || '100';
```

docker-compose.yml에서:
```yaml
  k6:
    environment:
      - BASE_URL=http://host.docker.internal:8083
      - TARGET_RPS=500
```

### 7.3 테스트 데이터 관리

#### 외부 데이터 파일 사용

`k6/data/users.json`:
```json
[
  {"userId": 1, "username": "user1"},
  {"userId": 2, "username": "user2"}
]
```

k6 스크립트:
```javascript
import { SharedArray } from 'k6/data';

const users = new SharedArray('users', function () {
  return JSON.parse(open('./data/users.json'));
});

export default function () {
  const user = users[Math.floor(Math.random() * users.length)];
  // user 데이터 사용
}
```

---

## 트러블슈팅

### 문제 1: Grafana에 데이터가 안 보임

**원인:**
- k6가 실행되지 않음
- InfluxDB 연결 실패
- 데이터베이스 이름 불일치

**해결:**
```bash
# 1. k6 로그 확인
docker logs ecommerce-k6

# 2. InfluxDB 데이터 확인
docker exec -it ecommerce-influxdb influx -execute 'SHOW DATABASES'
docker exec -it ecommerce-influxdb influx -database 'k6_test' -execute 'SHOW MEASUREMENTS'

# 3. Grafana 데이터 소스 테스트
# Grafana UI → Configuration → Data Sources → Test
```

### 문제 2: k6 네트워크 오류

**오류:**
```
Error response from daemon: network not found
```

**해결:**
```bash
docker-compose down --remove-orphans
docker network prune -f
docker-compose up -d
```

### 문제 3: 모든 요청이 실패

**원인:**
- 애플리케이션이 실행되지 않음
- BASE_URL이 잘못됨
- Docker 네트워크 통신 문제

**해결:**
```bash
# 1. 애플리케이션 확인
curl http://localhost:8080/api/health

# 2. k6에서 직접 테스트
docker exec -it ecommerce-k6 sh
# 컨테이너 안에서
curl http://host.docker.internal:8080/api/health

# 3. BASE_URL 확인
# macOS/Windows: http://host.docker.internal:8080
# Linux: http://172.17.0.1:8080 또는 --network host
```

### 문제 4: Redis 재고 부족

**증상:** 주문 API가 50% 실패

**해결:**
```bash
# Redis 재고 초기화
for i in {1..40}; do
  docker exec -it ecommerce-redis redis-cli SET product:$i:stock 100000
done
```

### 문제 5: DB 컬럼 누락 (version)

**오류:**
```
Unknown column 'version' in 'field list'
```

**해결:**
마이그레이션 파일 추가 (이미 완료):
```sql
-- V3__add_version_columns.sql
ALTER TABLE users ADD COLUMN version BIGINT DEFAULT 0 NOT NULL;
ALTER TABLE points ADD COLUMN version BIGINT DEFAULT 0 NOT NULL;
ALTER TABLE user_coupons ADD COLUMN version BIGINT DEFAULT 0 NOT NULL;
```

---

## 베스트 프랙티스

### 1. 점진적 부하 증가
```javascript
stages: [
  { duration: '1m', target: 10 },   // 워밍업
  { duration: '2m', target: 50 },   // 증가
  { duration: '2m', target: 100 },  // 증가
  { duration: '3m', target: 100 },  // 유지
  { duration: '1m', target: 0 },    // 쿨다운
]
```

### 2. Threshold 설정
```javascript
thresholds: {
  'http_req_duration{api:products}': ['p(95)<200'],
  'http_req_duration{api:orders}': ['p(95)<500'],
  http_req_failed: ['rate<0.01'],  // 1% 미만
}
```

### 3. 태그 활용
```javascript
http.get(`${BASE_URL}/api/products`, {
  tags: { api: 'products', type: 'read' }
});

http.post(`${BASE_URL}/api/orders`, payload, {
  tags: { api: 'orders', type: 'write' }
});
```

### 4. 시나리오 분리
```javascript
export const options = {
  scenarios: {
    read_heavy: {
      executor: 'constant-arrival-rate',
      rate: 100,
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 50,
      exec: 'readScenario',
    },
    write_heavy: {
      executor: 'constant-arrival-rate',
      rate: 20,
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 10,
      exec: 'writeScenario',
    },
  },
};

export function readScenario() {
  http.get(`${BASE_URL}/api/products`);
}

export function writeScenario() {
  http.post(`${BASE_URL}/api/orders`, payload);
}
```

---

## 참고 자료

- [k6 공식 문서](https://k6.io/docs/)
- [InfluxDB 1.8 문서](https://docs.influxdata.com/influxdb/v1.8/)
- [Grafana 문서](https://grafana.com/docs/)
- [k6 메트릭 가이드](https://k6.io/docs/using-k6/metrics/)