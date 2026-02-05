# k6 부하 테스트 가이드

이 디렉토리는 ECommerce 프로젝트의 부하 테스트를 위한 k6 스크립트와 설정을 포함합니다.

## 목차

- [설치](#설치)
- [디렉토리 구조](#디렉토리-구조)
- [테스트 시나리오](#테스트-시나리오)
- [실행 방법](#실행-방법)
- [결과 분석](#결과-분석)

## 설치

### macOS (Homebrew)
```bash
brew install k6
```

### Windows (Chocolatey)
```bash
choco install k6
```

### Linux
```bash
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update
sudo apt-get install k6
```

### Docker
```bash
docker pull grafana/k6:latest
```

## 디렉토리 구조

```
k6/
├── scripts/              # 테스트 스크립트
│   ├── smoke-test.js           # 스모크 테스트 (기본 기능 검증)
│   ├── load-test.js            # 부하 테스트 (일반적인 트래픽)
│   ├── stress-test.js          # 스트레스 테스트 (한계 테스트)
│   ├── cart-order-scenario.js  # 장바구니/주문 시나리오
│   └── coupon-race-test.js     # 쿠폰 선착순 레이스 컨디션 테스트
├── data/                 # 테스트 데이터 (CSV 등)
├── results/              # 테스트 결과 저장
└── README.md             # 이 문서
```

## 테스트 시나리오

### 1. Smoke Test (smoke-test.js)
- **목적**: 최소 부하로 시스템의 기본 기능 검증
- **VU**: 1명
- **Duration**: 1분
- **대상**: 주요 GET 엔드포인트 (상품, 카테고리)

### 2. Load Test (load-test.js)
- **목적**: 일반적인 트래픽 패턴 시뮬레이션
- **VU**: 20 → 50명 (점진적 증가)
- **Duration**: 4분
- **시나리오**:
  - 상품 목록 조회
  - 상품 상세 조회
  - 카테고리 조회
  - 포인트 조회

### 3. Stress Test (stress-test.js)
- **목적**: 시스템의 한계점 파악
- **VU**: 50 → 300명 (점진적 증가)
- **Duration**: 19분
- **시나리오**: 읽기(60%) + 쓰기(40%) 혼합

### 4. Cart & Order Scenario (cart-order-scenario.js)
- **목적**: 실제 사용자 구매 플로우 테스트
- **VU**: 최대 30명
- **Duration**: 5분
- **시나리오**:
  1. 장바구니에 상품 추가
  2. 장바구니 조회
  3. 주문 생성 (30% 확률)
  4. 주문 조회

### 5. Coupon Race Test (coupon-race-test.js)
- **목적**: 선착순 쿠폰 발급 시 레이스 컨디션 테스트
- **Rate**: 초당 100개 요청
- **Duration**: 30초
- **목표**: Redis 기반 원자적 연산 검증

## 실행 방법

### 사전 준비
1. 애플리케이션 실행 확인:
```bash
# Docker Compose로 인프라 실행
docker-compose up -d

# Spring Boot 애플리케이션 실행
./gradlew bootRun
```

2. 애플리케이션이 `http://localhost:8083`에서 실행 중인지 확인

### 기본 실행

```bash
# Smoke Test
k6 run k6/scripts/smoke-test.js

# Load Test
k6 run k6/scripts/load-test.js

# Stress Test
k6 run k6/scripts/stress-test.js

# Cart & Order Scenario
k6 run k6/scripts/cart-order-scenario.js

# Coupon Race Test
k6 run k6/scripts/coupon-race-test.js
```

### 환경 변수 지정

```bash
# 다른 서버 대상 테스트
k6 run -e BASE_URL=http://localhost:8080 k6/scripts/load-test.js
```

### 결과를 JSON으로 저장

```bash
k6 run --out json=k6/results/load-test-result.json k6/scripts/load-test.js
```

### HTML 리포트 생성 (xk6-reporter 필요)

```bash
k6 run --out json=k6/results/result.json k6/scripts/load-test.js
```

### Docker로 실행

```bash
docker run --rm -i --network=host \
  -v $(pwd)/k6/scripts:/scripts \
  grafana/k6:latest run /scripts/load-test.js
```

## 결과 분석

### 주요 메트릭

- **http_req_duration**: HTTP 요청 응답 시간
  - p(95): 95%의 요청이 이 시간 이하
  - p(99): 99%의 요청이 이 시간 이하

- **http_req_failed**: 실패한 요청의 비율
  - 목표: < 1% (일반 부하), < 10% (스트레스)

- **http_reqs**: 초당 처리된 요청 수 (RPS)

- **vus**: 가상 사용자 수

### 임계값 (Thresholds)

각 스크립트는 다음과 같은 임계값을 설정합니다:

```javascript
thresholds: {
  http_req_duration: ['p(95)<500'],   // 95%가 500ms 이하
  http_req_failed: ['rate<0.1'],      // 실패율 10% 이하
}
```

임계값을 초과하면 k6는 종료 코드 1을 반환합니다.

## 모니터링

### Grafana + InfluxDB 연동 (선택사항)

1. InfluxDB 실행:
```bash
docker run -d -p 8086:8086 \
  -e INFLUXDB_DB=k6 \
  influxdb:1.8
```

2. k6 실행 시 InfluxDB로 전송:
```bash
k6 run --out influxdb=http://localhost:8086/k6 k6/scripts/load-test.js
```

3. Grafana 대시보드에서 결과 확인

## 베스트 프랙티스

1. **Smoke Test 먼저**: 항상 스모크 테스트부터 시작
2. **점진적 증가**: VU를 점진적으로 증가시켜 시스템 반응 관찰
3. **실제 데이터**: 가능한 실제 프로덕션과 유사한 데이터 사용
4. **모니터링**: 테스트 중 서버 리소스(CPU, 메모리, DB) 모니터링
5. **반복 실행**: 일관성 확인을 위해 여러 번 실행

## 트러블슈팅

### Connection Refused
- 애플리케이션이 실행 중인지 확인
- BASE_URL이 올바른지 확인

### Too Many Open Files
```bash
# macOS/Linux
ulimit -n 10000
```

### Docker 네트워크 이슈
```bash
# host 네트워크 모드 사용
docker run --network=host ...
```

## 참고 자료

- [k6 공식 문서](https://k6.io/docs/)
- [k6 예제](https://k6.io/docs/examples/)
- [k6 Best Practices](https://k6.io/docs/testing-guides/api-load-testing/)