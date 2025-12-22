# 카프카 구조 및 동작 원리

## 1. 카프카 핵심 개념

### Producer (생산자)
- 메시지를 카프카 브로커에 적재하는 역할
- 메시지를 생성하여 특정 토픽으로 전송

### Consumer (소비자)
- 카프카 브로커에 적재된 메시지를 읽어오는 역할
- 파티션으로부터 순차적으로 메시지를 소비

### Broker (브로커)
- 카프카의 단위 서버
- 주요 역할:
  - Producer로부터 메시지를 받아 offset 지정 후 디스크에 저장
  - 메시지를 순차적으로 받아 key 값의 hash를 계산하여 파티션에 분배
  - Consumer의 파티션 Read 요청에 응답하여 디스크의 메시지 전달

### Topic (토픽)
- 브로커 내에서 논리적 단위로 여러 파티션들을 묶는 개념
- 예시: 쿠폰 발급, 결제 후 처리 등 비즈니스 도메인별로 구분

### Partition (파티션)
- Topic 내에서 물리적으로 메시지를 저장하는 단위
- 주요 특징:
  - Producer가 메시지 적재 시 key 값을 hash로 변환하여 특정 파티션에 할당
  - 메시지가 순차적으로 큐에 쌓이며 Consumer가 순차적으로 소비
  - **Leader Partition**: 1개의 리더 파티션이 읽기/쓰기 작업 담당
  - **Follower Partition**: 다른 브로커에 존재하는 복제본들
  - 메시지가 쌓일 때마다 Follower Partition에 복제되어 고가용성 보장

### Controller (컨트롤러)
- 클러스터 내 브로커 장애 감지 및 복구 담당
- 장애 발생 시:
  - 장애가 발생한 브로커 내 Leader Partition을 감지
  - 다른 브로커의 Follower Partition을 새로운 Leader로 승격

### Coordinator (코디네이터)
- Consumer 장애 발생 시 파티션 재할당 담당
- 파티션-Consumer 매칭 규칙:
  - **기본 원칙**: 1개 파티션 당 최대 1개의 Consumer 매칭
  - **이유**: 2개 이상 매칭 시 순차적으로 쌓인 메시지에 대한 동시성 이슈 발생

### Consumer Group (컨슈머 그룹)
- 서로 다른 관심사를 가진 Consumer들을 그룹으로 분리
- 장점:
  - 같은 파티션을 여러 Consumer Group이 독립적으로 소비 가능
  - 각 Consumer Group은 서로 다른 비즈니스 로직을 처리하므로 동시성 문제 없음
  - 예시: 주문 토픽을 결제팀과 배송팀이 각각 다른 목적으로 소비

## 2. Kafka의 역할과 장점

### 비동기 이벤트 처리
- 기존 방식: 한 트랜잭션 내에서 모든 비즈니스 로직 실행
- Kafka 활용:
  - 핵심 로직만 동기적으로 처리
  - 부가 로직은 Kafka를 통해 비동기 처리
  - 결과: 더 빠르고 효율적인 처리 가능

### MSA 환경에서의 관심사 분리
- 하나의 트랜잭션에 몰려있던 다양한 관심사를 분리
- 각 마이크로서비스가 독립적으로 이벤트를 소비하고 처리
- 느슨한 결합(Loose Coupling)을 통한 시스템 확장성 향상

## 3. 아키텍처 요약

```
Producer → Broker (Leader Partition) → Consumer
                ↓ (복제)
         Follower Partition
```

- **내결함성(Fault Tolerance)**: Follower Partition을 통한 데이터 복제
- **확장성(Scalability)**: 파티션 증가를 통한 처리량 증대
- **순서 보장(Ordering)**: 같은 파티션 내에서 메시지 순서 보장