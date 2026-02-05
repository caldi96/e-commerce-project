#!/bin/bash

# Pinpoint Agent 경로
AGENT_PATH="/Volumes/E 드라이브/study/ECommerce-project/pinpoint-agent/pinpoint-agent-2.5.4"

# Spring Boot 애플리케이션 실행
./gradlew bootRun \
  -Pagent="-javaagent:${AGENT_PATH}/pinpoint-bootstrap-2.5.4.jar" \
  -PagentId="ecommerce-api-001" \
  -PapplicationName="ECommerce-API"