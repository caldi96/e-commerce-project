# K6 Load Testing Setup for E-Commerce API

## Summary

This document provides a complete overview of the E-Commerce REST API endpoints and guidance for creating k6 load test scripts.

## Documentation Files Created

### 1. API_ENDPOINTS_FOR_K6_TESTING.md
**Purpose:** Comprehensive API reference with detailed endpoint specifications
**Contains:**
- Complete endpoint listing for all 8 controllers
- Request/response structures with examples
- HTTP status codes and validation rules
- Authentication/authorization status
- Field types and constraints
- Data dependency information
- Performance considerations

**Key Sections:**
- Cart Controller (4 endpoints)
- Product Controller (11 endpoints)
- Category Controller (5 endpoints)
- Order Controller (5 endpoints)
- Payment Controller (1 endpoint)
- Point Controller (3 endpoints)
- Coupon Controller (7 endpoints)
- Kafka Test Controller (2 endpoints)

### 2. API_QUICK_REFERENCE.md
**Purpose:** Quick lookup reference for developers
**Contains:**
- Endpoint summary table (38 total endpoints)
- Data flow for typical e-commerce scenarios
- Quick test scenarios (5 examples)
- Request/response field reference
- Common HTTP headers
- Error response format
- K6 test script patterns
- Quick curl command examples
- Performance considerations

### 3. K6_TEST_SCRIPT_EXAMPLES.js
**Purpose:** Working code examples for various load test scenarios
**Contains:**
- 7 detailed test functions with explanations:
  1. Product Browsing (Read-Heavy)
  2. Shopping Cart Operations
  3. Order Creation Flow
  4. Payment Processing
  5. Coupon First-Come-First-Served (Race Condition Testing)
  6. Loyalty Points
  7. Complete User Journey
- Setup and Teardown functions
- Stress test configuration
- Smoke test configuration
- Proper assertions and validations

## API Overview

### Base URL
```
http://localhost:8083
```

### Total Endpoints: 38

| Module | Count | Key Operations |
|--------|-------|-----------------|
| Cart | 4 | Create, Get, Update, Delete |
| Product | 11 | Create, List, Detail, Update, Stock, Activate, Delete |
| Category | 5 | CRUD operations |
| Order | 5 | Create (2 types), List, Detail, Cancel |
| Payment | 1 | Create payment |
| Point | 3 | Charge, Get balance, Get history |
| Coupon | 7 | CRUD, Issue (first-come-first-served) |
| Kafka Test | 2 | Send message, Health check |

### Key Features

1. **No Authentication Required** - All endpoints are public (development setup)
2. **Async Order Processing** - Orders use Kafka for async handling
3. **Redis-Backed Coupon Distribution** - Atomic first-come-first-served
4. **Pagination Support** - Products and orders support pagination
5. **Data Validation** - Comprehensive input validation on all endpoints

## Infrastructure

```
Service          | Port/Host
-----------------|------------------
Application      | localhost:8083
MySQL Database   | localhost:3307
Redis Cache      | localhost:6380
Kafka Brokers    | localhost:19093-19095
Consumer Group   | ecommerce-consumer-group
```

## Connection Pools

```
MySQL:
  - max-active: 10
  - max-idle: 10

Redis:
  - max-active: 10
  - max-idle: 10
  - min-idle: 2
```

## Request/Response Data Types

### Numeric IDs
- Long type (userId, productId, categoryId, etc.)
- Min value: 1

### Monetary Values
- BigDecimal type (price, amount, pointAmount)
- Min: 0 (inclusive)

### Quantities
- Integer/int type (quantity, stock)
- Min: 1

### Timestamps
- LocalDateTime format
- Example: "2025-12-23T10:30:00"

### Status Enums
- OrderStatus: PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED
- PaymentMethod: CREDIT_CARD, DEBIT_CARD, PAYPAL, BANK_TRANSFER
- DiscountType: PERCENTAGE, FIXED_AMOUNT
- ProductSortType: LATEST, BEST_SELLER, MOST_VIEWED, LOW_PRICE, HIGH_PRICE

## Test Scenarios

### Scenario 1: Product Browsing (Read-Heavy Load)
- GET /api/products with pagination
- GET /api/products/{id}
- GET /api/products/top-rank
- Best for: Simulating typical customer browsing behavior

### Scenario 2: Shopping Cart Operations
- POST /api/carts (add to cart)
- PATCH /api/carts/{cartId}/quantity (update)
- GET /api/carts/{userId} (view)
- DELETE /api/carts/{cartId} (remove)
- Best for: Testing cart management concurrency

### Scenario 3: Order Processing
- POST /api/orders/from-product (direct purchase)
- POST /api/orders/from-cart (cart checkout)
- GET /api/orders (list)
- POST /api/orders/{orderId}/cancel
- Best for: Testing async Kafka processing, order state management

### Scenario 4: Coupon Distribution (Race Condition)
- POST /api/coupons/issue (multiple concurrent attempts)
- Best for: Testing first-come-first-served via Redis atomicity

### Scenario 5: Complete User Journey
- Browse products
- Charge loyalty points
- Add to cart
- Create order
- Process payment
- Best for: End-to-end flow validation

## Using the Test Script Examples

### Basic Execution
```bash
# Run all tests with default configuration
k6 run K6_TEST_SCRIPT_EXAMPLES.js

# Run specific test function
k6 run K6_TEST_SCRIPT_EXAMPLES.js --duration 30s --vus 10

# Run with custom configuration
k6 run K6_TEST_SCRIPT_EXAMPLES.js --env BASE_URL=http://localhost:8083
```

### Run Specific Scenarios
```bash
# Run only product browsing test
k6 run K6_TEST_SCRIPT_EXAMPLES.js -e FUNCTIONS=productBrowsing

# Run smoke test
k6 run K6_TEST_SCRIPT_EXAMPLES.js --config stressTestOptions

# Run stress test
k6 run K6_TEST_SCRIPT_EXAMPLES.js --config stressTestOptions
```

### Output Results
```bash
# Save results to JSON
k6 run K6_TEST_SCRIPT_EXAMPLES.js --out json=results.json

# Stream to InfluxDB
k6 run K6_TEST_SCRIPT_EXAMPLES.js --out influxdb
```

## Critical Test Data Dependencies

Before running load tests, ensure:

1. **At least one product exists**
   ```bash
   POST /api/products
   {
     "name": "Test Product",
     "categoryId": 1,
     "price": 99.99,
     "stock": 1000,
     "minOrderQuantity": 1,
     "maxOrderQuantity": 10
   }
   ```

2. **Sufficient stock** - Tests assume productId=1 with adequate stock

3. **Test coupons created** - For coupon race condition tests

4. **User IDs** - Tests generate unique userIds using __VU variable

## Important Considerations for Load Testing

### Async Order Processing
- Orders return immediately with PENDING status
- Kafka handles actual processing asynchronously
- Tests should account for this delay (sleep(2) recommended after order creation)

### Redis Atomicity
- Coupon issuance uses Redis for atomic first-come-first-served
- Concurrent requests beyond stock limit will receive 409 Conflict
- This is expected behavior in race condition tests

### Connection Pool Saturation
- MySQL pool max-active=10
- Redis pool max-active=10
- Monitor these under high concurrent load
- May become bottleneck at 100+ concurrent users

### VU Isolation
- Each virtual user gets unique userId (__VU + offset)
- Prevents data conflicts in concurrent scenarios
- Ensures reproducible test results

## Example Test Commands

### Simple Connectivity Check
```bash
curl -X GET http://localhost:8083/api/test/kafka/health
```

### Create Test Category
```bash
curl -X POST http://localhost:8083/api/categories \
  -H "Content-Type: application/json" \
  -d '{
    "name":"Electronics",
    "displayOrder":1
  }'
```

### Create Test Product
```bash
curl -X POST http://localhost:8083/api/products \
  -H "Content-Type: application/json" \
  -d '{
    "name":"Laptop",
    "categoryId":1,
    "price":999.99,
    "stock":100,
    "minOrderQuantity":1,
    "maxOrderQuantity":5
  }'
```

### Create Test Coupon
```bash
curl -X POST http://localhost:8083/api/coupons \
  -H "Content-Type: application/json" \
  -d '{
    "name":"Test Coupon",
    "discountType":"FIXED_AMOUNT",
    "discountValue":10.00,
    "totalQuantity":1000,
    "perUserLimit":1,
    "startDate":"2025-12-23T00:00:00",
    "endDate":"2025-12-31T23:59:59"
  }'
```

## Troubleshooting

### "Product not found" errors
- Ensure at least one product exists (productId=1)
- Create test products before running load tests

### "User not found" errors
- Tests generate userIds automatically
- No pre-existing users required

### High latency on order operations
- Order creation is async (expected behavior)
- Wait 2+ seconds before querying order status
- Check Kafka consumer lag in logs

### Connection pool saturation
- Reduce concurrent VUs
- Increase connection pool sizes in configuration
- Check MySQL/Redis availability

### Coupon "Out of Stock" errors
- Expected when concurrent issuance exceeds totalQuantity
- Indicates successful rate limiting via Redis

## Security Notes

**Current Status:** No authentication/authorization implemented

For production deployment:
1. Implement JWT or OAuth2 authentication
2. Add @PreAuthorize annotations for role-based access
3. Secure payment endpoints with additional verification
4. Implement API rate limiting
5. Add request signing/verification
6. Use HTTPS for all communications

## Next Steps

1. Review API_ENDPOINTS_FOR_K6_TESTING.md for detailed endpoint specifications
2. Check API_QUICK_REFERENCE.md for quick lookups
3. Study K6_TEST_SCRIPT_EXAMPLES.js for implementation patterns
4. Create test-specific scripts for your load test scenarios
5. Run smoke tests first (validate API connectivity)
6. Execute stress tests with appropriate thresholds
7. Analyze results and optimize based on bottlenecks

## File Locations

```
Project Root: /Volumes/E 드라이브/study/ECommerce-project/

Documentation:
- API_ENDPOINTS_FOR_K6_TESTING.md (detailed reference)
- API_QUICK_REFERENCE.md (quick lookup)
- K6_TEST_SCRIPT_EXAMPLES.js (working examples)
- README_K6_TESTING.md (this file)

Source Code:
- src/main/java/.../cart/presentation/CartController.java
- src/main/java/.../product/presentation/ProductController.java
- src/main/java/.../order/presentation/OrderController.java
- src/main/java/.../category/presentation/CategoryController.java
- src/main/java/.../payment/presentation/PaymentController.java
- src/main/java/.../point/presentation/PointController.java
- src/main/java/.../coupon/presentation/CouponController.java
- src/main/java/.../common/test/KafkaTestController.java

Configuration:
- src/main/resources/application.yml
```

## Quick Reference

| Aspect | Value |
|--------|-------|
| Total Endpoints | 38 |
| Controllers | 8 |
| Base URL | http://localhost:8083 |
| Authentication | None (Development) |
| Database | MySQL (localhost:3307) |
| Cache | Redis (localhost:6380) |
| Message Queue | Kafka (localhost:19093-19095) |
| Request Format | JSON |
| Response Format | JSON |
| Async Processing | Kafka (Orders) |
| Race Condition Testing | Redis Coupons |
| Connection Pool Limit | 10 (MySQL/Redis) |

---

**Created:** 2025-12-23
**For:** K6 Load Testing of E-Commerce REST API
**Version:** 1.0

