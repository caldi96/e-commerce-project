# K6 Load Testing Documentation - File Index

## Overview

Complete REST API documentation for the Spring Boot E-Commerce project. Contains 38 endpoints across 8 controllers with comprehensive K6 load testing guidance.

**Total Documentation:** 5 files, 67 KB
**Analysis Date:** 2025-12-23
**API Endpoints:** 38 (fully documented)

---

## Documentation Files

### 1. README_K6_TESTING.md
**Size:** 11 KB
**Purpose:** Master guide and entry point
**Best For:** Getting started, understanding the full picture

**Contains:**
- Summary of all documentation files
- API overview (38 endpoints across 8 controllers)
- Infrastructure and connection pool details
- Request/response data types reference
- 5 test scenarios with descriptions
- Using the test script examples
- Critical test data dependencies
- Important load testing considerations
- Example test commands
- Troubleshooting guide
- Security notes and recommendations

**Start Here:** If you're new to this project

---

### 2. API_ENDPOINTS_FOR_K6_TESTING.md
**Size:** 19 KB
**Purpose:** Comprehensive API reference
**Best For:** Detailed endpoint specifications and implementation

**Contains:**
- Server configuration (base URL, ports)
- API overview (8 controllers, 40+ endpoints)
- Detailed specification for each endpoint:
  - HTTP method and path
  - Required parameters and query strings
  - Request/response JSON examples
  - HTTP status codes
  - Validation rules
- Authentication/authorization status
- Validation rules by entity type
- Response codes
- K6 test patterns
- File paths to source code
- Notes for K6 script development

**Reference:** When implementing specific endpoints

---

### 3. API_QUICK_REFERENCE.md
**Size:** 9.9 KB
**Purpose:** Quick lookup reference
**Best For:** Fast information retrieval during development

**Contains:**
- Endpoint summary table (all 38 endpoints)
- Typical e-commerce data flow diagram
- 5 quick test scenarios
- Request/response field reference
  - Numeric IDs (type, constraints)
  - Monetary values (type, constraints)
  - Quantities (type, constraints)
  - Pagination parameters
  - Status/type enums
  - Timestamps format
- Common HTTP headers
- Error response format
- K6 test script patterns
- Performance considerations
- Special considerations
- File locations
- Quick curl command examples
- Notes

**Reference:** Keep this open while coding

---

### 4. K6_TEST_SCRIPT_EXAMPLES.js
**Size:** 13 KB
**Purpose:** Working K6 test script code
**Best For:** Understanding K6 syntax and test patterns

**Contains:**
- 7 detailed test functions with full implementation:
  1. `productBrowsing()` - Read-heavy product listing/browsing
  2. `shoppingCart()` - Cart CRUD operations
  3. `orderCreation()` - Order creation and retrieval
  4. `paymentProcessing()` - Payment flow
  5. `couponRace()` - First-come-first-served race condition
  6. `loyaltyPoints()` - Points management
  7. `completeJourney()` - Full user journey
- `setup()` function - Test data preparation
- `teardown()` function - Cleanup
- Test configurations:
  - Default test configuration
  - Stress test options
  - Smoke test options
- Proper HTTP checks and assertions
- Comments explaining each step

**Usage:** Copy functions into your test scripts

---

### 5. DOCUMENTATION_SUMMARY.txt
**Size:** 14 KB
**Purpose:** Executive summary of findings
**Best For:** Quick overview of key findings

**Contains:**
- Project summary
- Findings summary (38 endpoints, 8 controllers)
- Complete controller breakdown
- Documentation file descriptions
- Key findings by category
- Test scenarios overview
- Special features tested
- Connection pool information
- Critical considerations for K6 testing
- File locations
- Quick start commands
- Recommendations
- Security notes
- Conclusion

**Reference:** For quick fact checking and summaries

---

## How to Use These Files

### For Quick Lookups
1. **API_QUICK_REFERENCE.md** - Find endpoint paths, parameters, data types
2. **Documentation Summary** - Get quick facts about the API

### For Implementation
1. **API_ENDPOINTS_FOR_K6_TESTING.md** - Full endpoint specifications with examples
2. **K6_TEST_SCRIPT_EXAMPLES.js** - Copy patterns from working examples

### For Learning
1. **README_K6_TESTING.md** - Understand the big picture
2. **K6_TEST_SCRIPT_EXAMPLES.js** - See how to structure tests
3. **API_QUICK_REFERENCE.md** - Learn data structures

### For Reference
1. **API_ENDPOINTS_FOR_K6_TESTING.md** - Detailed specs
2. **API_QUICK_REFERENCE.md** - Quick field reference
3. **K6_TEST_SCRIPT_EXAMPLES.js** - Pattern examples

---

## Quick Facts

### API Summary
- **Total Endpoints:** 38
- **Controllers:** 8
- **Base URL:** http://localhost:8083
- **Authentication:** None (public endpoints)

### Infrastructure
- **Application:** localhost:8083
- **Database:** MySQL (localhost:3307)
- **Cache:** Redis (localhost:6380)
- **Message Queue:** Kafka (localhost:19093-19095)

### Connection Pools
- **MySQL:** max-active=10, max-idle=10
- **Redis:** max-active=10, max-idle=10, min-idle=2

### Key Features
- Async order processing via Kafka
- Redis-backed coupon distribution (first-come-first-served)
- Pagination on products, orders, points
- Full CRUD operations on most resources

---

## Controller Endpoints

### Cart (4 endpoints)
- POST /api/carts
- GET /api/carts/{userId}
- PATCH /api/carts/{cartId}/quantity
- DELETE /api/carts/{cartId}

### Product (11 endpoints)
- POST, GET, GET/:id, GET/top-rank, PUT, PATCH/price
- PATCH/stock/increase, PATCH/stock/decrease
- POST/:id/activate, POST/:id/deactivate, DELETE

### Category (5 endpoints)
- POST, GET, GET/:id, PUT, DELETE

### Order (5 endpoints)
- POST/from-cart, POST/from-product, GET, GET/:id, POST/:id/cancel

### Payment (1 endpoint)
- POST /api/payments

### Point (3 endpoints)
- POST /charge, GET /balance, GET /history

### Coupon (7 endpoints)
- POST, GET, GET/:id, PUT, POST/issue
- PATCH/:id/deactivate, PATCH/:id/activate

### Kafka Test (2 endpoints)
- GET /send, GET /health

---

## Test Scenarios Provided

1. **Product Browsing** - Read-heavy load test
2. **Shopping Cart** - CRUD operations
3. **Order Processing** - Async order handling
4. **Coupon Distribution** - Race condition testing
5. **Loyalty Points** - Points management
6. **Complete Journey** - End-to-end flow

---

## Common Patterns in Tests

### Test Structure
```javascript
group('Test Name', () => {
  // 1. Make request
  let res = http.method(url, payload, options);
  
  // 2. Check response
  check(res, {
    'condition': (r) => assertion
  });
  
  // 3. Parse and extract data
  const data = JSON.parse(res.body);
  
  // 4. Sleep
  sleep(duration);
});
```

### Data Isolation
- Use `__VU` variable for unique per-virtual-user data
- userId = __VU + offset (prevents conflicts)
- Enables reproducible concurrent tests

### Async Handling
- Orders return PENDING immediately
- Use `sleep(2)` after order creation
- Query order status after sleep

### Error Handling
- 400: Validation errors (expected in tests)
- 404: Resource not found
- 409: Rate limiting (expected for coupons)
- 500: Server errors

---

## Critical Test Data

Before running K6 tests, ensure:

1. **Create Product**
   ```bash
   POST /api/products
   {
     "name": "Test Product",
     "price": 99.99,
     "stock": 1000,
     "minOrderQuantity": 1,
     "maxOrderQuantity": 10
   }
   ```

2. **Create Category** (optional but recommended)
   ```bash
   POST /api/categories
   {
     "name": "Test Category",
     "displayOrder": 1
   }
   ```

3. **Create Coupon** (for coupon tests)
   ```bash
   POST /api/coupons
   {
     "name": "Test Coupon",
     "discountType": "FIXED_AMOUNT",
     "discountValue": 10.00,
     "totalQuantity": 1000,
     "perUserLimit": 1,
     "startDate": "2025-12-23T00:00:00",
     "endDate": "2025-12-31T23:59:59"
   }
   ```

---

## Validation Rules Quick Reference

### Numeric IDs
- Type: Long
- Min: 1
- Fields: userId, productId, categoryId, couponId, cartId, orderId

### Monetary Values
- Type: BigDecimal
- Min: 0 (inclusive)
- Fields: price, amount, pointAmount, discountValue

### Quantities
- Type: Integer
- Min: 1
- Fields: quantity, stock, totalQuantity

### Required Fields
- All major fields use @NotNull or @NotBlank
- See API_ENDPOINTS_FOR_K6_TESTING.md for details

---

## Performance Bottlenecks

### Connection Pools
- MySQL: 10 max concurrent connections
- Redis: 10 max concurrent connections
- Tests with 100+ VUs may saturate pools

### Recommendations
- Monitor pool utilization
- Start with 10-20 VUs
- Gradually increase to find breaking point
- Document results for capacity planning

---

## Running K6 Tests

### Basic
```bash
k6 run K6_TEST_SCRIPT_EXAMPLES.js
```

### Custom VUs and Duration
```bash
k6 run K6_TEST_SCRIPT_EXAMPLES.js --vus 50 --duration 1m
```

### Export Results
```bash
k6 run K6_TEST_SCRIPT_EXAMPLES.js --out json=results.json
```

---

## File Locations

All files located in:
`/Volumes/E 드라이브/study/ECommerce-project/`

Documentation files:
- README_K6_TESTING.md
- API_ENDPOINTS_FOR_K6_TESTING.md
- API_QUICK_REFERENCE.md
- K6_TEST_SCRIPT_EXAMPLES.js
- DOCUMENTATION_SUMMARY.txt
- K6_DOCUMENTATION_INDEX.md (this file)

Source code:
- src/main/java/.../cart/presentation/CartController.java
- src/main/java/.../product/presentation/ProductController.java
- src/main/java/.../order/presentation/OrderController.java
- src/main/java/.../category/presentation/CategoryController.java
- src/main/java/.../payment/presentation/PaymentController.java
- src/main/java/.../point/presentation/PointController.java
- src/main/java/.../coupon/presentation/CouponController.java
- src/main/java/.../common/test/KafkaTestController.java

---

## Next Steps

1. Read README_K6_TESTING.md for overview
2. Check API_QUICK_REFERENCE.md for endpoint summaries
3. Study K6_TEST_SCRIPT_EXAMPLES.js for patterns
4. Create test data (product, category, coupon)
5. Run smoke test (validate connectivity)
6. Execute basic load test (10-20 VUs)
7. Analyze results and adjust
8. Run stress test (100+ VUs)
9. Document findings

---

## Support Files

For source code, refer to the controller files in:
`src/main/java/io/hhplus/ECommerce/ECommerce_project/`

Each controller has:
- Request classes (in presentation/request/)
- Response classes (in presentation/response/)
- Clear @RequestMapping and @GetMapping/@PostMapping annotations

---

**Created:** 2025-12-23
**Version:** 1.0
**Status:** Ready for Use

