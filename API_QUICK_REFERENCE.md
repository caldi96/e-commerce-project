# E-Commerce API Quick Reference for K6 Testing

## Endpoint Summary Table

| # | Method | Endpoint | Description | Status | Auth |
|----|--------|----------|-------------|--------|------|
| **CART (4 endpoints)** |
| 1 | POST | `/api/carts` | Create cart item | 200 | None |
| 2 | GET | `/api/carts/{userId}` | Get user's cart | 200 | None |
| 3 | PATCH | `/api/carts/{cartId}/quantity` | Update quantity | 200 | None |
| 4 | DELETE | `/api/carts/{cartId}` | Delete cart item | 204 | None |
| **PRODUCT (11 endpoints)** |
| 5 | POST | `/api/products` | Create product | 200 | None |
| 6 | GET | `/api/products` | List products (paginated) | 200 | None |
| 7 | GET | `/api/products/{id}` | Get product detail | 200 | None |
| 8 | GET | `/api/products/top-rank` | Get top ranked products | 200 | None |
| 9 | PUT | `/api/products/{id}` | Update product | 200 | None |
| 10 | PATCH | `/api/products/{id}/price` | Update price | 200 | None |
| 11 | PATCH | `/api/products/{id}/stock/increase` | Increase stock | 200 | None |
| 12 | PATCH | `/api/products/{id}/stock/decrease` | Decrease stock | 200 | None |
| 13 | POST | `/api/products/{id}/activate` | Activate product | 200 | None |
| 14 | POST | `/api/products/{id}/deactivate` | Deactivate product | 200 | None |
| 15 | DELETE | `/api/products/{id}` | Delete product | 204 | None |
| **CATEGORY (5 endpoints)** |
| 16 | POST | `/api/categories` | Create category | 200 | None |
| 17 | GET | `/api/categories` | List categories | 200 | None |
| 18 | GET | `/api/categories/{id}` | Get category | 200 | None |
| 19 | PUT | `/api/categories/{id}` | Update category | 200 | None |
| 20 | DELETE | `/api/categories/{id}` | Delete category | 204 | None |
| **ORDER (5 endpoints)** |
| 21 | POST | `/api/orders/from-cart` | Create order from cart | 201 | None |
| 22 | POST | `/api/orders/from-product` | Create order from product | 201 | None |
| 23 | GET | `/api/orders` | List orders (paginated) | 200 | None |
| 24 | GET | `/api/orders/{orderId}` | Get order detail | 200 | None |
| 25 | POST | `/api/orders/{orderId}/cancel` | Cancel order | 204 | None |
| **PAYMENT (1 endpoint)** |
| 26 | POST | `/api/payments` | Create payment | 201 | None |
| **POINT (3 endpoints)** |
| 27 | POST | `/api/points/charge` | Charge points | 200 | None |
| 28 | GET | `/api/points/balance` | Get point balance | 200 | None |
| 29 | GET | `/api/points/history` | Get point history | 200 | None |
| **COUPON (7 endpoints)** |
| 30 | POST | `/api/coupons` | Create coupon | 200 | None |
| 31 | GET | `/api/coupons` | List coupons | 200 | None |
| 32 | GET | `/api/coupons/{id}` | Get coupon detail | 200 | None |
| 33 | PUT | `/api/coupons/{id}` | Update coupon | 200 | None |
| 34 | POST | `/api/coupons/issue` | Issue coupon (first-come-first-served) | 200 | None |
| 35 | PATCH | `/api/coupons/{id}/deactivate` | Deactivate coupon | 200 | None |
| 36 | PATCH | `/api/coupons/{id}/activate` | Activate coupon | 200 | None |
| **KAFKA TEST (2 endpoints)** |
| 37 | GET | `/api/test/kafka/send` | Send test message | 200 | None |
| 38 | GET | `/api/test/kafka/health` | Check Kafka health | 200 | None |

**Total: 38 REST API Endpoints**

---

## Data Flow for Typical E-Commerce Scenario

```
1. Category Setup
   POST /api/categories -> categoryId
   
2. Product Setup
   POST /api/products (with categoryId) -> productId
   
3. User Preparation
   POST /api/points/charge -> balance
   
4. Coupon Setup
   POST /api/coupons -> couponId
   
5. Shopping
   POST /api/carts -> cartId
   PATCH /api/carts/{cartId}/quantity (optional)
   
6. Checkout
   POST /api/orders/from-cart -> orderId
   (or POST /api/orders/from-product)
   
7. Payment
   POST /api/payments -> paymentId
   
8. Order Tracking
   GET /api/orders?userId={userId}
   GET /api/orders/{orderId}
```

---

## Quick Test Scenarios

### Scenario 1: Product Browsing (Read-Heavy)
- GET /api/products (with different sortType, categoryId, pagination)
- GET /api/products/{id}
- GET /api/products/top-rank

### Scenario 2: Simple Purchase (Positive Path)
- POST /api/carts (add to cart)
- POST /api/orders/from-cart (create order)
- POST /api/payments (complete payment)

### Scenario 3: Direct Purchase
- POST /api/orders/from-product (skip cart)
- POST /api/payments

### Scenario 4: Loyalty Program
- POST /api/points/charge
- GET /api/points/balance
- GET /api/points/history

### Scenario 5: Promotional Campaign
- POST /api/coupons (create coupon)
- POST /api/coupons/issue (users grab coupons - race condition test)
- POST /api/orders/from-product (with couponId)

---

## Request/Response Field Reference

### Commonly Used Fields by Type

#### Numeric IDs
- userId, productId, categoryId, couponId, cartId, orderId
- Always Long type
- Required for most operations

#### Monetary Values
- price, amount, pointAmount, discountValue
- BigDecimal type
- Min: 0 (inclusive)

#### Quantities
- quantity, stock, totalQuantity, minOrderQuantity, maxOrderQuantity
- Integer/int type
- Min: 1

#### Pagination
- page: Default 0
- size: Default varies (10-20)
- totalElements, totalPages: Calculated

#### Status/Type Enums
- OrderStatus: PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED
- PaymentMethod: CREDIT_CARD, DEBIT_CARD, PAYPAL, BANK_TRANSFER
- DiscountType: PERCENTAGE, FIXED_AMOUNT
- ProductSortType: LATEST, BEST_SELLER, MOST_VIEWED, LOW_PRICE, HIGH_PRICE

#### Timestamps
- createdAt, updatedAt: LocalDateTime format
- Format: "2025-12-23T10:30:00"

---

## Common HTTP Headers

```
Content-Type: application/json
Accept: application/json
```

No authentication headers required (no JWT, no Bearer token needed)

---

## Error Response Format

```json
{
  "message": "Error description",
  "code": "ERROR_CODE",
  "timestamp": "2025-12-23T10:30:00"
}
```

Common errors:
- 400: Validation errors (missing fields, invalid values)
- 404: Resource not found
- 409: Conflict (e.g., coupon out of stock)
- 500: Server errors

---

## K6 Test Script Patterns

### Setup Phase
```javascript
// Create test data
- Create category
- Create products
- Create coupons
- Charge points to test users
```

### Load Testing Phase
```javascript
// High-volume operations
- Browse products (parallel GET requests)
- Add to cart (concurrent POST)
- Create orders (async processing)
- Issue coupons (Redis race condition)
```

### Teardown Phase
```javascript
// Cleanup
- Cancel orders
- Deactivate coupons
- Delete products
- Delete categories
```

### Data Isolation
```javascript
// Use unique identifiers
- userId: Generate per VU (virtual user)
- productId: Use existing or create dedicated ones
- timestamps: Use Math.random() or __VU for variation
```

---

## Performance Considerations

### Connection Pools
- MySQL: max-active=10
- Redis: max-active=10, max-idle=10
- Watch for saturation under high load

### Asynchronous Processing
- Order creation: Immediate response (PENDING), Kafka handles async
- Coupon issuance: Redis immediate, DB eventual consistency
- Test design should account for these delays

### Redis Optimization
- Used for: Caching, coupon distribution, stock management
- Atomic operations for coupon issuance
- Connection pool limits concurrent access

### Kafka Topics
- Multiple partitions for parallel processing
- Consumer group: ecommerce-consumer-group
- Test concurrent message production

---

## Special Considerations

### Cart Operations
- No foreign key validation enforced in some cases
- Consider race conditions with concurrent updates to same cartId
- userId parameter validates ownership

### Order Async Processing
- Returned immediately with PENDING status
- Actual processing via Kafka
- Query status later or listen for completion events

### Coupon First-Come-First-Served
- Redis ensures atomic issuance
- Per-user limits enforced
- Test high concurrency (common in flash sales)

### Stock Management
- Transactional for decrease/increase
- Consider overlap between cart and direct orders
- Test concurrent stock modifications

---

## File Locations for Reference

**Controller Source Code:**
```
/Volumes/E 드라이브/study/ECommerce-project/src/main/java/io/hhplus/ECommerce/ECommerce_project/
├── cart/presentation/CartController.java
├── product/presentation/ProductController.java
├── order/presentation/OrderController.java
├── category/presentation/CategoryController.java
├── payment/presentation/PaymentController.java
├── point/presentation/PointController.java
├── coupon/presentation/CouponController.java
└── common/test/KafkaTestController.java
```

**Configuration:**
```
/Volumes/E 드라이브/study/ECommerce-project/src/main/resources/application.yml
```

---

## Quick Test Commands Examples

### Test connectivity
```bash
curl -X GET http://localhost:8083/api/test/kafka/health
```

### Create a category
```bash
curl -X POST http://localhost:8083/api/categories \
  -H "Content-Type: application/json" \
  -d '{"name":"Electronics","displayOrder":1}'
```

### Create a product
```bash
curl -X POST http://localhost:8083/api/products \
  -H "Content-Type: application/json" \
  -d '{"name":"Laptop","categoryId":1,"price":999.99,"stock":50,"minOrderQuantity":1,"maxOrderQuantity":5}'
```

### Create cart item
```bash
curl -X POST http://localhost:8083/api/carts \
  -H "Content-Type: application/json" \
  -d '{"userId":1,"productId":1,"quantity":2}'
```

### Create order from product
```bash
curl -X POST http://localhost:8083/api/orders/from-product \
  -H "Content-Type: application/json" \
  -d '{"userId":1,"productId":1,"quantity":2}'
```

### Create payment
```bash
curl -X POST http://localhost:8083/api/payments \
  -H "Content-Type: application/json" \
  -d '{"orderId":1,"paymentMethod":"CREDIT_CARD"}'
```

---

## Notes

- **No authentication required** - All endpoints are public (development setup)
- **Async order processing** - Orders created with Kafka async, returns PENDING immediately
- **Redis-backed coupon distribution** - Atomicity guaranteed for first-come scenarios
- **Database**: MySQL on port 3307
- **Cache**: Redis on port 6380
- **Message Queue**: Kafka on ports 19093, 19094, 19095

