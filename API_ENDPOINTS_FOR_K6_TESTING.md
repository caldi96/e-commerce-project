# E-Commerce REST API Endpoints - K6 Load Test Reference

## Server Configuration
- Base URL: `http://localhost:8083`
- Server Port: 8083
- Database: MySQL (localhost:3307)
- Cache: Redis (localhost:6380)
- Message Queue: Kafka (localhost:19093, 19094, 19095)

## API Overview
The application has **8 REST controllers** with **40+ endpoints** organized into the following modules:

1. **Cart** - Shopping cart management
2. **Product** - Product catalog and inventory
3. **Order** - Order creation and management
4. **Category** - Product category management
5. **Payment** - Payment processing
6. **Point** - User loyalty points
7. **Coupon** - Coupon management and distribution
8. **Kafka Test** - Testing/debugging endpoints

---

## 1. CART CONTROLLER
**Base Path:** `/api/carts`
**Authentication:** None (No @PreAuthorize or security annotations)

### 1.1 Create Cart Item
```
POST /api/carts
Content-Type: application/json

Request Body:
{
  "userId": 1,           // Required, Min: 1
  "productId": 5,        // Required, Min: 1
  "quantity": 3          // Required, Min: 1
}

Response: 200 OK
{
  "id": 101,
  "userId": 1,
  "productId": 5,
  "quantity": 3,
  "createdAt": "2025-12-23T10:30:45"
}
```

### 1.2 Get User's Cart Items
```
GET /api/carts/{userId}
Path Parameters: userId (Long)

Response: 200 OK
[
  {
    "id": 101,
    "userId": 1,
    "productId": 5,
    "quantity": 3,
    "createdAt": "2025-12-23T10:30:45"
  }
]
```

### 1.3 Update Cart Item Quantity
```
PATCH /api/carts/{cartId}/quantity
Content-Type: application/json
Path Parameters: cartId (Long)

Request Body:
{
  "userId": 1,           // Required, Min: 1
  "quantity": 5          // Required, Min: 1
}

Response: 200 OK
{
  "id": 101,
  "userId": 1,
  "quantity": 5,
  "updatedAt": "2025-12-23T10:35:20"
}
```

### 1.4 Delete Cart Item
```
DELETE /api/carts/{cartId}?userId={userId}
Path Parameters: cartId (Long)
Query Parameters: userId (Long, Required)

Response: 204 No Content
```

---

## 2. PRODUCT CONTROLLER
**Base Path:** `/api/products`
**Authentication:** None

### 2.1 Create Product
```
POST /api/products
Content-Type: application/json

Request Body:
{
  "name": "Laptop",                    // Required, NotBlank
  "categoryId": 2,                     // Optional
  "description": "High performance laptop",
  "price": 999.99,                     // Required, Min: 0
  "stock": 50,                         // Min: 0
  "minOrderQuantity": 1,               // Required, Min: 1
  "maxOrderQuantity": 5                // Required, Min: 1
}

Response: 200 OK
{
  "id": 101,
  "categoryId": 2,
  "name": "Laptop",
  "description": "High performance laptop",
  "price": 999.99,
  "stock": 50,
  "soldCount": 0,
  "viewCount": 0,
  "isActive": true,
  "minOrderQuantity": 1,
  "maxOrderQuantity": 5,
  "createdAt": "2025-12-23T10:30:00",
  "updatedAt": "2025-12-23T10:30:00"
}
```

### 2.2 Get Product List (with pagination, filtering, sorting)
```
GET /api/products?categoryId={categoryId}&sortType={sortType}&page={page}&size={size}

Query Parameters:
  - categoryId: Optional (Long)
  - sortType: Optional, Default: "LATEST" (Options: LATEST, BEST_SELLER, MOST_VIEWED, LOW_PRICE, HIGH_PRICE)
  - page: Optional, Default: 0 (int)
  - size: Optional, Default: 20 (int)

Response: 200 OK
{
  "content": [
    {
      "id": 101,
      "categoryId": 2,
      "name": "Laptop",
      "description": "High performance laptop",
      "price": 999.99,
      "stock": 50,
      "soldCount": 10,
      "viewCount": 250,
      "isActive": true,
      "minOrderQuantity": 1,
      "maxOrderQuantity": 5,
      "createdAt": "2025-12-23T10:30:00",
      "updatedAt": "2025-12-23T10:30:00"
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 150,
  "totalPages": 8,
  "isFirst": true,
  "isLast": false
}
```

### 2.3 Get Single Product
```
GET /api/products/{id}
Path Parameters: id (Long)

Response: 200 OK
{
  "id": 101,
  "categoryId": 2,
  "name": "Laptop",
  "description": "High performance laptop",
  "price": 999.99,
  "stock": 50,
  "soldCount": 10,
  "viewCount": 250,
  "isActive": true,
  "minOrderQuantity": 1,
  "maxOrderQuantity": 5,
  "createdAt": "2025-12-23T10:30:00",
  "updatedAt": "2025-12-23T10:30:00"
}
```

### 2.4 Get Top Ranked Products
```
GET /api/products/top-rank?type={type}&limit={limit}

Query Parameters:
  - type: Optional, Default: "daily" (Options: daily, weekly)
  - limit: Optional, Default: 10 (Min: 1, Max: 100)

Response: 200 OK
[
  {
    "rank": 1,
    "product": {
      "id": 101,
      "categoryId": 2,
      "name": "Laptop",
      "description": "High performance laptop",
      "price": 999.99,
      "stock": 50,
      "soldCount": 100,
      "viewCount": 5000,
      "isActive": true,
      "minOrderQuantity": 1,
      "maxOrderQuantity": 5,
      "createdAt": "2025-12-23T10:30:00",
      "updatedAt": "2025-12-23T10:30:00"
    }
  }
]
```

### 2.5 Update Product (Full)
```
PUT /api/products/{id}
Content-Type: application/json
Path Parameters: id (Long)

Request Body:
{
  "name": "Laptop Pro",
  "categoryId": 2,
  "description": "Updated description",
  "price": 1099.99,
  "stock": 45,
  "minOrderQuantity": 1,
  "maxOrderQuantity": 5
}

Response: 200 OK
(Returns updated ProductResponse)
```

### 2.6 Update Product Price
```
PATCH /api/products/{id}/price
Content-Type: application/json
Path Parameters: id (Long)

Request Body:
{
  "price": 1099.99  // Required, Min: 0
}

Response: 200 OK
(Returns updated ProductResponse)
```

### 2.7 Increase Stock
```
PATCH /api/products/{id}/stock/increase
Content-Type: application/json
Path Parameters: id (Long)

Request Body:
{
  "quantity": 20  // Required, Min: 1
}

Response: 200 OK
(Returns updated ProductResponse)
```

### 2.8 Decrease Stock
```
PATCH /api/products/{id}/stock/decrease
Content-Type: application/json
Path Parameters: id (Long)

Request Body:
{
  "quantity": 5  // Required, Min: 1
}

Response: 200 OK
(Returns updated ProductResponse)
```

### 2.9 Activate Product
```
POST /api/products/{id}/activate
Path Parameters: id (Long)

Response: 200 OK
(Returns updated ProductResponse with isActive: true)
```

### 2.10 Deactivate Product
```
POST /api/products/{id}/deactivate
Path Parameters: id (Long)

Response: 200 OK
(Returns updated ProductResponse with isActive: false)
```

### 2.11 Delete Product
```
DELETE /api/products/{id}
Path Parameters: id (Long)

Response: 204 No Content
```

---

## 3. CATEGORY CONTROLLER
**Base Path:** `/api/categories`
**Authentication:** None

### 3.1 Create Category
```
POST /api/categories
Content-Type: application/json

Request Body:
{
  "name": "Electronics",      // Required, NotBlank
  "displayOrder": 1           // Required, Min: 1
}

Response: 200 OK
{
  "id": 5,
  "name": "Electronics",
  "displayOrder": 1,
  "createdAt": "2025-12-23T10:30:00",
  "updatedAt": "2025-12-23T10:30:00"
}
```

### 3.2 Get Category List
```
GET /api/categories

Response: 200 OK
[
  {
    "id": 5,
    "name": "Electronics",
    "displayOrder": 1,
    "createdAt": "2025-12-23T10:30:00",
    "updatedAt": "2025-12-23T10:30:00"
  }
]
```

### 3.3 Get Single Category
```
GET /api/categories/{id}
Path Parameters: id (Long)

Response: 200 OK
{
  "id": 5,
  "name": "Electronics",
  "displayOrder": 1,
  "createdAt": "2025-12-23T10:30:00",
  "updatedAt": "2025-12-23T10:30:00"
}
```

### 3.4 Update Category
```
PUT /api/categories/{id}
Content-Type: application/json
Path Parameters: id (Long)

Request Body:
{
  "name": "Electronics & Gadgets",  // Required, NotBlank
  "displayOrder": 2                 // Required, Min: 1
}

Response: 200 OK
(Returns updated CategoryResponse)
```

### 3.5 Delete Category
```
DELETE /api/categories/{id}
Path Parameters: id (Long)

Response: 204 No Content
```

---

## 4. ORDER CONTROLLER
**Base Path:** `/api/orders`
**Authentication:** None
**Note:** Orders support asynchronous processing with Kafka

### 4.1 Create Order from Cart
```
POST /api/orders/from-cart
Content-Type: application/json

Request Body:
{
  "userId": 1,              // Required
  "cartIds": [101, 102],    // Required
  "pointAmount": 100.00,    // Optional
  "couponId": 5             // Optional
}

Response: 201 Created
{
  "orderId": 1001,
  "userId": 1,
  "totalAmount": 999.99,
  "shippingFee": 10.00,
  "discountAmount": 100.00,
  "pointAmount": 50.00,
  "finalAmount": 859.99,
  "orderStatus": "PENDING",
  "orderedAt": "2025-12-23T10:35:00",
  "orderItems": [
    {
      "orderItemId": 2001,
      "productId": 101,
      "productName": "Laptop",
      "quantity": 1,
      "unitPrice": 999.99,
      "subTotal": 999.99
    }
  ],
  "message": "주문이 접수되었습니다. 처리가 완료되면 알림을 보내드립니다."
}
```

### 4.2 Create Order from Product (Direct)
```
POST /api/orders/from-product
Content-Type: application/json

Request Body:
{
  "userId": 1,              // Required
  "productId": 101,         // Required
  "quantity": 2,            // Required, Min: 1
  "pointAmount": 50.00,     // Optional
  "couponId": 5             // Optional
}

Response: 201 Created
(Same response structure as 4.1)
```

### 4.3 Get Order List (with pagination and status filter)
```
GET /api/orders?userId={userId}&orderStatus={orderStatus}&page={page}&size={size}

Query Parameters:
  - userId: Required (Long)
  - orderStatus: Optional (Options: PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED)
  - page: Optional, Default: 0 (int)
  - size: Optional, Default: 10 (int)

Response: 200 OK
{
  "orders": [
    {
      "orderId": 1001,
      "userId": 1,
      "totalAmount": 999.99,
      "shippingFee": 10.00,
      "discountAmount": 100.00,
      "pointAmount": 50.00,
      "finalAmount": 859.99,
      "orderStatus": "PENDING",
      "orderedAt": "2025-12-23T10:35:00",
      "orderItems": [...]
    }
  ],
  "page": 0,
  "size": 10,
  "totalElements": 25,
  "totalPages": 3
}
```

### 4.4 Get Order Detail
```
GET /api/orders/{orderId}?userId={userId}
Path Parameters: orderId (Long)
Query Parameters: userId (Long, Required)

Response: 200 OK
(Same order detail structure as above)
```

### 4.5 Cancel Order
```
POST /api/orders/{orderId}/cancel?userId={userId}
Path Parameters: orderId (Long)
Query Parameters: userId (Long, Required)

Response: 204 No Content
```

---

## 5. PAYMENT CONTROLLER
**Base Path:** `/api/payments`
**Authentication:** None

### 5.1 Create Payment
```
POST /api/payments
Content-Type: application/json

Request Body:
{
  "orderId": 1001,                    // Required
  "paymentMethod": "CREDIT_CARD"      // Required (Options: CREDIT_CARD, DEBIT_CARD, PAYPAL, BANK_TRANSFER)
}

Response: 201 Created
{
  "id": 5001,
  "orderId": 1001,
  "paymentMethod": "CREDIT_CARD",
  "amount": 859.99,
  "status": "COMPLETED",
  "createdAt": "2025-12-23T10:36:00",
  "updatedAt": "2025-12-23T10:36:00"
}
```

---

## 6. POINT CONTROLLER
**Base Path:** `/api/points`
**Authentication:** None

### 6.1 Charge Points
```
POST /api/points/charge
Content-Type: application/json

Request Body:
{
  "userId": 1,                   // Required
  "amount": 1000.00,             // Required, Min: 1
  "description": "Premium plan"  // Optional
}

Response: 200 OK
{
  "id": 501,
  "userId": 1,
  "amount": 1000.00,
  "balanceAfter": 2500.00,
  "transactionType": "CHARGE",
  "description": "Premium plan",
  "createdAt": "2025-12-23T10:40:00"
}
```

### 6.2 Get Point Balance
```
GET /api/points/balance?userId={userId}

Query Parameters:
  - userId: Required (Long)

Response: 200 OK
{
  "userId": 1,
  "balance": 2500.00,
  "lastUpdated": "2025-12-23T10:40:00"
}
```

### 6.3 Get Point History
```
GET /api/points/history?userId={userId}&page={page}&size={size}

Query Parameters:
  - userId: Required (Long)
  - page: Optional, Default: 0 (int)
  - size: Optional, Default: 20 (int)

Response: 200 OK
{
  "content": [
    {
      "id": 501,
      "userId": 1,
      "amount": 1000.00,
      "balanceAfter": 2500.00,
      "transactionType": "CHARGE",
      "description": "Premium plan",
      "createdAt": "2025-12-23T10:40:00"
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 15,
  "totalPages": 1,
  "isFirst": true,
  "isLast": true
}
```

---

## 7. COUPON CONTROLLER
**Base Path:** `/api/coupons`
**Authentication:** None
**Note:** Coupon issuance uses Redis for fast first-come-first-served distribution

### 7.1 Create Coupon
```
POST /api/coupons
Content-Type: application/json

Request Body:
{
  "name": "Black Friday Sale",              // Required, NotBlank
  "code": "BF2025",                         // Optional
  "discountType": "PERCENTAGE",             // Required (Options: PERCENTAGE, FIXED_AMOUNT)
  "discountValue": 20.00,                   // Required, Min: 0 (exclusive)
  "maxDiscountAmount": 500.00,              // Optional (for percentage type)
  "minOrderAmount": 100.00,                 // Optional
  "totalQuantity": 1000,                    // Required, Min: 1
  "perUserLimit": 1,                        // Required, Min: 1
  "startDate": "2025-12-23T00:00:00",      // Required
  "endDate": "2025-12-31T23:59:59"         // Required
}

Response: 200 OK
{
  "id": 5,
  "name": "Black Friday Sale",
  "code": "BF2025",
  "discountType": "PERCENTAGE",
  "discountValue": 20.00,
  "maxDiscountAmount": 500.00,
  "minOrderAmount": 100.00,
  "totalQuantity": 1000,
  "perUserLimit": 1,
  "remainingQuantity": 1000,
  "isActive": true,
  "startDate": "2025-12-23T00:00:00",
  "endDate": "2025-12-31T23:59:59",
  "createdAt": "2025-12-23T10:45:00",
  "updatedAt": "2025-12-23T10:45:00"
}
```

### 7.2 Get Coupon List
```
GET /api/coupons

Response: 200 OK
[
  {
    "id": 5,
    "name": "Black Friday Sale",
    "code": "BF2025",
    "discountType": "PERCENTAGE",
    "discountValue": 20.00,
    "maxDiscountAmount": 500.00,
    "minOrderAmount": 100.00,
    "totalQuantity": 1000,
    "perUserLimit": 1,
    "remainingQuantity": 950,
    "isActive": true,
    "startDate": "2025-12-23T00:00:00",
    "endDate": "2025-12-31T23:59:59",
    "createdAt": "2025-12-23T10:45:00",
    "updatedAt": "2025-12-23T10:45:00"
  }
]
```

### 7.3 Get Single Coupon
```
GET /api/coupons/{id}
Path Parameters: id (Long)

Response: 200 OK
(Same coupon structure as above)
```

### 7.4 Update Coupon
```
PUT /api/coupons/{id}
Content-Type: application/json
Path Parameters: id (Long)

Request Body:
(Same structure as 7.1 Create Coupon)

Response: 200 OK
(Returns updated CouponResponse)
```

### 7.5 Issue Coupon (First-Come-First-Served)
```
POST /api/coupons/issue
Content-Type: application/json

Request Body:
{
  "userId": 1,      // Required
  "couponId": 5     // Required
}

Response: 200 OK
(Empty body - Redis handles immediate issuance, DB saves asynchronously)
```

### 7.6 Deactivate Coupon
```
PATCH /api/coupons/{id}/deactivate
Path Parameters: id (Long)

Response: 200 OK
(Returns CouponResponse with isActive: false)
```

### 7.7 Activate Coupon
```
PATCH /api/coupons/{id}/activate
Path Parameters: id (Long)

Response: 200 OK
(Returns CouponResponse with isActive: true)
```

---

## 8. KAFKA TEST CONTROLLER
**Base Path:** `/api/test/kafka`
**Authentication:** None
**Purpose:** Testing/Debugging only

### 8.1 Send Test Message
```
GET /api/test/kafka/send?message={message}

Query Parameters:
  - message: Optional, Default: "Hello Kafka!"

Response: 200 OK
{
  "success": true,
  "topic": "test-topic",
  "key": "test-key-1703337645000",
  "message": "Hello Kafka!",
  "timestamp": 1703337645000
}
```

### 8.2 Check Kafka Health
```
GET /api/test/kafka/health

Response: 200 OK
{
  "kafka": "ready",
  "timestamp": 1703337645000
}
```

---

## AUTHENTICATION & AUTHORIZATION

**Current Status:** NO AUTHENTICATION REQUIRED

The application does NOT implement Spring Security, @PreAuthorize, @Secured, or JWT authentication. All endpoints are publicly accessible.

**Recommendations for Production:**
- Implement Spring Security with JWT token-based authentication
- Add @PreAuthorize annotations for role-based access control
- Secure payment endpoints
- Add rate limiting

---

## KEY VALIDATION RULES

### Cart Validation
- userId: Min 1
- productId: Min 1
- quantity: Min 1

### Product Validation
- name: Required, NotBlank
- price: Required, Min 0
- categoryId: Optional
- minOrderQuantity: Required, Min 1
- maxOrderQuantity: Required, Min 1
- stock: Min 0

### Order Validation
- userId: Required
- productId/cartIds: Required
- quantity: Required, Min 1
- pointAmount: Optional
- couponId: Optional

### Payment Validation
- orderId: Required
- paymentMethod: Required

### Point Validation
- userId: Required
- amount: Required, Min 1.0

### Coupon Validation
- name: Required, NotBlank
- discountType: Required
- discountValue: Required, Min 0 (exclusive)
- totalQuantity: Required, Min 1
- perUserLimit: Required, Min 1
- startDate: Required
- endDate: Required

---

## RESPONSE CODES

- **200 OK** - Successful GET, POST, PATCH, PUT
- **201 Created** - Successful POST (Order, Payment creation)
- **204 No Content** - Successful DELETE
- **400 Bad Request** - Validation errors
- **404 Not Found** - Resource not found
- **500 Internal Server Error** - Server errors

---

## COMMON PATTERNS FOR K6 TESTING

### Test Data Setup
1. Create categories first
2. Create products in categories
3. Create coupons
4. Charge points to users
5. Then execute order workflows

### High-Load Scenarios
1. Product browsing (GET endpoints)
2. Shopping cart operations (POST/PATCH cart)
3. Order creation (async processing via Kafka)
4. Coupon issuance (Redis-backed)
5. Point operations

### Concurrent Testing Considerations
1. Order creation is asynchronous (uses Kafka)
2. Coupon issuance uses Redis for atomicity
3. Cart operations may have race conditions with stock
4. Consider using distinct userIds and productIds to avoid conflicts

---

## FILE PATHS

Controller files:
- `/Volumes/E 드라이브/study/ECommerce-project/src/main/java/io/hhplus/ECommerce/ECommerce_project/cart/presentation/CartController.java`
- `/Volumes/E 드라이브/study/ECommerce-project/src/main/java/io/hhplus/ECommerce/ECommerce_project/product/presentation/ProductController.java`
- `/Volumes/E 드라이브/study/ECommerce-project/src/main/java/io/hhplus/ECommerce/ECommerce_project/order/presentation/OrderController.java`
- `/Volumes/E 드라이브/study/ECommerce-project/src/main/java/io/hhplus/ECommerce/ECommerce_project/category/presentation/CategoryController.java`
- `/Volumes/E 드라이브/study/ECommerce-project/src/main/java/io/hhplus/ECommerce/ECommerce_project/payment/presentation/PaymentController.java`
- `/Volumes/E 드라이브/study/ECommerce-project/src/main/java/io/hhplus/ECommerce/ECommerce_project/point/presentation/PointController.java`
- `/Volumes/E 드라이브/study/ECommerce-project/src/main/java/io/hhplus/ECommerce/ECommerce_project/coupon/presentation/CouponController.java`
- `/Volumes/E 드라이브/study/ECommerce-project/src/main/java/io/hhplus/ECommerce/ECommerce_project/common/test/KafkaTestController.java`

---

## NOTES FOR K6 TEST SCRIPT DEVELOPMENT

### Data Dependencies
- User IDs must exist before being referenced
- Product IDs must exist before cart operations
- Cart items must exist before order creation
- Orders must exist before payment creation
- Category IDs are optional but can be used for filtering

### Async Processing
- Order creation returns immediately (PENDING status)
- Kafka handles actual order processing asynchronously
- Coupon issuance is Redis-backed with async DB persistence
- Tests should account for eventual consistency

### Rate Limiting Considerations
- MySQL connection pool: max-active=10
- Redis pool: max-active=10, max-idle=10
- No explicit API rate limiting configured
- Kafka brokers support multiple consumer groups

### Test Data Cleanup
- Orders can be cancelled via DELETE endpoint
- Coupons can be deactivated
- Products can be deleted
- Categories can be deleted

