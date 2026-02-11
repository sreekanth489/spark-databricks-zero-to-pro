"""
E-commerce dataset generator for Spark Databricks Zero-to-Pro notebooks.

Generates four related datasets:
- Customers: customer profiles with demographics
- Products: product catalog with categories and pricing
- Orders: order transactions linking customers to products
- Clickstream: website click events for customer journeys

All functions return lists of dicts that can be converted to Spark DataFrames.
Usage in a Databricks notebook:

    # Inline usage (no import needed — just paste the functions)
    # Or if mounted/uploaded:
    from data_generators.generate_ecommerce import generate_customers, generate_orders
"""

import random
from datetime import datetime, timedelta


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

def generate_customers(count=100, seed=42):
    """Generate customer profiles.

    Returns list of dicts with keys:
        customer_id, name, email, city, state, country, signup_date, tier
    """
    random.seed(seed)

    first_names = [
        "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
        "Ivy", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Peter",
        "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander"
    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
        "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
        "Jackson", "White", "Harris", "Martin", "Thompson", "Moore", "Allen"
    ]
    cities = [
        ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
        ("Houston", "TX"), ("Phoenix", "AZ"), ("Seattle", "WA"),
        ("Denver", "CO"), ("Boston", "MA"), ("Atlanta", "GA"),
        ("Portland", "OR"), ("Austin", "TX"), ("Miami", "FL")
    ]
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    tier_weights = [0.4, 0.3, 0.2, 0.1]

    customers = []
    for i in range(1, count + 1):
        first = random.choice(first_names)
        last = random.choice(last_names)
        city, state = random.choice(cities)
        signup = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
        customers.append({
            "customer_id": f"C-{i:05d}",
            "name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "city": city,
            "state": state,
            "country": "US",
            "signup_date": signup.strftime("%Y-%m-%d"),
            "tier": random.choices(tiers, weights=tier_weights, k=1)[0],
        })
    return customers


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

def generate_products(count=50, seed=42):
    """Generate product catalog.

    Returns list of dicts with keys:
        product_id, name, category, subcategory, price, weight_kg
    """
    random.seed(seed)

    catalog = {
        "Electronics": {
            "Smartphones": (199.99, 999.99, 0.15, 0.25),
            "Laptops": (499.99, 2499.99, 1.2, 3.0),
            "Headphones": (29.99, 349.99, 0.1, 0.4),
            "Tablets": (149.99, 1099.99, 0.3, 0.8),
        },
        "Clothing": {
            "T-Shirts": (9.99, 49.99, 0.15, 0.3),
            "Jeans": (29.99, 129.99, 0.5, 1.0),
            "Jackets": (49.99, 299.99, 0.6, 1.5),
        },
        "Home & Kitchen": {
            "Cookware": (19.99, 199.99, 0.5, 3.0),
            "Furniture": (99.99, 999.99, 5.0, 30.0),
            "Lighting": (14.99, 149.99, 0.3, 2.0),
        },
        "Books": {
            "Fiction": (7.99, 24.99, 0.2, 0.5),
            "Non-Fiction": (9.99, 39.99, 0.2, 0.6),
            "Technical": (19.99, 79.99, 0.3, 0.8),
        },
    }

    products = []
    categories = list(catalog.keys())
    for i in range(1, count + 1):
        cat = random.choice(categories)
        subcat = random.choice(list(catalog[cat].keys()))
        price_min, price_max, weight_min, weight_max = catalog[cat][subcat]
        products.append({
            "product_id": f"P-{i:05d}",
            "name": f"{subcat.rstrip('s')} Model-{random.randint(100,999)}",
            "category": cat,
            "subcategory": subcat,
            "price": round(random.uniform(price_min, price_max), 2),
            "weight_kg": round(random.uniform(weight_min, weight_max), 2),
        })
    return products


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def generate_orders(customers, products, count=500, seed=42):
    """Generate order transactions.

    Args:
        customers: list of customer dicts (from generate_customers)
        products: list of product dicts (from generate_products)
        count: number of orders to generate

    Returns list of dicts with keys:
        order_id, customer_id, product_id, quantity, total_amount,
        order_date, status, payment_method
    """
    random.seed(seed)

    statuses = ["completed", "shipped", "processing", "cancelled", "returned"]
    status_weights = [0.5, 0.2, 0.15, 0.1, 0.05]
    payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer"]

    customer_ids = [c["customer_id"] for c in customers]
    product_lookup = {p["product_id"]: p for p in products}
    product_ids = list(product_lookup.keys())

    orders = []
    for i in range(1, count + 1):
        pid = random.choice(product_ids)
        qty = random.randint(1, 5)
        price = product_lookup[pid]["price"]
        order_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))
        orders.append({
            "order_id": f"O-{i:06d}",
            "customer_id": random.choice(customer_ids),
            "product_id": pid,
            "quantity": qty,
            "total_amount": round(price * qty, 2),
            "order_date": order_date.strftime("%Y-%m-%d"),
            "status": random.choices(statuses, weights=status_weights, k=1)[0],
            "payment_method": random.choice(payment_methods),
        })
    return orders


# ---------------------------------------------------------------------------
# Clickstream
# ---------------------------------------------------------------------------

def generate_clickstream(customers, products, count=1000, seed=42):
    """Generate website clickstream events.

    Returns list of dicts with keys:
        event_id, customer_id, product_id, event_type, page,
        timestamp, session_id, device
    """
    random.seed(seed)

    event_types = ["page_view", "click", "add_to_cart", "purchase", "search"]
    event_weights = [0.4, 0.25, 0.15, 0.1, 0.1]
    pages = ["home", "category", "product", "cart", "checkout", "search_results"]
    devices = ["desktop", "mobile", "tablet"]
    device_weights = [0.45, 0.40, 0.15]

    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]

    events = []
    for i in range(1, count + 1):
        ts = datetime(2024, 1, 1) + timedelta(
            days=random.randint(0, 365),
            seconds=random.randint(0, 86399)
        )
        events.append({
            "event_id": f"E-{i:07d}",
            "customer_id": random.choice(customer_ids),
            "product_id": random.choice(product_ids) if random.random() > 0.3 else None,
            "event_type": random.choices(event_types, weights=event_weights, k=1)[0],
            "page": random.choice(pages),
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "session_id": f"S-{random.randint(1, count // 5):06d}",
            "device": random.choices(devices, weights=device_weights, k=1)[0],
        })
    return events
