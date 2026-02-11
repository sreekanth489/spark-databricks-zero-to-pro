# Data Generators

Shared utilities for generating sample datasets used across all notebooks in this repository.

## Files

| File | Purpose |
|------|---------|
| `generator_utils.py` | General-purpose helpers: random IDs, timestamps, names, emails |
| `generate_ecommerce.py` | E-commerce domain: customers, products, orders, clickstream |

## Usage

### In Databricks Notebooks

Most notebooks **inline** their data generation (they don't import from this directory) so they remain self-contained. These files serve as the canonical reference and can be uploaded to DBFS or a Volume if you want to share them across notebooks.

```python
# Option 1: Upload to DBFS and add to sys.path
import sys
sys.path.append("/dbfs/FileStore/data-generators/")
from generate_ecommerce import generate_customers, generate_orders

# Option 2: Copy functions directly into your notebook (preferred for portability)
```

### Locally

```python
from generate_ecommerce import generate_customers, generate_products, generate_orders

customers = generate_customers(count=100, seed=42)
products = generate_products(count=50, seed=42)
orders = generate_orders(customers, products, count=500, seed=42)
```

## Datasets

### E-commerce

| Generator | Default Count | Schema |
|-----------|---------------|--------|
| `generate_customers()` | 100 | customer_id, name, email, city, state, country, signup_date, tier |
| `generate_products()` | 50 | product_id, name, category, subcategory, price, weight_kg |
| `generate_orders()` | 500 | order_id, customer_id, product_id, quantity, total_amount, order_date, status, payment_method |
| `generate_clickstream()` | 1000 | event_id, customer_id, product_id, event_type, page, timestamp, session_id, device |

## Determinism

All generators accept a `seed` parameter (default `42`) for reproducible output. The same seed always produces the same data.
