"""
Order CSV Generator for Day 22: Spark Declarative Pipelines Lab

Generates deterministic daily order CSV files for the e-commerce pipeline.
Each file represents one day of orders across all 8 stores.

Usage:
    python generate_orders.py --start-date 2024-01-01 --end-date 2024-01-31 --output-dir ./full-load

Output format:
    order_id,order_date,store_id,customer_type,order_amount,items_count,customer_rating
    ORDNE01240101A3F2,2024-01-01,NE01,new,124.50,3,5
"""

import argparse
import csv
import hashlib
import os
import random
from datetime import datetime, timedelta


STORE_IDS = ["NE01", "NE02", "SE01", "SE02", "MW01", "MW02", "WE01", "WE02"]
CUSTOMER_TYPES = ["new", "returning"]
CUSTOMER_TYPE_WEIGHTS = [0.35, 0.65]  # 35% new, 65% returning


def generate_order_id(store_id: str, order_date: str, sequence: int) -> str:
    """Generate a deterministic order ID.

    Format: ORD[store_prefix][YYMMDD][4-char hash]
    Example: ORDNE01240101A3F2
    """
    raw = f"{store_id}-{order_date}-{sequence}"
    hash_suffix = hashlib.md5(raw.encode()).hexdigest()[:4].upper()
    date_compact = order_date.replace("-", "")[2:]  # YYMMDD
    return f"ORD{store_id}{date_compact}{hash_suffix}"


def generate_day_orders(order_date: str, seed: int) -> list:
    """Generate all orders for a single day across all stores.

    Args:
        order_date: Date string in YYYY-MM-DD format.
        seed: Random seed derived from the date for determinism.

    Returns:
        List of order dictionaries.
    """
    rng = random.Random(seed)
    orders = []

    for store_id in STORE_IDS:
        num_orders = rng.randint(20, 50)

        for seq in range(num_orders):
            customer_type = rng.choices(
                CUSTOMER_TYPES, weights=CUSTOMER_TYPE_WEIGHTS, k=1
            )[0]
            order_amount = round(rng.uniform(8.99, 499.99), 2)
            items_count = rng.randint(1, 12)
            customer_rating = rng.randint(1, 5)

            orders.append({
                "order_id": generate_order_id(store_id, order_date, seq),
                "order_date": order_date,
                "store_id": store_id,
                "customer_type": customer_type,
                "order_amount": f"{order_amount:.2f}",
                "items_count": items_count,
                "customer_rating": customer_rating,
            })

    return orders


def date_to_seed(date_str: str) -> int:
    """Convert a date string to a deterministic integer seed."""
    return int(hashlib.md5(date_str.encode()).hexdigest()[:8], 16)


def main():
    parser = argparse.ArgumentParser(
        description="Generate deterministic e-commerce order CSV files."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2024-01-01",
        help="Start date (YYYY-MM-DD). Default: 2024-01-01",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2024-01-05",
        help="End date inclusive (YYYY-MM-DD). Default: 2024-01-05",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./full-load",
        help="Output directory for CSV files. Default: ./full-load",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")

    current = start
    total_orders = 0
    total_files = 0

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        seed = date_to_seed(date_str)
        orders = generate_day_orders(date_str, seed)

        filename = f"orders_{date_str}.csv"
        filepath = os.path.join(args.output_dir, filename)

        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "order_id", "order_date", "store_id", "customer_type",
                    "order_amount", "items_count", "customer_rating",
                ],
            )
            writer.writeheader()
            writer.writerows(orders)

        total_orders += len(orders)
        total_files += 1
        print(f"  Generated {filepath} ({len(orders)} orders)")

        current += timedelta(days=1)

    print(f"\nDone: {total_files} files, {total_orders} total orders in {args.output_dir}/")


if __name__ == "__main__":
    main()
