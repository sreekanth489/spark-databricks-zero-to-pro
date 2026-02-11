"""
Shared data generation utilities for Spark Databricks Zero-to-Pro notebooks.

These utilities help generate deterministic sample data for use in
Databricks notebooks. All functions accept an optional seed for reproducibility.
"""

import random
import string
from datetime import datetime, timedelta


def set_seed(seed=42):
    """Set random seed for reproducible data generation."""
    random.seed(seed)


def random_id(prefix="ID", length=8):
    """Generate a random ID string like 'ID-A3F8B2C1'."""
    chars = string.ascii_uppercase + string.digits
    suffix = "".join(random.choices(chars, k=length))
    return f"{prefix}-{suffix}"


def random_ids(prefix="ID", length=8, count=10):
    """Generate a list of unique random IDs."""
    ids = set()
    while len(ids) < count:
        ids.add(random_id(prefix, length))
    return list(ids)


def random_timestamp(start_date="2023-01-01", end_date="2024-12-31", fmt="%Y-%m-%d %H:%M:%S"):
    """Generate a random timestamp between start_date and end_date."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86399)
    dt = start + timedelta(days=random_days, seconds=random_seconds)
    return dt.strftime(fmt)


def random_timestamps(count=10, start_date="2023-01-01", end_date="2024-12-31"):
    """Generate a sorted list of random timestamps."""
    timestamps = [random_timestamp(start_date, end_date) for _ in range(count)]
    timestamps.sort()
    return timestamps


def random_name():
    """Generate a random full name."""
    first_names = [
        "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
        "Ivy", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Peter",
        "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
        "Yara", "Zach"
    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
        "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
        "Jackson", "White", "Harris", "Martin", "Thompson", "Moore", "Allen"
    ]
    return f"{random.choice(first_names)} {random.choice(last_names)}"


def random_email(name=None):
    """Generate a random email from a name."""
    if name is None:
        name = random_name()
    domains = ["example.com", "testmail.com", "sample.org", "demo.net"]
    local = name.lower().replace(" ", ".") + str(random.randint(1, 999))
    return f"{local}@{random.choice(domains)}"


def random_choice_weighted(options, weights):
    """Pick a random option based on weights."""
    return random.choices(options, weights=weights, k=1)[0]


def random_amount(min_val=1.00, max_val=500.00, decimals=2):
    """Generate a random monetary amount."""
    return round(random.uniform(min_val, max_val), decimals)


def date_range(start_date="2023-01-01", periods=30, freq_days=1):
    """Generate a list of date strings."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    return [(start + timedelta(days=i * freq_days)).strftime("%Y-%m-%d") for i in range(periods)]


def generate_spark_dataframe(spark, data, schema=None):
    """Create a Spark DataFrame from a list of dicts or tuples.

    Args:
        spark: SparkSession
        data: list of dicts or list of tuples
        schema: optional StructType or list of column names

    Returns:
        pyspark.sql.DataFrame
    """
    if isinstance(data[0], dict):
        return spark.createDataFrame([tuple(d.values()) for d in data], list(data[0].keys()))
    return spark.createDataFrame(data, schema=schema)
