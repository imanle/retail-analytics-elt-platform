"""
generate_sample_data.py

Generates:
- orders/          -> CSV files (one per day)
- order_items/     -> Parquet files (one per day)
- customers/       -> Single CSV (full snapshot)
- products/        -> Single CSV (full snapshot)
- shipments/       -> CSV files (one per day)
- payments/        -> CSV files (one per day)

Usage:
    pip install pandas pyarrow faker
    python scripts/generate_sample_data.py
    python scripts/generate_sample_data.py --days 7 --start-date 2026-05-01
"""

import argparse
import os
import random
from datetime import date, datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "raw_data")

N_CUSTOMERS = 200
N_PRODUCTS = 60
ORDERS_PER_DAY_MIN = 20
ORDERS_PER_DAY_MAX = 50
ITEMS_PER_ORDER_MIN = 1
ITEMS_PER_ORDER_MAX = 5

ORDER_STATUSES = ["completed", "cancelled", "refunded", "pending"]
ORDER_STATUS_WEIGHTS = [0.65, 0.15, 0.10, 0.10]

PAYMENT_METHODS = ["credit_card", "paypal", "bank_transfer", "cash_on_delivery", "gift_card"]
PAYMENT_STATUS_MAP = {
    "completed": "paid",
    "cancelled": "refunded",
    "refunded": "refunded",
    "pending": "pending",
}

CARRIERS = ["DHL", "FedEx", "UPS", "USPS", "Local Courier"]
SHIPPING_METHODS = ["standard", "express", "overnight"]
SHIPMENT_STATUSES = ["delivered", "in_transit", "pending", "failed"]

CATEGORIES = {
    "Electronics": ["Laptops", "Phones", "Tablets", "Accessories"],
    "Clothing": ["Men", "Women", "Kids", "Sportswear"],
    "Home & Garden": ["Furniture", "Decor", "Kitchen", "Garden"],
    "Books": ["Fiction", "Non-Fiction", "Technical", "Children"],
    "Sports": ["Gym", "Outdoor", "Team Sports", "Water Sports"],
}

BRANDS = ["TechPro", "StyleCo", "HomePlus", "ReadMore", "SportMax", "EcoWear", "UrbanGear", "PrimeTech"]

CURRENCIES = ["USD", "EUR", "GBP"]
COUNTRIES = ["United States", "Germany", "United Kingdom", "France", "Canada", "Australia", "Netherlands", "Spain"]


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def generate_customers(n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        country = random.choice(COUNTRIES)
        rows.append({
            "customer_id": f"CUST{i:05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "country": country,
            "city": fake.city(),
            "created_at": fake.date_time_between(start_date="-3y", end_date="-30d").isoformat(),
        })
    return pd.DataFrame(rows)


def generate_products(n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        category = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[category])
        cost = round(random.uniform(5, 300), 2)
        sale = round(cost * random.uniform(1.2, 2.5), 2)
        rows.append({
            "product_id": f"PROD{i:05d}",
            "product_name": f"{random.choice(BRANDS)} {fake.word().capitalize()} {random.randint(100, 999)}",
            "category": category,
            "subcategory": subcategory,
            "brand": random.choice(BRANDS),
            "cost_price": cost,
            "sale_price": sale,
            "is_active": random.choices([True, False], weights=[0.9, 0.1])[0],
        })
    return pd.DataFrame(rows)


def generate_orders_for_day(
    order_date: date,
    customer_ids: list,
    start_order_seq: int,
) -> pd.DataFrame:
    n = random.randint(ORDERS_PER_DAY_MIN, ORDERS_PER_DAY_MAX)
    rows = []
    for i in range(n):
        order_id = f"ORD{start_order_seq + i:07d}"
        status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS)[0]
        currency = random.choices(CURRENCIES, weights=[0.7, 0.2, 0.1])[0]
        created_dt = datetime.combine(order_date, datetime.min.time()) + timedelta(
            seconds=random.randint(0, 86399)
        )
        rows.append({
            "order_id": order_id,
            "customer_id": random.choice(customer_ids),
            "order_date": str(order_date),
            "order_status": status,
            "currency": currency,
            "total_amount": 0.0,  # will be filled after items are created
            "created_at": created_dt.isoformat(),
            "updated_at": created_dt.isoformat(),
        })
    return pd.DataFrame(rows)


def generate_order_items_for_orders(
    orders_df: pd.DataFrame,
    product_ids: list,
    products_df: pd.DataFrame,
    start_item_seq: int,
) -> pd.DataFrame:
    price_map = dict(zip(products_df["product_id"], products_df["sale_price"]))
    rows = []
    order_totals = {}
    seq = start_item_seq

    for _, order in orders_df.iterrows():
        n_items = random.randint(ITEMS_PER_ORDER_MIN, ITEMS_PER_ORDER_MAX)
        order_total = 0.0
        for _ in range(n_items):
            product_id = random.choice(product_ids)
            qty = random.randint(1, 4)
            unit_price = price_map.get(product_id, round(random.uniform(10, 200), 2))
            discount = round(unit_price * random.uniform(0, 0.15), 2)
            tax = round((unit_price - discount) * qty * 0.08, 2)
            line_total = round((unit_price - discount) * qty + tax, 2)
            order_total += line_total
            rows.append({
                "order_item_id": f"ITEM{seq:08d}",
                "order_id": order["order_id"],
                "product_id": product_id,
                "quantity": qty,
                "unit_price": unit_price,
                "discount_amount": discount,
                "tax_amount": tax,
                "line_total": line_total,
            })
            seq += 1
        order_totals[order["order_id"]] = round(order_total, 2)

    return pd.DataFrame(rows), order_totals


def generate_shipments_for_orders(
    orders_df: pd.DataFrame,
    order_date: date,
    start_ship_seq: int,
) -> pd.DataFrame:
    rows = []
    seq = start_ship_seq
    for _, order in orders_df.iterrows():
        # Only completed / in-transit orders get shipments
        if order["order_status"] in ("cancelled",):
            continue
        carrier = random.choice(CARRIERS)
        method = random.choice(SHIPPING_METHODS)
        shipping_cost = round(random.uniform(3, 30), 2)
        shipped_dt = datetime.combine(order_date, datetime.min.time()) + timedelta(
            hours=random.randint(2, 24)
        )
        delivery_days = {"standard": random.randint(3, 7), "express": random.randint(1, 3), "overnight": 1}[method]
        delivered_dt = shipped_dt + timedelta(days=delivery_days, hours=random.randint(0, 8))
        # Some shipments are still in transit (delivered_at is None)
        is_delivered = random.random() > 0.15
        status = "delivered" if is_delivered else random.choice(["in_transit", "pending"])
        rows.append({
            "shipment_id": f"SHIP{seq:07d}",
            "order_id": order["order_id"],
            "carrier": carrier,
            "shipping_method": method,
            "shipping_cost": shipping_cost,
            "shipped_at": shipped_dt.isoformat(),
            "delivered_at": delivered_dt.isoformat() if is_delivered else None,
            "shipment_status": status,
        })
        seq += 1
    return pd.DataFrame(rows)


def generate_payments_for_orders(
    orders_df: pd.DataFrame,
    order_totals: dict,
    order_date: date,
    start_pay_seq: int,
) -> pd.DataFrame:
    rows = []
    seq = start_pay_seq
    for _, order in orders_df.iterrows():
        if order["order_status"] == "pending":
            continue
        method = random.choice(PAYMENT_METHODS)
        status = PAYMENT_STATUS_MAP[order["order_status"]]
        paid_dt = datetime.combine(order_date, datetime.min.time()) + timedelta(
            hours=random.randint(0, 23), minutes=random.randint(0, 59)
        )
        rows.append({
            "payment_id": f"PAY{seq:08d}",
            "order_id": order["order_id"],
            "payment_method": method,
            "payment_status": status,
            "payment_amount": order_totals.get(order["order_id"], 0.0),
            "paid_at": paid_dt.isoformat(),
        })
        seq += 1
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# File writers
# ---------------------------------------------------------------------------

def save_csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"  Saved CSV  ({len(df):>6,} rows) -> {path}")


def save_parquet(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"  Saved PQ   ({len(df):>6,} rows) -> {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(start_date: date, n_days: int) -> None:
    print("=" * 60)
    print("Retail Analytics ELT Platform — Sample Data Generator")
    print("=" * 60)

    # Static reference data
    print("\n[1/3] Generating reference data (customers & products)...")
    customers_df = generate_customers(N_CUSTOMERS)
    products_df = generate_products(N_PRODUCTS)

    save_csv(customers_df, os.path.join(RAW_DATA_DIR, "customers", "customers.csv"))
    save_csv(products_df, os.path.join(RAW_DATA_DIR, "products", "products.csv"))

    customer_ids = customers_df["customer_id"].tolist()
    product_ids = products_df["product_id"].tolist()

    # Daily transactional data
    print(f"\n[2/3] Generating {n_days} day(s) of transactional data starting {start_date}...")

    order_seq = 1
    item_seq = 1
    ship_seq = 1
    pay_seq = 1

    all_orders = []
    all_items = []

    for day_offset in range(n_days):
        current_date = start_date + timedelta(days=day_offset)
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"\n  Date: {date_str}")

        orders_df = generate_orders_for_day(current_date, customer_ids, order_seq)
        items_df, order_totals = generate_order_items_for_orders(
            orders_df, product_ids, products_df, item_seq
        )

        # Back-fill total_amount on orders
        orders_df["total_amount"] = orders_df["order_id"].map(order_totals)

        shipments_df = generate_shipments_for_orders(orders_df, current_date, ship_seq)
        payments_df = generate_payments_for_orders(orders_df, order_totals, current_date, pay_seq)

        # Save daily files
        save_csv(orders_df, os.path.join(RAW_DATA_DIR, "orders", f"orders_{date_str}.csv"))
        save_parquet(items_df, os.path.join(RAW_DATA_DIR, "order_items", f"order_items_{date_str}.parquet"))
        save_csv(shipments_df, os.path.join(RAW_DATA_DIR, "shipments", f"shipments_{date_str}.csv"))
        save_csv(payments_df, os.path.join(RAW_DATA_DIR, "payments", f"payments_{date_str}.csv"))

        order_seq += len(orders_df)
        item_seq += len(items_df)
        ship_seq += len(shipments_df)
        pay_seq += len(payments_df)

        all_orders.append(orders_df)
        all_items.append(items_df)

    # Summary
    print("\n[3/3] Summary")
    print("-" * 40)
    total_orders = sum(len(df) for df in all_orders)
    total_items = sum(len(df) for df in all_items)
    print(f"  Customers   : {N_CUSTOMERS:,}")
    print(f"  Products    : {N_PRODUCTS:,}")
    print(f"  Days        : {n_days}")
    print(f"  Orders      : {total_orders:,}")
    print(f"  Order Items : {total_items:,}")
    print("\nDone! Raw data is ready in the raw_data/ folder.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sample retail data")
    parser.add_argument(
        "--start-date",
        type=lambda s: date.fromisoformat(s),
        default=date(2026, 5, 1),
        help="First date to generate data for (YYYY-MM-DD). Default: 2026-05-01",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to generate. Default: 7",
    )
    args = parser.parse_args()
    main(start_date=args.start_date, n_days=args.days)