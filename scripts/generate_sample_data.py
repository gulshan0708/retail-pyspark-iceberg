"""Generates sample raw data for the retail pipeline.

A few rows are deliberately bad (nulls, duplicates, orphan keys, negative
numbers) so the data quality checks in Pipeline 1 have something to catch.
"""

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)  # same seed -> identical data on every run (reproducible)

OUT = Path("data/raw")
N_CUSTOMERS, N_PRODUCTS, N_ORDERS = 200, 50, 1000

COUNTRIES = ["US", "UK", "DE", "FR", "IN", "PL"]
CATEGORIES = ["Electronics", "Books", "Home", "Toys", "Clothing"]
BRANDS = ["Acme", "Globex", "Initech", "Umbrella", "Hooli"]
STATUSES = ["placed", "shipped", "delivered", "cancelled"]
METHODS = ["card", "paypal", "bank_transfer"]
FIRST_NAMES = ["Anna", "Ben", "Chen", "Dev", "Elena", "Farid", "Grace", "Hugo"]
LAST_NAMES = ["Kim", "Nowak", "Patel", "Rossi", "Smith", "Tanaka", "Weber"]


def write_csv(path: Path, header: list, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def write_json(path: Path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(records, f, indent=2)


def sql_str(value: str) -> str:
    """Wraps text in single quotes for SQL, doubling any quote inside it."""
    return "'" + value.replace("'", "''") + "'"


def main() -> None:
    # ---- products: written as a SQL file, loaded into Postgres later ----
    products = []
    for pid in range(1, N_PRODUCTS + 1):
        cost = round(random.uniform(5, 200), 2)
        price = round(cost * random.uniform(1.2, 2.0), 2)
        products.append([pid, f"Product {pid}", random.choice(CATEGORIES),
                         random.choice(BRANDS), cost, price])
    price_of = {p[0]: p[5] for p in products}  # product_id -> current_price

    sql = [
        "DROP TABLE IF EXISTS products;",
        "CREATE TABLE products (product_id INT PRIMARY KEY, product_name TEXT, "
        "category TEXT, brand TEXT, unit_cost NUMERIC(10,2), current_price NUMERIC(10,2));",
    ]
    for pid, name, cat, brand, cost, price in products:
        sql.append(
            f"INSERT INTO products VALUES ({pid}, {sql_str(name)}, "
            f"{sql_str(cat)}, {sql_str(brand)}, {cost}, {price});"
        )
    (OUT / "postgres").mkdir(parents=True, exist_ok=True)
    (OUT / "postgres" / "products.sql").write_text("\n".join(sql) + "\n")

    # ---- customers (JSON) ----
    customers = []
    for cid in range(1, N_CUSTOMERS + 1):
        first, last = random.choice(FIRST_NAMES), random.choice(LAST_NAMES)
        customers.append({
            "customer_id": cid,
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{cid}@example.com",
            "signup_date": (date(2023, 1, 1) + timedelta(days=random.randint(0, 500))).isoformat(),
            "country": random.choice(COUNTRIES),
        })
    customers[10]["email"] = None                 # BAD: null email
    customers[20]["email"] = "not-an-email"       # BAD: malformed email
    customers.append(dict(customers[5]))          # BAD: duplicate customer_id

    # ---- orders (CSV), order_items (CSV), payments (JSON) ----
    orders, items, payments = [], [], []
    item_id = 1
    for oid in range(1, N_ORDERS + 1):
        order_date = date(2024, 1, 1) + timedelta(days=random.randint(0, 180))
        status = random.choice(STATUSES)
        total = 0.0
        for _ in range(random.randint(1, 4)):
            pid = random.randint(1, N_PRODUCTS)
            qty = random.randint(1, 5)
            items.append([item_id, oid, pid, qty, price_of[pid]])
            item_id += 1
            total += qty * price_of[pid]
        orders.append([oid, random.randint(1, N_CUSTOMERS), order_date.isoformat(), status, oid])
        payments.append({
            "payment_id": oid,
            "order_id": oid,
            "payment_method": random.choice(METHODS),
            "amount": round(total, 2),
            "payment_status": "refunded" if status == "cancelled" else "paid",
            "payment_date": f"{order_date.isoformat()}T{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00",
        })

    orders[30][3] = "  SHIPPED "         # BAD: messy text (cleaning will fix it)
    orders[40][1] = 9999                 # BAD: orphan customer_id
    orders.append(list(orders[50]))      # BAD: duplicate order_id
    items[60][3] = -3                    # BAD: negative quantity
    items[70][2] = 9999                  # BAD: orphan product_id
    payments[80]["amount"] = -25.0       # BAD: negative amount
    print(f"{OUT}")
    write_json(OUT / "customers" / "customers.json", customers)
    write_csv(OUT / "orders" / "orders.csv",
              ["order_id", "customer_id", "order_date", "order_status", "payment_id"], orders)
    write_csv(OUT / "order_items" / "order_items.csv",
              ["order_item_id", "order_id", "product_id", "quantity", "unit_price"], items)
    write_json(OUT / "payments" / "payments.json", payments)

    print(f"products:    {len(products)} rows (-> Postgres SQL file)")
    print(f"customers:   {len(customers)} rows (3 bad)")
    print(f"orders:      {len(orders)} rows (3 bad)")
    print(f"order_items: {len(items)} rows (2 bad)")
    print(f"payments:    {len(payments)} rows (1 bad)")


if __name__ == "__main__":
    main()