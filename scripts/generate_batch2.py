"""Creates a second batch of raw data (20 new orders) to simulate 'day 2'.

Orders 1001-1010 fall on March dates that already have sales, so their gold
rows get UPDATED. Orders 1011-1020 fall on new July dates, so their gold rows
get INSERTED. Everything is computed from the loop counter, so the output is
the same every time.
"""

import csv
import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

RAW = Path("data/raw")
FIRST_NEW_ORDER_ID = 1001


def write_csv(path: Path, header: list, rows: list) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def next_item_id() -> int:
    """Continues numbering after the highest existing order_item_id."""
    with open(RAW / "order_items" / "order_items.csv") as f:
        return max(int(row["order_item_id"]) for row in csv.DictReader(f)) + 1


def main() -> None:
    item_id = next_item_id()
    orders, items, payments = [], [], []

    for i in range(20):
        order_id = FIRST_NEW_ORDER_ID + i
        # Conditional expression: "A if condition else B"
        order_date = (
            date(2024, 3, 1) + timedelta(days=i)
            if i < 10
            else date(2024, 7, 1) + timedelta(days=i - 10)
        )
        total = Decimal("0")
        for line in range(2):
            quantity = (i + line) % 3 + 1
            unit_price = Decimal(10 + (i * 7 + line * 13) % 90)  # 10 to 99
            product_id = (i * 3 + line * 11) % 50 + 1             # always 1 to 50
            items.append([item_id, order_id, product_id, quantity, f"{unit_price:.2f}"])
            item_id += 1
            total += quantity * unit_price
        customer_id = (i * 7) % 200 + 1                           # always 1 to 200
        orders.append([order_id, customer_id, order_date.isoformat(), "placed", order_id])
        payments.append({
            "payment_id": order_id,
            "order_id": order_id,
            "payment_method": "card",
            "amount": float(total),
            "payment_status": "paid",
            "payment_date": f"{order_date.isoformat()}T12:00:00",
        })

    write_csv(RAW / "orders" / "orders_batch2.csv",
              ["order_id", "customer_id", "order_date", "order_status", "payment_id"], orders)
    write_csv(RAW / "order_items" / "order_items_batch2.csv",
              ["order_item_id", "order_id", "product_id", "quantity", "unit_price"], items)
    with open(RAW / "payments" / "payments_batch2.json", "w") as f:
        json.dump(payments, f, indent=2)

    print(f"orders: {len(orders)}, order_items: {len(items)}, payments: {len(payments)}")


if __name__ == "__main__":
    main()