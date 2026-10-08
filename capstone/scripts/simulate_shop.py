"""Capstone - simulate OLTP traffic on the Olist Postgres (the CDC source).

Every tick it does a random mix of realistic business events:
  * new order  (maybe a new customer) with 1-3 items and a payment        -> INSERTs
  * order lifecycle: created -> approved -> invoiced -> shipped -> delivered -> UPDATEs
  * cancellation of a not-yet-shipped order                               -> UPDATE
  * deletion of a fraudulent order                                          -> DELETE (cascades to items/payments)
  * --chaos: occasionally writes BAD data (negative price, unknown status) so your DQ gates have something to catch

Usage:
    python scripts/simulate_shop.py --minutes 2 --rate 2        # ~2 events / second for 2 minutes
    python scripts/simulate_shop.py --minutes 1 --chaos         # include bad records
"""
import argparse
import hashlib
import random
import time
import uuid
from datetime import datetime, timedelta

import psycopg2

NEXT_STATUS = {"created": "approved", "approved": "invoiced", "invoiced": "shipped", "shipped": "delivered"}
PAYMENT_TYPES = ["credit_card", "credit_card", "credit_card", "boleto", "voucher", "debit_card"]


def md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def new_order(cur, chaos: bool):
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    if random.random() < 0.3:  # new customer
        cid = md5(str(uuid.uuid4()))
        cur.execute("SELECT customer_zip_code_prefix, customer_city, customer_state FROM customers ORDER BY random() LIMIT 1")
        z, city, st = cur.fetchone()
        cur.execute("INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)"
                    " VALUES (%s, %s, %s, %s, %s)", (cid, md5(str(uuid.uuid4())), z, city, st))
    else:
        cid = cur.fetchone()[0]
    oid = md5(str(uuid.uuid4()))
    now = datetime.now()
    status = "created"
    if chaos and random.random() < 0.3:
        status = "lost_in_space"  # not an accepted value -> DQ should catch it
    cur.execute("INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, order_estimated_delivery_date)"
                " VALUES (%s, %s, %s, %s, %s)", (oid, cid, status, now, now + timedelta(days=random.randint(7, 30))))
    total = 0.0
    for i in range(1, random.randint(1, 3) + 1):
        cur.execute("SELECT p.product_id, oi.seller_id, avg(oi.price) FROM products p JOIN order_items oi USING (product_id)"
                    " GROUP BY 1, 2 ORDER BY random() LIMIT 1")
        pid, sid, price = cur.fetchone()
        price = round(float(price) * random.uniform(0.9, 1.1), 2)
        if chaos and random.random() < 0.3:
            price = -price  # negative price -> DQ should catch it
        freight = round(random.uniform(8, 40), 2)
        total += price + freight
        cur.execute("INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)", (oid, i, pid, sid, now + timedelta(days=3), price, freight))
    cur.execute("INSERT INTO order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value)"
                " VALUES (%s, 1, %s, %s, %s)", (oid, random.choice(PAYMENT_TYPES), random.choice([1, 1, 2, 3, 6]), round(total, 2)))
    return f"new order {oid[:8]} ({status})"


def advance_order(cur):
    cur.execute("SELECT order_id, order_status FROM orders WHERE order_status IN ('created','approved','invoiced','shipped')"
                " ORDER BY order_purchase_timestamp DESC LIMIT 50")
    rows = cur.fetchall()
    if not rows:
        return "no open orders"
    oid, st = random.choice(rows)
    nxt = NEXT_STATUS[st]
    extra = {"approved": ", order_approved_at = now()", "shipped": ", order_delivered_carrier_date = now()",
             "delivered": ", order_delivered_customer_date = now()"}.get(nxt, "")
    cur.execute(f"UPDATE orders SET order_status = %s{extra} WHERE order_id = %s", (nxt, oid))
    return f"order {oid[:8]} {st} -> {nxt}"


def cancel_order(cur):
    cur.execute("UPDATE orders SET order_status = 'canceled' WHERE order_id = ("
                " SELECT order_id FROM orders WHERE order_status IN ('created','approved') ORDER BY random() LIMIT 1)"
                " RETURNING order_id")
    r = cur.fetchone()
    return f"order {r[0][:8]} canceled" if r else "nothing to cancel"


def delete_fraud(cur):
    cur.execute("DELETE FROM orders WHERE order_id = ("
                " SELECT order_id FROM orders WHERE order_status = 'created' ORDER BY random() LIMIT 1) RETURNING order_id")
    r = cur.fetchone()
    return f"fraudulent order {r[0][:8]} DELETED (cascade to items/payments)" if r else "nothing to delete"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="host=localhost port=5433 dbname=olist user=olist password=olist")
    ap.add_argument("--minutes", type=float, default=2)
    ap.add_argument("--rate", type=float, default=2, help="events per second")
    ap.add_argument("--chaos", action="store_true", help="sometimes write invalid data")
    args = ap.parse_args()

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False
    end = time.time() + args.minutes * 60
    n = 0
    while time.time() < end:
        r = random.random()
        with conn.cursor() as cur:
            if r < 0.45:
                msg = new_order(cur, args.chaos)
            elif r < 0.90:
                msg = advance_order(cur)
            elif r < 0.97:
                msg = cancel_order(cur)
            else:
                msg = delete_fraud(cur)
        conn.commit()  # one business event = one transaction
        n += 1
        print(f"{datetime.now():%H:%M:%S} #{n:<5} {msg}")
        time.sleep(1 / args.rate)
    conn.close()
    print(f"done: {n} business events")


if __name__ == "__main__":
    main()
