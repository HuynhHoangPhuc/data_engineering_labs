#!/usr/bin/env python3
"""Generate a deterministic, realistic *synthetic* Olist seed (02_seed.sql).

Standard library only. Same schema/column names as the Kaggle
"Brazilian E-Commerce Public Dataset by Olist"; distributions (states,
categories, prices, payment types, order statuses, delivery delays,
repeat customers, seller popularity) are modelled on the real data.

    python3 generate_seed.py                   # 5000 orders -> init/02_seed.sql
    python3 generate_seed.py --orders 20000 --out /tmp/big_seed.sql

Re-running with the same --seed produces byte-identical output.
"""
import argparse
import hashlib
import math
import os
import random
from datetime import datetime, timedelta

# (state, weight %, [(city, zip_from, zip_to), ...]) -- zip = 5-digit CEP prefix
GEO = [
    ("SP", 42.0, [("sao paulo", 1000, 5999), ("campinas", 13000, 13139), ("guarulhos", 7000, 7399),
                  ("santo andre", 9000, 9299), ("sao bernardo do campo", 9600, 9899), ("osasco", 6000, 6299),
                  ("ribeirao preto", 14000, 14114), ("sorocaba", 18000, 18109),
                  ("santa barbara d'oeste", 13450, 13459)]),
    ("RJ", 12.9, [("rio de janeiro", 20000, 23799), ("niteroi", 24000, 24399), ("nova iguacu", 26000, 26099),
                  ("duque de caxias", 25000, 25299)]),
    ("MG", 11.7, [("belo horizonte", 30000, 31999), ("uberlandia", 38400, 38415), ("contagem", 32000, 32399),
                  ("juiz de fora", 36000, 36099)]),
    ("RS", 5.5, [("porto alegre", 90000, 91999), ("caxias do sul", 95000, 95099), ("canoas", 92000, 92499)]),
    ("PR", 5.1, [("curitiba", 80000, 82999), ("londrina", 86000, 86099), ("maringa", 87000, 87099)]),
    ("SC", 3.7, [("florianopolis", 88000, 88099), ("joinville", 89200, 89239), ("blumenau", 89000, 89099)]),
    ("BA", 3.4, [("salvador", 40000, 42599), ("feira de santana", 44000, 44099)]),
    ("DF", 2.2, [("brasilia", 70000, 72799)]),
    ("ES", 2.0, [("vitoria", 29000, 29099), ("vila velha", 29100, 29129)]),
    ("GO", 2.0, [("goiania", 74000, 74899)]),
    ("PE", 1.7, [("recife", 50000, 52999)]),
    ("CE", 1.3, [("fortaleza", 60000, 61599)]),
    ("PA", 1.0, [("belem", 66000, 66999)]),
    ("MT", 0.9, [("cuiaba", 78000, 78099)]),
    ("MA", 0.7, [("sao luis", 65000, 65099)]),
    ("MS", 0.7, [("campo grande", 79000, 79129)]),
    ("PB", 0.5, [("joao pessoa", 58000, 58099)]),
    ("PI", 0.5, [("teresina", 64000, 64099)]),
    ("RN", 0.5, [("natal", 59000, 59099)]),
    ("AL", 0.4, [("maceio", 57000, 57099)]),
    ("SE", 0.3, [("aracaju", 49000, 49099)]),
    ("TO", 0.3, [("palmas", 77000, 77299)]),
    ("RO", 0.25, [("porto velho", 76800, 76834)]),
    ("AM", 0.15, [("manaus", 69000, 69099)]),
    ("AC", 0.08, [("rio branco", 69900, 69923)]),
    ("AP", 0.07, [("macapa", 68900, 68911)]),
    ("RR", 0.05, [("boa vista", 69300, 69339)]),
]
# extra delivery days by region (distance from the SP-centred seller base)
REGION_DELAY = {"SP": 0, "RJ": 3, "MG": 3, "PR": 4, "SC": 5, "RS": 6, "ES": 5, "DF": 6, "GO": 6, "MS": 7,
                "MT": 9, "BA": 9, "PE": 11, "CE": 12, "PB": 12, "RN": 12, "AL": 12, "SE": 11, "PI": 12,
                "MA": 12, "TO": 10, "PA": 13, "AM": 16, "RO": 14, "AC": 16, "AP": 17, "RR": 18}
# sellers are concentrated in the south-east
SELLER_STATE_W = {"SP": 60, "PR": 8, "MG": 8, "RJ": 5, "SC": 4, "RS": 3, "GO": 1, "DF": 1, "BA": 1, "ES": 1, "PE": 0.5}

# (category, weight, price multiplier, weight multiplier)
CATEGORIES = [
    ("cama_mesa_banho", 11.0, 0.9, 1.2), ("beleza_saude", 9.3, 1.1, 0.6), ("esporte_lazer", 7.7, 1.1, 1.0),
    ("moveis_decoracao", 7.4, 0.9, 1.5), ("informatica_acessorios", 7.0, 1.3, 0.8),
    ("utilidades_domesticas", 6.2, 0.8, 1.0), ("relogios_presentes", 5.3, 2.0, 0.3), ("telefonia", 4.0, 0.7, 0.3),
    ("ferramentas_jardim", 3.9, 1.0, 1.4), ("automotivo", 3.8, 1.2, 1.1), ("brinquedos", 3.6, 1.0, 0.9),
    ("cool_stuff", 3.4, 1.5, 1.2), ("perfumaria", 3.1, 1.1, 0.4), ("bebes", 2.7, 1.2, 1.1),
    ("eletronicos", 2.5, 0.7, 0.4), ("papelaria", 2.2, 0.9, 0.6), ("fashion_bolsas_e_acessorios", 1.8, 0.7, 0.4),
    ("pet_shop", 1.7, 1.0, 1.0), ("moveis_escritorio", 1.5, 1.6, 4.0), ("consoles_games", 1.0, 1.2, 0.6),
    ("malas_acessorios", 1.0, 1.4, 1.8), ("construcao_ferramentas_construcao", 0.8, 1.2, 1.5),
    ("eletrodomesticos", 0.7, 1.4, 2.5), ("instrumentos_musicais", 0.6, 1.9, 1.6),
    ("eletroportateis", 0.6, 2.4, 2.0), ("casa_construcao", 0.5, 1.2, 1.5), ("livros_interesse_geral", 0.5, 0.7, 0.5),
    ("alimentos", 0.4, 0.6, 0.6), ("moveis_sala", 0.4, 1.3, 3.0), ("casa_conforto", 0.4, 1.3, 1.5),
    ("bebidas", 0.3, 0.7, 1.0), ("audio", 0.3, 1.8, 0.6), ("climatizacao", 0.25, 2.0, 2.2),
    ("pcs", 0.15, 12.0, 3.0), ("eletrodomesticos_2", 0.2, 5.0, 5.0),
]
NULL_CATEGORY_RATE = 0.018

ORDER_STATUS = [("delivered", 97.0), ("shipped", 1.1), ("canceled", 0.6), ("unavailable", 0.6),
                ("invoiced", 0.3), ("processing", 0.3), ("created", 0.05), ("approved", 0.05)]

START = datetime(2017, 1, 1)
END = datetime(2018, 8, 31, 23, 59, 59)
SNAPSHOT = datetime(2018, 9, 3, 9, 0, 0)  # "export time" -- no event after this


def hex_id(kind: str, n: int) -> str:
    """32-char lowercase hex id, like the Kaggle dataset (md5 of a stable key)."""
    return hashlib.md5(f"olist-lab-{kind}-{n}".encode()).hexdigest()


def weighted(rng, items, weights):
    return rng.choices(items, weights=weights, k=1)[0]


def ts(dt):
    return "NULL" if dt is None else f"'{dt:%Y-%m-%d %H:%M:%S}'"


def s(v):
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def pick_location(rng, states, state_w, geo_by_state):
    st = weighted(rng, states, state_w)
    city, z0, z1 = rng.choice(geo_by_state[st])
    return st, city, rng.randint(z0, z1)


def purchase_time(rng):
    # volume grows over time (Olist roughly tripled 2017 -> 2018): inverse-CDF of a linear ramp
    span = (END - START).total_seconds()
    u = rng.random()
    a = 0.35  # relative density at START (1.0 at END)
    x = (-a + math.sqrt(a * a + (1 - a * a) * u)) / (1 - a)
    t = START + timedelta(seconds=x * span)
    # busier in the afternoon/evening
    hour = weighted(rng, list(range(24)), [1, 0.6, 0.3, 0.2, 0.2, 0.3, 0.8, 2, 3.5, 4.5, 5.5, 5.8,
                                           5.6, 5.8, 5.8, 5.6, 5.8, 5.5, 5, 5, 5.5, 5.8, 5.2, 3.5])
    return t.replace(hour=hour, minute=rng.randint(0, 59), second=rng.randint(0, 59))


def generate(n_orders: int, seed: int):
    rng = random.Random(seed)
    states = [g[0] for g in GEO]
    state_w = [g[1] for g in GEO]
    geo_by_state = {g[0]: g[2] for g in GEO}

    # ---------------- sellers ----------------
    n_sellers = max(20, n_orders // 16)
    sellers = []
    s_states = list(SELLER_STATE_W)
    s_w = list(SELLER_STATE_W.values())
    for i in range(n_sellers):
        st, city, zipc = pick_location(rng, s_states, s_w, geo_by_state)
        onboard = START - timedelta(days=rng.randint(30, 400))
        sellers.append(dict(seller_id=hex_id("seller", i), seller_zip_code_prefix=zipc,
                            seller_city=city, seller_state=st, updated_at=onboard))
    # Zipf-like seller popularity: a few big sellers, a long tail
    seller_pop = [1.0 / (r + 1) ** 0.9 for r in range(n_sellers)]

    # ---------------- products ----------------
    n_products = max(50, int(n_orders * 0.3))
    cat_names = [c[0] for c in CATEGORIES]
    cat_w = [c[1] for c in CATEGORIES]
    cat_meta = {c[0]: (c[2], c[3]) for c in CATEGORIES}
    products = []
    for i in range(n_products):
        cat = None if rng.random() < NULL_CATEGORY_RATE else weighted(rng, cat_names, cat_w)
        pm, wm = cat_meta.get(cat, (1.0, 1.0))
        weight = int(min(40000, max(50, rng.lognormvariate(math.log(700 * wm), 0.9))))
        side = max(11, int((weight / 8) ** (1 / 3) * rng.uniform(1.6, 2.6)))
        seller_idx = weighted(rng, range(n_sellers), seller_pop)
        base_price = round(min(6700.0, max(3.5, rng.lognormvariate(math.log(75 * pm), 0.8))), 2)
        products.append(dict(
            product_id=hex_id("product", i),
            product_category_name=cat,
            product_name_lenght=None if cat is None else rng.randint(20, 64),
            product_description_lenght=None if cat is None else int(min(3992, max(4, rng.lognormvariate(6.5, 0.7)))),
            product_photos_qty=None if cat is None else weighted(rng, [1, 2, 3, 4, 5, 6], [50, 20, 12, 8, 5, 5]),
            product_weight_g=weight,
            product_length_cm=min(105, side + rng.randint(0, 15)),
            product_height_cm=max(2, min(105, side - rng.randint(0, 10))),
            product_width_cm=max(6, min(118, side + rng.randint(-5, 10))),
            updated_at=sellers[seller_idx]["updated_at"] + timedelta(days=rng.randint(1, 60)),
            _seller=seller_idx, _price=base_price,
        ))
    # product popularity: long tail too
    prod_pop = [1.0 / (r + 1) ** 0.7 for r in range(n_products)]

    # ---------------- customers / orders / items / payments ----------------
    customers, orders, items, payments = [], [], [], []
    unique_customers = []  # (unique_id, state, city, zip)
    for i in range(n_orders):
        # ~3% of orders come from a returning person (same customer_unique_id, new customer_id)
        if unique_customers and rng.random() < 0.03:
            uid, st, city, zipc = rng.choice(unique_customers)
        else:
            st, city, zipc = pick_location(rng, states, state_w, geo_by_state)
            uid = hex_id("unique", len(unique_customers))
            unique_customers.append((uid, st, city, zipc))

        order_id = hex_id("order", i)
        customer_id = hex_id("customer", i)
        purchased = purchase_time(rng)
        status = weighted(rng, [x[0] for x in ORDER_STATUS], [x[1] for x in ORDER_STATUS])
        if purchased > SNAPSHOT - timedelta(days=5) and status == "delivered":
            status = "shipped"  # very recent orders cannot be delivered yet

        approved = carrier = delivered = None
        if status != "created":
            approved = purchased + timedelta(minutes=rng.choice([rng.randint(5, 60), rng.randint(60, 2880)]))
        if status in ("delivered", "shipped") or (status == "canceled" and rng.random() < 0.3):
            carrier = approved + timedelta(hours=rng.randint(12, 24 * 6))
        if status == "delivered":
            base = rng.gammavariate(2.2, 3.0) + REGION_DELAY[st]
            delivered = carrier + timedelta(days=base, hours=rng.randint(0, 23))
            if delivered > SNAPSHOT:
                delivered = SNAPSHOT - timedelta(hours=rng.randint(1, 48))
        estimated = (purchased + timedelta(days=rng.randint(12, 30) + REGION_DELAY[st])).replace(
            hour=0, minute=0, second=0)
        last_event = max(x for x in (purchased, approved, carrier, delivered) if x is not None)

        customers.append(dict(customer_id=customer_id, customer_unique_id=uid, customer_zip_code_prefix=zipc,
                              customer_city=city, customer_state=st, updated_at=purchased))
        orders.append(dict(order_id=order_id, customer_id=customer_id, order_status=status,
                           order_purchase_timestamp=purchased, order_approved_at=approved,
                           order_delivered_carrier_date=carrier, order_delivered_customer_date=delivered,
                           order_estimated_delivery_date=estimated, updated_at=last_event))

        # items: 1 product (90%), sometimes several units of the same product, sometimes several products
        n_items = weighted(rng, [1, 2, 3, 4, 5, 6], [88.0, 8.0, 2.2, 1.0, 0.5, 0.3])
        first = products[weighted(rng, range(n_products), prod_pop)]
        chosen = [first] * n_items if rng.random() < 0.55 else \
            [first] + [products[weighted(rng, range(n_products), prod_pop)] for _ in range(n_items - 1)]
        order_total = 0.0
        if status in ("unavailable",) and rng.random() < 0.9:
            chosen = []  # unavailable orders usually have no items in the real data
        for k, p in enumerate(chosen, start=1):
            price = round(p["_price"] * rng.uniform(0.95, 1.05), 2)
            freight = round(min(400.0, max(0.0, 8 + p["product_weight_g"] / 1000 * rng.uniform(1.5, 3.0)
                                           + REGION_DELAY[st] * rng.uniform(0.8, 1.6))), 2)
            order_total += price + freight
            items.append(dict(order_id=order_id, order_item_id=k, product_id=p["product_id"],
                              seller_id=sellers[p["_seller"]]["seller_id"],
                              shipping_limit_date=(approved or purchased) + timedelta(days=rng.randint(2, 7)),
                              price=price, freight_value=freight, updated_at=approved or purchased))
        if not chosen:
            order_total = round(rng.lognormvariate(math.log(120), 0.8), 2)

        # payments (sum of payment_value == order total, like the real data)
        order_total = round(order_total, 2)
        ptype = weighted(rng, ["credit_card", "boleto", "voucher", "debit_card"], [73.9, 19.0, 5.6, 1.5])
        pay_ts = approved or purchased
        if ptype == "voucher" or (ptype == "credit_card" and rng.random() < 0.03):
            # split payment: one or more vouchers + the remainder
            n_v = weighted(rng, [1, 2, 3], [70, 20, 10])
            remaining = order_total
            seq = 1
            for _ in range(n_v):
                if remaining <= 0:
                    break
                v = round(min(remaining, rng.uniform(5, max(6, order_total * 0.6))), 2)
                payments.append(dict(order_id=order_id, payment_sequential=seq, payment_type="voucher",
                                     payment_installments=1, payment_value=v, updated_at=pay_ts))
                remaining = round(remaining - v, 2)
                seq += 1
            if remaining > 0:
                payments.append(dict(order_id=order_id, payment_sequential=seq,
                                     payment_type="credit_card" if ptype == "credit_card" else "voucher",
                                     payment_installments=1, payment_value=remaining, updated_at=pay_ts))
        else:
            inst = 1
            if ptype == "credit_card":
                inst = weighted(rng, [1, 2, 3, 4, 5, 6, 8, 10], [49, 12, 10, 7, 5, 4, 4, 9])
            payments.append(dict(order_id=order_id, payment_sequential=1, payment_type=ptype,
                                 payment_installments=inst, payment_value=order_total, updated_at=pay_ts))

    for p in products:
        del p["_seller"], p["_price"]
    # numeric surrogate key, increasing with purchase time (like an identity column)
    orders.sort(key=lambda o: (o["order_purchase_timestamp"], o["order_id"]))
    for seq, o in enumerate(orders, start=1):
        upd = o.pop("updated_at")
        o["order_seq"] = seq
        o["updated_at"] = upd
    return dict(customers=customers, sellers=sellers, products=products, orders=orders,
                order_items=items, order_payments=payments)


def fmt(v):
    return ts(v) if isinstance(v, datetime) else s(v)


def write_sql(data, path, seed, n_orders):
    order = ["customers", "sellers", "products", "orders", "order_items", "order_payments"]
    with open(path, "w", encoding="utf-8") as f:
        f.write("-- =====================================================================\n")
        f.write("-- SYNTHETIC Olist seed -- generated by labs/datasets/olist/generate_seed.py\n")
        f.write(f"-- seed={seed} orders={n_orders}.  DO NOT EDIT BY HAND; re-generate instead.\n")
        f.write("-- Row counts: " + ", ".join(f"{t}={len(data[t])}" for t in order) + "\n")
        f.write("-- For the real Kaggle data see labs/datasets/olist/README.md\n")
        f.write("-- =====================================================================\n")
        f.write("SET client_encoding = 'UTF8';\nBEGIN;\n")
        for table in order:
            rows = data[table]
            cols = list(rows[0].keys())
            for start in range(0, len(rows), 500):
                chunk = rows[start:start + 500]
                f.write(f"\nINSERT INTO {table} ({', '.join(cols)}) VALUES\n")
                f.write(",\n".join("(" + ", ".join(fmt(r[c]) for c in cols) + ")" for r in chunk))
                f.write(";\n")
        f.write("\n-- continue the identity after the explicitly inserted order_seq values\n")
        f.write("SELECT setval(pg_get_serial_sequence('orders', 'order_seq'), (SELECT max(order_seq) FROM orders));\n")
        f.write("\nCOMMIT;\nANALYZE;\n")


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--orders", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=os.path.join(here, "init", "02_seed.sql"))
    a = ap.parse_args()
    data = generate(a.orders, a.seed)
    write_sql(data, a.out, a.seed, a.orders)
    size = os.path.getsize(a.out) / 1e6
    print(f"wrote {a.out} ({size:.2f} MB): " + ", ".join(f"{k}={len(v)}" for k, v in data.items()))


if __name__ == "__main__":
    main()
