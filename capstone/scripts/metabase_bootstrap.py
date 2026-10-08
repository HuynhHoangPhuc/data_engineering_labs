"""Capstone - optional Metabase bootstrap for the `serve` stage (stdlib only, no pip install needed).

    python scripts/metabase_bootstrap.py            # after `docker compose --profile serve up -d`

What it does (all of it can also be done by hand in the UI, see README section 6.3):
  1. first-run setup: creates the admin user below (skipped if Metabase is already set up)
  2. adds the Trino connection "Lakehouse (Trino)"  (engine "Starburst (Trino)", host trino:8080, catalog lake, schema gold)
  3. creates ONE example question - "Daily revenue (gold.daily_sales)" - and a dashboard "Olist - Capstone" holding it
  4. runs the question and prints the first rows, so you can compare them with the same SQL in Trino

The other R7 charts (orders by status, late-delivery rate by state, top categories, freshness) are YOUR job.
"""
import json
import sys
import time
import urllib.error
import urllib.request

MB = "http://localhost:3000"
ADMIN = {"email": "admin@capstone.local", "password": "Capstone-2026!", "first_name": "Cap", "last_name": "Stone"}
TRINO_DB = {
    "name": "Lakehouse (Trino)",
    "engine": "starburst",                     # Metabase's bundled "Starburst (Trino)" driver
    "details": {"host": "trino", "port": 8080,  # container name + container port (NOT localhost:8085)
                "catalog": "lake", "schema": "gold", "user": "metabase", "ssl": False},
}
EXAMPLE_SQL = """SELECT date_day, round(sum(revenue), 2) AS revenue, sum(orders) AS orders
FROM lake.gold.daily_sales
GROUP BY date_day
ORDER BY date_day"""


def call(method, path, body=None, session=None):
    req = urllib.request.Request(MB + path, method=method, data=None if body is None else json.dumps(body).encode())
    req.add_header("Content-Type", "application/json")
    if session:
        req.add_header("X-Metabase-Session", session)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:500]}")


def main():
    for _ in range(60):                         # Metabase needs ~1-2 min to start
        try:
            if call("GET", "/api/health").get("status") == "ok":
                break
        except Exception:
            pass
        time.sleep(5)
    props = call("GET", "/api/session/properties")
    if props.get("setup-token") and not props.get("has-user-setup"):
        call("POST", "/api/setup", {"token": props["setup-token"], "user": ADMIN,
                                    "prefs": {"site_name": "Olist Capstone", "allow_tracking": False}})
        print(f"admin user created: {ADMIN['email']} / {ADMIN['password']}")
    session = call("POST", "/api/session", {"username": ADMIN["email"], "password": ADMIN["password"]})["id"]

    dbs = call("GET", "/api/database", session=session)["data"]
    db = next((d for d in dbs if d["name"] == TRINO_DB["name"]), None)
    if db is None:
        db = call("POST", "/api/database", TRINO_DB, session=session)
        print(f"database added: id={db['id']} ({TRINO_DB['engine']} -> trino:8080 / lake.gold)")
    call("POST", f"/api/database/{db['id']}/sync_schema", {}, session=session)

    card = call("POST", "/api/card", {
        "name": "Daily revenue (gold.daily_sales)", "display": "line",
        "dataset_query": {"type": "native", "database": db["id"], "native": {"query": EXAMPLE_SQL}},
        "visualization_settings": {"graph.dimensions": ["date_day"], "graph.metrics": ["revenue"]},
    }, session=session)
    dash = call("POST", "/api/dashboard", {"name": "Olist - Capstone"}, session=session)
    call("PUT", f"/api/dashboard/{dash['id']}", {"dashcards": [
        {"id": -1, "card_id": card["id"], "row": 0, "col": 0, "size_x": 24, "size_y": 8}]}, session=session)

    res = call("POST", f"/api/card/{card['id']}/query", {}, session=session)
    if res.get("status") != "completed":
        sys.exit(f"question failed: {res.get('error')}")
    rows = res["data"]["rows"]
    print(f"question id={card['id']} returned {len(rows)} rows; last 3: {rows[-3:]}")
    print(f"dashboard: {MB}/dashboard/{dash['id']}   (login {ADMIN['email']} / {ADMIN['password']})")


if __name__ == "__main__":
    main()
