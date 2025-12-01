from flask import Flask, jsonify, render_template
import psycopg2
import os

app = Flask(__name__)

# Replace YOUR_PASSWORD with your actual PostgreSQL password
DATABASE_URL = "postgresql://postgres:JNrjRwrEaZTZLshqqqlszHqsiYneAhws@tramway.proxy.rlwy.net:36780/railway"

def get_conn():
    return psycopg2.connect(DATABASE_URL)

@app.route("/")
def home():
    return render_template("index.html")

# --------------------------------------------------
# 1) SALES TREND (Daily Units Sold)
# --------------------------------------------------
@app.route("/api/sales-trend")
def sales_trend():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT Date, SUM(UnitsSold)
        FROM InventoryTransactions
        GROUP BY Date
        ORDER BY Date;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([{"date": str(r[0]), "units_sold": r[1]} for r in rows])

# --------------------------------------------------
# 2) SALES BY REGION
# --------------------------------------------------
@app.route("/api/sales-by-region")
def sales_by_region():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT r.RegionName, SUM(i.UnitsSold)
        FROM InventoryTransactions i
        JOIN Stores s ON i.StoreID = s.StoreID
        JOIN Regions r ON s.RegionID = r.RegionID
        GROUP BY r.RegionName;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([{"region": r[0], "units_sold": r[1]} for r in rows])

# --------------------------------------------------
# 3) SALES BY CATEGORY
# --------------------------------------------------
@app.route("/api/sales-by-category")
def sales_by_category():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.CategoryName, SUM(i.UnitsSold)
        FROM InventoryTransactions i
        JOIN Products p ON i.ProductID = p.ProductID
        JOIN Categories c ON p.CategoryID = c.CategoryID
        GROUP BY c.CategoryName;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([{"category": r[0], "units_sold": r[1]} for r in rows])

# --------------------------------------------------
# 4) WEATHER IMPACT ON SALES
# --------------------------------------------------
@app.route("/api/weather-impact")
def weather_impact():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT WeatherCondition, AVG(UnitsSold)
        FROM InventoryTransactions
        GROUP BY WeatherCondition;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {"weather": r[0], "avg_units_sold": float(r[1])} for r in rows
    ])

# --------------------------------------------------
# 5) LOW INVENTORY ITEMS (Threshold < 50)
# --------------------------------------------------
@app.route("/api/low-inventory")
def low_inventory():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT ProductID, InventoryLevel
        FROM InventoryTransactions
        WHERE InventoryLevel < 50
        ORDER BY InventoryLevel ASC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {"product": r[0], "inventory_level": r[1]} for r in rows
    ])

# Run Server
if __name__ == "__main__":
    app.run(debug=True)

