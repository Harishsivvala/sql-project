import pandas as pd
import psycopg2

# Update your connection string here
DATABASE_URL = "postgres://postgres:1234567890@localhost:5432/inventory_db"

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def load_csv():
    df = pd.read_csv("inventory_forecasting.csv")

    # Clean column names: replace spaces and slashes with _
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    # Convert Date column (DD-MM-YYYY → YYYY-MM-DD)
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y")

    return df

def etl():
    df = load_csv()
    conn = get_conn()
    cur = conn.cursor()

    # 1️⃣ Insert Regions
    for region in df["Region"].unique():
        cur.execute("""
            INSERT INTO Regions (RegionName)
            VALUES (%s)
            ON CONFLICT (RegionName) DO NOTHING;
        """, (region,))

    # 2️⃣ Insert Categories
    for category in df["Category"].unique():
        cur.execute("""
            INSERT INTO Categories (CategoryName)
            VALUES (%s)
            ON CONFLICT (CategoryName) DO NOTHING;
        """, (category,))

    # 3️⃣ Insert Stores
    for _, row in df[["Store_ID", "Region"]].drop_duplicates().iterrows():
        cur.execute("""
            INSERT INTO Stores (StoreID, RegionID)
            SELECT %s, RegionID FROM Regions WHERE RegionName=%s
            ON CONFLICT (StoreID) DO NOTHING;
        """, (row["Store_ID"], row["Region"]))

    # 4️⃣ Insert Products
    for _, row in df[["Product_ID", "Category"]].drop_duplicates().iterrows():
        cur.execute("""
            INSERT INTO Products (ProductID, CategoryID)
            SELECT %s, CategoryID FROM Categories WHERE CategoryName=%s
            ON CONFLICT (ProductID) DO NOTHING;
        """, (row["Product_ID"], row["Category"]))

    # 5️⃣ Insert Transaction Records
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO InventoryTransactions (
                Date, StoreID, ProductID, InventoryLevel,
                UnitsSold, UnitsOrdered, DemandForecast, Price,
                Discount, WeatherCondition, HolidayPromotion,
                CompetitorPricing, Seasonality
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            row["Date"], row["Store_ID"], row["Product_ID"],
            row["Inventory_Level"], row["Units_Sold"], row["Units_Ordered"],
            row["Demand_Forecast"], row["Price"], row["Discount"],
            row["Weather_Condition"], row["Holiday_Promotion"],
            row["Competitor_Pricing"], row["Seasonality"]
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("ETL completed successfully!")

if __name__ == "__main__":
    etl()
