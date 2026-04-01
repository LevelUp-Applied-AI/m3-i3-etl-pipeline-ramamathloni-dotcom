import pandas as pd
from sqlalchemy import create_engine, text
import os
import json
from datetime import datetime

# --- CONFIGURATION ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/amman_market"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def initialize_metadata_table(engine):
    """Tier 2: Ensure the metadata table exists"""
    query = text("""
        CREATE TABLE IF NOT EXISTS etl_metadata (
            run_id SERIAL PRIMARY KEY,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            rows_processed INT,
            status VARCHAR(20)
        );
    """)
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()

def get_last_run_timestamp(engine):
    """Tier 2: Fetch last successful run timestamp"""
    query = "SELECT MAX(start_time) FROM etl_metadata WHERE status = 'success'"
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()[0]
        return result if result else '2000-01-01 00:00:00'
    except:
        return '2000-01-01 00:00:00'

def log_etl_run(engine, start_time, rows, status):
    """Tier 2: Log details to etl_metadata"""
    end_time = datetime.now()
    query = text("""
        INSERT INTO etl_metadata (start_time, end_time, rows_processed, status)
        VALUES (:start, :end, :rows, :status)
    """)
    with engine.connect() as conn:
        conn.execute(query, {"start": start_time, "end": end_time, "rows": rows, "status": status})
        conn.commit()

def extract(engine, last_run_time):
    print(f"--- [Extract Stage] Fetching data... ---")
    
    # Note: To enable Incremental mode again, uncomment the WHERE clause below
    # order_query = f"SELECT * FROM orders WHERE order_date > '{last_run_time}'"
    order_query = "SELECT * FROM orders" 
    
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql(order_query, engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    
    return {"customers": customers, "products": products, "orders": orders, "order_items": order_items}

def transform(data_dict):
    print("--- [Transform Stage] ---")
    c, p, o, oi = data_dict["customers"], data_dict["products"], data_dict["orders"], data_dict["order_items"]
    
    if o.empty: 
        return pd.DataFrame()

    merged = oi.merge(o, on="order_id").merge(p, on="product_id")
    merged["line_total"] = merged["quantity"] * merged["unit_price"]
    
    # Basic filters
    merged = merged[(merged["status"] != 'cancelled') & (merged["quantity"] <= 100)]
    
    if merged.empty:
        return pd.DataFrame()

    cust_summary = merged.groupby("customer_id").agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()
    
    # Tier 1: Outlier Detection (3 Standard Deviations)
    if len(cust_summary) > 1:
        mean_rev = cust_summary['total_revenue'].mean()
        std_rev = cust_summary['total_revenue'].std()
        cust_summary['is_outlier'] = cust_summary['total_revenue'] > (mean_rev + 3 * std_rev)
    else:
        cust_summary['is_outlier'] = False
    
    # Merging customer names
    name_col = 'name' if 'name' in c.columns else 'customer_name'
    final_df = cust_summary.merge(c[["customer_id", name_col]], on="customer_id")
    final_df = final_df.rename(columns={name_col: "customer_name"})
    
    return final_df

def validate_and_report(df):
    if df.empty: 
        return True
    print("--- [Validation & JSON Reporting Stage] ---")
    
    checks = {
        "No_Nulls": bool(not df[['customer_id', 'customer_name']].isnull().any().any()),
        "Revenue_Positive": bool((df['total_revenue'] > 0).all()),
        "Unique_IDs": bool(df['customer_id'].is_unique)
    }
    
    # Tier 1: Outliers data preparation
    outliers_data = df[df['is_outlier'] == True][['customer_id', 'total_revenue']].copy()
    outliers_data['total_revenue'] = outliers_data['total_revenue'].astype(float)
    outliers_list = outliers_data.to_dict(orient='records')
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_records": int(len(df)),
        "checks": checks,
        "flagged_outliers": outliers_list
    }
    
    report_path = os.path.join(OUTPUT_DIR, 'quality_report.json')
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=4)
        
    for check, status in checks.items():
        print(f"[{'PASS' if status else 'FAIL'}] {check}")
        
    return all(checks.values())

def load(df, engine):
    if df.empty: 
        return
    # Use 'replace' for the first run or 'append' for true incremental
    df.to_sql("customer_analytics_challenges", engine, if_exists="replace", index=False)
    
    csv_path = os.path.join(OUTPUT_DIR, "customer_analytics_advanced.csv")
    df.to_csv(csv_path, index=False)
    print(f"✅ Exported {len(df)} records to CSV and Database.")

def main():
    print("🚀 Starting Advanced ETL...")
    engine = create_engine(DB_URL)
    initialize_metadata_table(engine)
    start_time = datetime.now()
    
    try:
        last_run = get_last_run_timestamp(engine)
        data = extract(engine, last_run)
        processed_df = transform(data)
        
        if processed_df.empty:
            print("ℹ️ No data matched the filters.")
            log_etl_run(engine, start_time, 0, "success")
            return

        if validate_and_report(processed_df):
            load(processed_df, engine)
            log_etl_run(engine, start_time, len(processed_df), "success")
            print("\n✨ ETL Run Completed Successfully!")
        else:
            log_etl_run(engine, start_time, 0, "failed")
            print("\n❌ Validation Failed.")
            
    except Exception as e:
        print(f"💥 Pipeline Error: {e}")

if __name__ == "__main__":
    main()