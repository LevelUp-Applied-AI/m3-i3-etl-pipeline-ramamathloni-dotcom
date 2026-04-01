import pandas as pd
from sqlalchemy import create_engine, text
import os
import json
from datetime import datetime

# --- CONFIGURATION (Tier 3: Configuration Management) ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/amman_market"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def extract(engine):
    print("\n--- 📥 [Stage 1: Extract Stage] ---")
    # ملاحظة لـ Tier 2: في التحميل التدريجي نستخدم "WHERE order_date > last_run"
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    
    print(f"✅ Extracted: {len(customers)} Customers, {len(products)} Products, {len(orders)} Orders")
    return {"customers": customers, "products": products, "orders": orders, "order_items": order_items}

def transform(data_dict):
    print("\n--- ⚙️ [Stage 2: Transform Stage] ---")
    c, p, o, oi = data_dict["customers"], data_dict["products"], data_dict["orders"], data_dict["order_items"]
    
    # دمج البيانات الأساسية
    merged = oi.merge(o, on="order_id").merge(p, on="product_id")
    merged["line_total"] = merged["quantity"] * merged["unit_price"]
    
    # الفلاتر الأساسية (Tier 0 Requirements)
    merged = merged[(merged["status"] != 'cancelled') & (merged["quantity"] <= 100)]
    
    # تجميع البيانات
    cust_summary = merged.groupby("customer_id").agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()
    
    # حساب المتوسط وقيمة الطلب
    cust_summary["avg_order_value"] = cust_summary["total_revenue"] / cust_summary["total_orders"]
    
    # --- Tier 1: Statistical Outlier Detection ---
    mean_rev = cust_summary['total_revenue'].mean()
    std_rev = cust_summary['total_revenue'].std()
    # تحديد القيم الشاذة (أكثر من 3 انحرافات معيارية عن المتوسط)
    cust_summary['is_outlier'] = cust_summary['total_revenue'] > (mean_rev + 3 * std_rev)
    
    # حساب أفضل فئة (Top Category) لكل عميل
    cat_revenue = merged.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    top_cat = cat_revenue.sort_values(["customer_id", "line_total"], ascending=[True, False]).drop_duplicates("customer_id")
    top_cat = top_cat.rename(columns={"category": "top_category"})[["customer_id", "top_category"]]
    
    # الدمج النهائي مع اسم العميل
    name_col = 'name' if 'name' in c.columns else 'customer_name'
    final_df = cust_summary.merge(top_cat, on="customer_id").merge(c[["customer_id", name_col]], on="customer_id")
    final_df = final_df.rename(columns={name_col: "customer_name"})
    
    print(f"📊 Transformation complete. Created {len(final_df)} customer records.")
    return final_df

def validate_and_report(df):
    print("\n--- ✅ [Stage 3: Validation & Tier 1 Reporting Stage] ---")
    
    # ضمان أن كل البيانات هي Python Native Types عشان ملف الـ JSON ما يعطي إيرور
    checks = {
        "No Nulls": bool(not df[['customer_id', 'customer_name']].isnull().any().any()),
        "Revenue > 0": bool((df['total_revenue'] > 0).all()),
        "Unique IDs": bool(df['customer_id'].is_unique)
    }
    
    # تحويل بيانات الـ Outliers لقائمة قواميس (Tier 1 Requirement)
    outliers_df = df[df['is_outlier'] == True][['customer_id', 'total_revenue', 'customer_name']].copy()
    outliers_df['total_revenue'] = outliers_df['total_revenue'].astype(float)
    outliers_list = outliers_df.to_dict(orient='records')
    
    # بناء التقرير النهائي (Advanced Reporting)
    quality_report = {
        "run_timestamp": datetime.now().isoformat(),
        "total_records_checked": int(len(df)),
        "validation_results": checks,
        "outlier_summary": {
            "count": int(len(outliers_list)),
            "details": outliers_list
        }
    }
    
    # الحفظ في ملف JSON
    report_path = os.path.join(OUTPUT_DIR, "quality_report.json")
    with open(report_path, 'w') as f:
        json.dump(quality_report, f, indent=4)
    
    for check, status in checks.items():
        print(f"[{'PASS' if status else 'FAIL'}] {check}")
        
    print(f"⚠️ Tier 1: Quality report saved to '{report_path}'. Found {len(outliers_list)} outliers.")
    
    if not all(checks.values()):
        raise ValueError("CRITICAL: One or more data quality checks failed!")
        
    return df

def load(df, engine):
    print("\n--- 📤 [Stage 4: Loading Stage] ---")
    
    # Load to PostgreSQL (New table for advanced analytics)
    df.to_sql("customer_analytics_advanced", engine, if_exists="replace", index=False)
    
    # Save to CSV
    csv_path = os.path.join(OUTPUT_DIR, "customer_analytics_advanced.csv")
    df.to_csv(csv_path, index=False)
    
    print(f"🚀 Success! Results loaded to database table and '{csv_path}'.")

def main():
    # 1. Create Engine
    engine = create_engine(DB_URL)
    
    try:
        # 2. Extract
        raw_data = extract(engine)
        
        # 3. Transform
        summary = transform(raw_data)
        
        # 4. Validate & Report (Tier 1 Challenges)
        summary = validate_and_report(summary)
        
        # 5. Load
        load(summary, engine)
        
        print("\n✨ ADVANCED ETL PIPELINE COMPLETED SUCCESSFULLY ✨")
        
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {str(e)}")

if __name__ == "__main__":
    main()


    #########2222222222222222222222#####
    