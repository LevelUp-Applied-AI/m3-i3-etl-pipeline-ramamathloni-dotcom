import pandas as pd
from sqlalchemy import create_engine, text
import os
import json
import logging
from datetime import datetime

# --- Tier 3: Setup Logging with UTF-8 Support ---
def setup_logging(log_path):
    # Added encoding='utf-8' to handle emojis and special characters on Windows
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("ETL_Framework")

def extract(engine, logger):
    logger.info("Stage 1: Extracting data from PostgreSQL")
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    
    logger.info(f"Extracted {len(orders)} orders and {len(customers)} customers.")
    return {"customers": customers, "products": products, "orders": orders, "order_items": order_items}

def transform(data_dict, logger):
    logger.info("Stage 2: Transforming data")
    c, p, o, oi = data_dict["customers"], data_dict["products"], data_dict["orders"], data_dict["order_items"]
    
    if o.empty:
        logger.warning("No orders found to transform.")
        return pd.DataFrame()

    merged = oi.merge(o, on="order_id").merge(p, on="product_id")
    merged["line_total"] = merged["quantity"] * merged["unit_price"]
    
    # Advanced Filters (Tier 0 & Tier 1)
    merged = merged[(merged["status"] != 'cancelled') & (merged["quantity"] <= 100)]
    
    cust_summary = merged.groupby("customer_id").agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()
    
    # Tier 1: Outlier Detection (Mean + 3*STD)
    if len(cust_summary) > 1:
        mean_rev = cust_summary['total_revenue'].mean()
        std_rev = cust_summary['total_revenue'].std()
        cust_summary['is_outlier'] = cust_summary['total_revenue'] > (mean_rev + 3 * std_rev)
    else:
        cust_summary['is_outlier'] = False
    
    # Add Customer Names
    name_col = 'name' if 'name' in c.columns else 'customer_name'
    final_df = cust_summary.merge(c[["customer_id", name_col]], on="customer_id")
    final_df = final_df.rename(columns={name_col: "customer_name"})
    
    logger.info(f"Transformation complete. Generated {len(final_df)} rows.")
    return final_df

def validate_and_report(df, output_dir, logger):
    logger.info("Stage 3: Validating data and generating JSON report")
    if df.empty: return True
    
    checks = {
        "No_Nulls": bool(not df[['customer_id', 'customer_name']].isnull().any().any()),
        "Revenue_Positive": bool((df['total_revenue'] > 0).all()),
        "Unique_IDs": bool(df['customer_id'].is_unique)
    }
    
    # Prepare Outliers for JSON
    outliers_list = df[df['is_outlier'] == True][['customer_id', 'total_revenue']].to_dict(orient='records')
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_records": int(len(df)),
        "checks": checks,
        "outliers_found": len(outliers_list),
        "flagged_outliers": outliers_list
    }
    
    report_path = os.path.join(output_dir, 'quality_report_tier3.json')
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=4)
    
    logger.info(f"Quality report saved to {report_path}")
    return all(checks.values())

def load(df, engine, config, logger):
    logger.info(f"Stage 4: Loading data to table: {config['target_table']}")
    # Load to DB
    df.to_sql(config['target_table'], engine, if_exists="replace", index=False)
    
    # Load to CSV
    csv_path = os.path.join(config['output_dir'], config['csv_filename'])
    df.to_csv(csv_path, index=False)
    logger.info(f"Data successfully saved to CSV: {csv_path}")

def main():
    # --- Tier 3: Load Configuration ---
    config_file = "config.json"
    if not os.path.exists(config_file):
        print(f"Error: {config_file} not found!")
        return

    with open(config_file, "r") as f:
        config = json.load(f)

    # Setup Directory and Logger
    os.makedirs(config['output_dir'], exist_ok=True)
    logger = setup_logging(config['log_file'])
    
    logger.info("🚀 Starting Tier 3 Framework Pipeline")
    engine = create_engine(config['db_url'])
    
    try:
        data = extract(engine, logger)
        processed_df = transform(data, logger)
        
        if not processed_df.empty:
            if validate_and_report(processed_df, config['output_dir'], logger):
                load(processed_df, engine, config, logger)
                logger.info("✨ ETL Framework Run Successfully!")
            else:
                logger.error("Validation failed. Pipeline stopped.")
        else:
            logger.warning("No data found to process.")
            
    except Exception as e:
        logger.critical(f"Pipeline crashed with error: {e}")

if __name__ == "__main__":
    main()