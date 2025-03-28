# === Imports ===
import os
import csv
import mysql.connector
from dotenv import load_dotenv
from config.logging_config import setup_logging  
from utils.input_validator import validate_csv  

# === Load environment variables and setup logger ===
load_dotenv()
logger = setup_logging()

# === Connect to Database ===
def create_db_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB")
        )
        logger.info("✅ Successfully connected to MySQL.")
        return connection
    except mysql.connector.Error as err:
        logger.error(f"❌ Database connection failed: {err}")
        raise

# === ETL Processing Functions ===

def process_sales_summary():
    try:
        file_path = os.path.join(os.getenv("STAGING_DIR"), "feb_sales_summary.csv")
        expected_columns = [
            "Day", "Shipping region", "Shipping city", "Order UTM source", "Order UTM medium",
            "Order UTM campaign", "Referring channel", "Total sales", "Gross sales",
            "Discounts", "Shipping charges", "Taxes", "Net sales"
        ]
        validate_csv(file_path, expected_columns)

        conn = create_db_connection()
        cursor = conn.cursor()
        batch = []

        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                batch.append((
                    row["Day"], row["Shipping region"], row["Shipping city"],
                    row["Order UTM source"], row["Order UTM medium"], row["Order UTM campaign"],
                    row["Referring channel"], float(row["Total sales"].replace(',', '')),
                    float(row["Gross sales"].replace(',', '')), float(row["Discounts"].replace(',', '')),
                    float(row["Shipping charges"].replace(',', '')), float(row["Taxes"].replace(',', '')),
                    float(row["Net sales"].replace(',', ''))
                ))

                if len(batch) == 100:
                    cursor.executemany("""
                        INSERT INTO sales_fact (
                            day, shipping_region, shipping_city, order_utm_source, order_utm_medium,
                            order_utm_campaign, referring_channel, total_sales, gross_sales,
                            discounts, shipping_charges, taxes, net_sales
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, batch)
                    conn.commit()
                    batch.clear()

            if batch:
                cursor.executemany("""
                    INSERT INTO sales_fact (
                        day, shipping_region, shipping_city, order_utm_source, order_utm_medium,
                        order_utm_campaign, referring_channel, total_sales, gross_sales,
                        discounts, shipping_charges, taxes, net_sales
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                conn.commit()

        cursor.close()
        conn.close()
        logger.info("✅ Sales summary ETL completed.")

    except Exception as e:
        logger.error(f"❌ Error in Sales Summary ETL: {e}")

def process_sessions_location():
    try:
        file_path = os.path.join(os.getenv("STAGING_DIR"), "feb_sessions_by_location.csv")
        expected_columns = [
            "Session country", "Session region", "Session city",
            "Online store visitors", "Sessions"
        ]
        validate_csv(file_path, expected_columns)

        conn = create_db_connection()
        cursor = conn.cursor()
        batch = []

        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                batch.append((
                    row["Session country"], row["Session region"], row["Session city"],
                    int(row["Online store visitors"].replace(',', '')),
                    int(row["Sessions"].replace(',', ''))
                ))

                if len(batch) == 100:
                    cursor.executemany("""
                        INSERT INTO sessions_by_location_fact (
                            session_country, session_region, session_city, 
                            online_store_visitors, sessions
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, batch)
                    conn.commit()
                    batch.clear()

            if batch:
                cursor.executemany("""
                    INSERT INTO sessions_by_location_fact (
                        session_country, session_region, session_city, 
                        online_store_visitors, sessions
                    ) VALUES (%s, %s, %s, %s, %s)
                """, batch)
                conn.commit()

        cursor.close()
        conn.close()
        logger.info("✅ Sessions by location ETL completed.")

    except Exception as e:
        logger.error(f"❌ Error in Sessions by Location ETL: {e}")

def process_page_sessions():
    try:
        file_path = os.path.join(os.getenv("STAGING_DIR"), "feb_sessions_by_day.csv")
        expected_columns = [
            "Day", "Landing page path", "Sessions", "Conversion rate"
        ]
        validate_csv(file_path, expected_columns)

        conn = create_db_connection()
        cursor = conn.cursor()
        batch = []

        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                batch.append((
                    row["Day"], row["Landing page path"],
                    int(row["Sessions"].replace(',', '')),
                    float(row["Conversion rate"].replace('%', '').strip()) / 100
                ))

                if len(batch) == 100:
                    cursor.executemany("""
                        INSERT INTO page_sessions_fact (
                            day, landing_page_path, sessions, conversion_rate
                        ) VALUES (%s, %s, %s, %s)
                    """, batch)
                    conn.commit()
                    batch.clear()

            if batch:
                cursor.executemany("""
                    INSERT INTO page_sessions_fact (
                        day, landing_page_path, sessions, conversion_rate
                    ) VALUES (%s, %s, %s, %s)
                """, batch)
                conn.commit()

        cursor.close()
        conn.close()
        logger.info("✅ Page sessions ETL completed.")

    except Exception as e:
        logger.error(f"❌ Error in Page Sessions ETL: {e}")

# === Entry Point ===
def main():
    logger.info("🚀 Shopify ETL pipeline started.")
    try:
        logger.info("👉 Starting Sales Summary ETL.")
        process_sales_summary()

        logger.info("👉 Starting Sessions by Location ETL.")
        process_sessions_location()

        logger.info("👉 Starting Page Sessions ETL.")
        process_page_sessions()

        logger.info("✅ All ETL processes completed successfully!")

    except Exception as e:
        logger.error(f"🔥 ETL pipeline encountered an error: {e}")

    logger.info("📜 ETL pipeline run ended.")

if __name__ == "__main__":
    main()
