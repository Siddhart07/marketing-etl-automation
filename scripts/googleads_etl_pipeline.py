# === Import Required Libraries ===
import os
import mysql.connector
import logging
from google.ads.googleads.client import GoogleAdsClient
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# === Logging Setup ===
logger = logging.getLogger("google_ads_etl")
logger.setLevel(logging.INFO)

log_file_path = os.path.join(os.getenv("LOG_DIR"), 'google_ads_etl.log')
file_handler = logging.FileHandler(log_file_path)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.info("Google Ads ETL Pipeline started.")

# === Secure MySQL Connection ===
def create_secure_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database="shopify_etl"
        )
        logger.info("Successfully connected to MySQL.")
        return conn
    except mysql.connector.Error as err:
        logger.error(f"MySQL connection error: {err}")
        raise

# === Google Ads API Connection ===
def create_google_ads_client():
    try:
        config_dict = {
            "developer_token": os.getenv("GOOGLE_DEVELOPER_TOKEN"),
            "client_id": os.getenv("GOOGLE_CLIENT_ID"),
            "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
            "refresh_token": os.getenv("GOOGLE_REFRESH_TOKEN"),
            "login_customer_id": os.getenv("GOOGLE_CUSTOMER_ID"),
            "token_uri": "https://oauth2.googleapis.com/token",
            "use_proto_plus": True
        }
        client = GoogleAdsClient.load_from_dict(config_dict)
        logger.info("Google Ads API client created successfully.")
        return client
    except Exception as e:
        logger.error(f"Error creating Google Ads client: {e}")
        raise

# === List all linked client accounts under MCC ===
def list_linked_client_accounts(client, manager_id):
    try:
        query = """
            SELECT
                customer_client.client_customer,
                customer_client.descriptive_name,
                customer_client.id
            FROM customer_client
            WHERE customer_client.manager = false
        """
        service = client.get_service("GoogleAdsService")
        response = service.search_stream(customer_id=str(manager_id), query=query)
        
        linked_clients = []
        for batch in response:
            for row in batch.results:
                linked_clients.append((row.customer_client.descriptive_name, row.customer_client.id))
                logger.info(f"Found client: {row.customer_client.descriptive_name} (ID: {row.customer_client.id})")
        
        return linked_clients

    except Exception as e:
        logger.error(f"Error listing linked clients: {e}")
        raise

# === Fetch Data from Google Ads ===
def fetch_google_ads_data(client, chosen_client_id, start_date, end_date):
    try:
        query = f"""
            SELECT 
                campaign.id, 
                campaign.name, 
                campaign.status, 
                customer.id,  
                campaign.bidding_strategy_type,  
                campaign_budget.amount_micros,
                metrics.impressions, 
                metrics.clicks, 
                metrics.ctr, 
                metrics.average_cpc,  
                metrics.cost_micros, 
                metrics.conversions, 
                metrics.conversions_value, 
                metrics.interaction_rate 
            FROM campaign 
            WHERE campaign.status = 'ENABLED'
            AND segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
        google_ads_service = client.get_service("GoogleAdsService")
        response = google_ads_service.search(customer_id=str(chosen_client_id), query=query)
        results = list(response)
        logger.info(f"{len(results)} rows fetched for period {start_date} to {end_date}")
        return results
    except Exception as e:
        logger.error(f"Error fetching data from Google Ads: {e}")
        raise

# === Data Loading ===
def load_data_into_mysql(data, start_date, end_date):
    try:
        conn = create_secure_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO google_ads_campaigns_fact (
            campaign_id, campaign_name, status, start_date, end_date, customer_id,
            bid_strategy_type, budget, impressions, clicks, ctr, avg_cost, cost,
            conversions, conv_value, interaction_rate, ROAS
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        for row in data:
            budget = row.campaign_budget.amount_micros / 1_000_000
            cost = row.metrics.cost_micros / 1_000_000
            avg_cost = row.metrics.average_cpc / 1_000_000
            roas = row.metrics.conversions_value / cost if cost > 0 else 0
            
            cursor.execute(insert_query, (
                row.campaign.id,
                row.campaign.name,
                row.campaign.status.name,
                start_date,
                end_date,
                row.customer.id,
                row.campaign.bidding_strategy_type.name,
                budget,
                row.metrics.impressions,
                row.metrics.clicks,
                row.metrics.ctr,
                avg_cost,
                cost,
                row.metrics.conversions,
                row.metrics.conversions_value,
                row.metrics.interaction_rate,
                roas
            ))
        
        conn.commit()
        logger.info(f"{cursor.rowcount} rows inserted in google_ads_campaigns_fact table.")
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error inserting data into MySQL: {e}")
        raise

# === Pipeline Main ===
def main():
    try:
        logger.info("Starting Google Ads data extraction.")
        client = create_google_ads_client()
        manager_id = os.getenv("GOOGLE_CUSTOMER_ID").strip()
        linked_clients = list_linked_client_accounts(client, manager_id)

        print("\nAvailable linked client accounts:")
        for i, (name, client_id) in enumerate(linked_clients, 1):
            print(f"{i}. {name} (ID: {client_id})")

        while True:
            choice_input = input("\nEnter the number of the client account you want to load data for: ").strip()
            if choice_input.isdigit() and 1 <= int(choice_input) <= len(linked_clients):
                choice = int(choice_input)
                break
            else:
                print("Invalid input. Please enter a valid number from the list.")

        chosen_client_id = linked_clients[choice - 1][1]

        start_date = "2025-03-01"
        end_date = "2025-03-07"

        data = fetch_google_ads_data(client, chosen_client_id, start_date, end_date)

        if data:
            logger.info("Loading data into MySQL.")
            load_data_into_mysql(data, start_date, end_date)
            logger.info("Google Ads ETL pipeline completed successfully!")
        else:
            logger.info("No data found for the given period.")

    except Exception as e:
        logger.error(f"Google Ads ETL pipeline failed: {e}")

if __name__ == "__main__":
    main()
