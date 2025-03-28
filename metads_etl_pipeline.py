# === Import Required Libraries ===
import os
import logging
import requests
import pandas as pd
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import json

load_dotenv()

# === Logging Setup ===
logger = logging.getLogger("meta_ads_etl")
logger.setLevel(logging.INFO)

log_file_path = os.path.join(os.getenv("LOG_DIR"), 'meta_ads_etl.log')
file_handler = logging.FileHandler(log_file_path)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.info("Meta Ads ETL Pipeline started.")

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

# === Fetch Data from Meta Ads API ===
def fetch_meta_ads_data(account_id, start_date, end_date):
    try:
        access_token = os.getenv("META_ACCESS_TOKEN")
        base_url = f"https://graph.facebook.com/v22.0/{account_id}"

        # Fetch campaign-level metadata
        campaigns_url = f"{base_url}/campaigns"
        campaign_params = {
            "fields": "id,name,status,daily_budget",
            "access_token": access_token
        }
        campaigns_response = requests.get(campaigns_url, params=campaign_params).json()
        all_campaigns = campaigns_response.get("data", [])
        while "paging" in campaigns_response and "next" in campaigns_response["paging"]:
            next_url = campaigns_response["paging"]["next"]
            campaigns_response = requests.get(next_url).json()
            all_campaigns.extend(campaigns_response.get("data", []))

        campaign_info = {
            str(c["id"]): {
                "name": c.get("name", "MISSING_NAME"),
                "status": c.get("status", "MISSING_STATUS"),
                "daily_budget": float(c.get("daily_budget", 0)) / 100
            }
            for c in all_campaigns
        }

        # Attribution settings
        adsets_url = f"{base_url}/adsets"
        adsets_params = {
            "fields": "id,campaign_id,attribution_setting",
            "access_token": access_token
        }
        adsets_response = requests.get(adsets_url, params=adsets_params).json()
        adset_attribution = {
            adset.get("campaign_id"): adset.get("attribution_setting", "UNKNOWN")
            for adset in adsets_response.get("data", [])
        }

        # Campaign insights with purchase ROAS
        insights_url = f"{base_url}/insights"
        insights_params = {
            "fields": "campaign_id,spend,reach,impressions,clicks,ctr,cpc,cpm,purchase_roas,actions",
            "time_range": json.dumps({"since": start_date, "until": end_date}),
            "level": "campaign",
            "access_token": access_token
        }
        insights_response = requests.get(insights_url, params=insights_params).json()
        data = []

        for campaign in insights_response.get("data", []):
            campaign_id = campaign.get("campaign_id")
            spend = float(campaign.get("spend", 0))
            roas_value = float(next((roas.get("value", 0) for roas in campaign.get("purchase_roas", [])), 0))
            revenue = spend * roas_value

            logger.info(f"Campaign {campaign_id} | Spend: {spend} | ROAS: {roas_value} | Revenue: {revenue}")

            conversions = int(next(
                (x.get("value", 0) for x in campaign.get("actions", [])
                 if x.get("action_type") in ["purchase", "offsite_conversion.fb_pixel_purchase"]),
                0
            ))
            link_clicks = int(next(
                (x.get("value", 0) for x in campaign.get("actions", [])
                 if x.get("action_type") in ["link_click", "onsite_link_click"]),
                0
            ))
            cost_per_result = spend / conversions if conversions > 0 else 0

            campaign_name = campaign_info.get(str(campaign_id), {}).get("name", "MISSING_NAME")
            campaign_status = campaign_info.get(str(campaign_id), {}).get("status", "MISSING_STATUS")
            budget = campaign_info.get(str(campaign_id), {}).get("daily_budget", 0)
            attribution_setting = adset_attribution.get(campaign_id, "UNKNOWN")
            customer_id = account_id.replace("act_", "")

            data.append({
                "campaign_id": campaign_id,
                "customer_id": customer_id,
                "campaign_name": campaign_name,
                "status": campaign_status,
                "attribution_setting": attribution_setting,
                "start_date": start_date,
                "end_date": end_date,
                "budget": budget,
                "amount_spent": spend,
                "reach": campaign.get("reach", 0),
                "impressions": campaign.get("impressions", 0),
                "clicks": campaign.get("clicks", 0),
                "ctr": campaign.get("ctr", 0),
                "cpc": campaign.get("cpc", 0),
                "cpm": campaign.get("cpm", 0),
                "conversions": conversions,
                "purchase_roas": roas_value,
                "revenue": revenue,
                "link_clicks": link_clicks,
                "cost_per_result": cost_per_result
            })

        logger.info(f"{len(data)} campaigns fetched from Meta Ads API.")
        return data

    except Exception as e:
        logger.error(f"Error fetching data from Meta Ads API: {e}")
        raise

# === Load Data into MySQL ===
def load_data_into_mysql(data):
    try:
        conn = create_secure_db_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO meta_ads_campaigns_fact (
            campaign_id, customer_id, campaign_name, status, attribution_setting,
            start_date, end_date, budget, amount_spent, reach, impressions, clicks, ctr, cpc, cpm,
            conversions, purchase_roas, revenue, link_clicks, cost_per_result
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            campaign_name = VALUES(campaign_name),
            status = VALUES(status),
            budget = VALUES(budget),
            amount_spent = VALUES(amount_spent),
            reach = VALUES(reach),
            impressions = VALUES(impressions),
            clicks = VALUES(clicks),
            ctr = VALUES(ctr),
            cpc = VALUES(cpc),
            cpm = VALUES(cpm),
            conversions = VALUES(conversions),
            purchase_roas = VALUES(purchase_roas),
            revenue = VALUES(revenue),
            link_clicks = VALUES(link_clicks),
            cost_per_result = VALUES(cost_per_result);
        """

        insert_data = [
            (
                row.get("campaign_id"),
                row.get("customer_id"),
                row.get("campaign_name"),
                row.get("status"),
                row.get("attribution_setting"),
                row.get("start_date"),
                row.get("end_date"),
                row.get("budget"),
                row.get("amount_spent"),
                row.get("reach"),
                row.get("impressions"),
                row.get("clicks"),
                row.get("ctr"),
                row.get("cpc"),
                row.get("cpm"),
                row.get("conversions"),
                row.get("purchase_roas"),
                row.get("revenue"),
                row.get("link_clicks"),
                row.get("cost_per_result")
            )
            for row in data
        ]

        cursor.executemany(insert_query, insert_data)
        conn.commit()
        logger.info(f"{cursor.rowcount} rows inserted/updated in meta_ads_campaigns_fact.")
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error inserting data into MySQL: {e}")
        raise

# === Main Pipeline ===
def main():
    try:
        logger.info("Starting Meta Ads data extraction.")
        start_date = "2025-02-22"
        end_date = "2025-02-28"
        account_id = os.getenv("META_AD_ACCOUNT_ID")

        data = fetch_meta_ads_data(account_id, start_date, end_date)
        if data:
            logger.info("Loading data into MySQL.")
            load_data_into_mysql(data)
            logger.info("Meta Ads ETL pipeline completed successfully!")
        else:
            logger.info("No data found for the given period.")

    except Exception as e:
        logger.error(f"Meta Ads ETL pipeline failed: {e}")

if __name__ == "__main__":
    main()
