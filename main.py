"""
Cloud Run: WooCommerce → BigQuery Daily Sync
Fetches orders, order items, products, and customers from WooCommerce REST API
and loads to BigQuery for KPI reporting and trending analysis.
Incremental batch processing using sync_metadata cursor and Firestore locking.
"""
from flask import Flask, jsonify
import logging
import json
import base64
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import secretmanager

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# GCP Configuration
PROJECT_ID = "truerc-kpi-dashboard"
DATASET_ID = "woocommerce"
WOOCOMMERCE_URL = "https://www.truerc.ca"
WOO_API_BASE = f"{WOOCOMMERCE_URL}/wp-json/wc/v3"

# Lock TTL: 15 minutes
LOCK_TTL_SECONDS = 900

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds, exponential

def get_secret(secret_id: str) -> str:
    """Retrieve secret from Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Failed to retrieve secret '{secret_id}': {e}")
        raise

def get_woo_headers() -> Dict[str, str]:
    """Generate WooCommerce API headers with basic auth."""
    consumer_key = get_secret("woo_consumer_key")
    consumer_secret = get_secret("woo_consumer_secret")
    credentials = base64.b64encode(f"{consumer_key}:{consumer_secret}".encode()).decode()
    return {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/json",
    }

def fetch_woo_data_batch(
    endpoint: str, page: int, per_page: int = 1000, after: Optional[str] = None,
    orderby: Optional[str] = "date", order: Optional[str] = "asc",
) -> List[Dict]:
    """Fetch ONE page of data from WooCommerce API with retry logic."""
    url = f"{WOO_API_BASE}{endpoint}"
    headers = get_woo_headers()
    params = {"per_page": per_page, "page": page}
    if orderby: params["orderby"] = orderby
    if order: params["order"] = order
    if after: params["after"] = after

    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Fetching {endpoint} page {page} (attempt {attempt + 1}/{MAX_RETRIES})...")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if "application/json" not in response.headers.get("content-type", ""):
                logger.error(f"Invalid content type: {response.headers.get('content-type')}")
                raise ValueError(f"Expected JSON, got {response.headers.get('content-type')}")
            response.raise_for_status()
            records = response.json()
            logger.info(f"✓ Fetched {len(records)} records (page {page})")
            return records
        except requests.exceptions.HTTPError as e:
            if response.status_code in [429, 503] and attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF ** (attempt + 1)
                logger.warning(f"Rate limited/unavailable, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            logger.error(f"HTTP error {response.status_code}: {e}")
            raise
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF ** (attempt + 1)
                logger.warning(f"Failed to fetch (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
                continue
            logger.error(f"Failed to fetch {endpoint} page {page}: {e}")
            raise
    return []

def fetch_woo_data_all(
    endpoint: str, per_page: int = 1000, after: Optional[str] = None,
    orderby: Optional[str] = None, order: Optional[str] = None,
) -> List[Dict]:
    """Fetch ALL pages of data from WooCommerce API."""
    all_records = []
    page = 1
    while True:
        records = fetch_woo_data_batch(endpoint, page, per_page, after, orderby, order)
        if not records:
            break
        all_records.extend(records)
        if len(records) < per_page:
            break
        page += 1
    logger.info(f"✓ Total {endpoint} records fetched: {len(all_records)}")
    return all_records

def load_customers_to_bigquery(customers: List[Dict]) -> int:
    """Transform and load customers to BigQuery with billing/shipping addresses."""
    if not customers:
        return 0
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.customers"
    transformed = []
    
    for customer in customers:
        try:
            billing = customer.get("billing", {})
            shipping = customer.get("shipping", {})
            
            transformed.append({
                "customer_id": customer.get("id"),
                "email": customer.get("email"),
                "first_name": customer.get("first_name"),
                "last_name": customer.get("last_name"),
                "username": customer.get("username"),
                "role": customer.get("role"),
                "is_paying_customer": customer.get("is_paying_customer"),
                "date_created": customer.get("date_created"),
                "date_modified": customer.get("date_modified"),
                "billing_first_name": billing.get("first_name"),
                "billing_last_name": billing.get("last_name"),
                "billing_company": billing.get("company"),
                "billing_address_1": billing.get("address_1"),
                "billing_address_2": billing.get("address_2"),
                "billing_city": billing.get("city"),
                "billing_state": billing.get("state"),
                "billing_postcode": billing.get("postcode"),
                "billing_country": billing.get("country"),
                "billing_email": billing.get("email"),
                "billing_phone": billing.get("phone"),
                "shipping_first_name": shipping.get("first_name"),
                "shipping_last_name": shipping.get("last_name"),
                "shipping_company": shipping.get("company"),
                "shipping_address_1": shipping.get("address_1"),
                "shipping_address_2": shipping.get("address_2"),
                "shipping_city": shipping.get("city"),
                "shipping_state": shipping.get("state"),
                "shipping_postcode": shipping.get("postcode"),
                "shipping_country": shipping.get("country"),
                "shipping_phone": shipping.get("phone"),
                "updated_at": datetime.utcnow().isoformat(),
            })
        except Exception as e:
            logger.warning(f"Failed to transform customer {customer.get('id')}: {e}")
            continue

    if not transformed:
        return 0

    temp_table = f"{PROJECT_ID}.{DATASET_ID}.temp_customers_{int(datetime.utcnow().timestamp())}"
    try:
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, autodetect=True)
        load_job = client.load_table_from_json(transformed, temp_table, job_config=job_config)
        load_job.result()
        logger.info(f"✓ Loaded {len(transformed)} customers to temp table")

        merge_query = f"""
        MERGE INTO `{table_id}` T
        USING `{temp_table}` S
        ON T.customer_id = S.customer_id
        WHEN MATCHED THEN UPDATE SET 
          email = S.email, first_name = S.first_name, last_name = S.last_name, 
          username = S.username, role = S.role, is_paying_customer = S.is_paying_customer, 
          date_modified = S.date_modified,
          billing_first_name = S.billing_first_name, billing_last_name = S.billing_last_name,
          billing_company = S.billing_company, billing_address_1 = S.billing_address_1,
          billing_address_2 = S.billing_address_2, billing_city = S.billing_city,
          billing_state = S.billing_state, billing_postcode = S.billing_postcode,
          billing_country = S.billing_country, billing_email = S.billing_email,
          billing_phone = S.billing_phone,
          shipping_first_name = S.shipping_first_name, shipping_last_name = S.shipping_last_name,
          shipping_company = S.shipping_company, shipping_address_1 = S.shipping_address_1,
          shipping_address_2 = S.shipping_address_2, shipping_city = S.shipping_city,
          shipping_state = S.shipping_state, shipping_postcode = S.shipping_postcode,
          shipping_country = S.shipping_country, shipping_phone = S.shipping_phone,
          updated_at = S.updated_at
        WHEN NOT MATCHED THEN INSERT 
          (customer_id, email, first_name, last_name, username, role, is_paying_customer,
           date_created, date_modified, billing_first_name, billing_last_name,
           billing_company, billing_address_1, billing_address_2, billing_city, billing_state,
           billing_postcode, billing_country, billing_email, billing_phone,
           shipping_first_name, shipping_last_name, shipping_company, shipping_address_1,
           shipping_address_2, shipping_city, shipping_state, shipping_postcode,
           shipping_country, shipping_phone, updated_at)
        VALUES 
          (S.customer_id, S.email, S.first_name, S.last_name, S.username, S.role,
           S.is_paying_customer, S.date_created, S.date_modified, S.billing_first_name,
           S.billing_last_name, S.billing_company, S.billing_address_1, S.billing_address_2,
           S.billing_city, S.billing_state, S.billing_postcode, S.billing_country,
           S.billing_email, S.billing_phone, S.shipping_first_name, S.shipping_last_name,
           S.shipping_company, S.shipping_address_1, S.shipping_address_2, S.shipping_city,
           S.shipping_state, S.shipping_postcode, S.shipping_country, S.shipping_phone,
           S.updated_at)
        """
        client.query(merge_query).result()
        logger.info(f"✓ MERGE completed for customers ({len(transformed)} rows)")
        return len(transformed)
    finally:
        try:
            client.delete_table(temp_table, not_found_ok=True)
        except Exception as e:
            logger.warning(f"Failed to delete temp table: {e}")

@app.route('/init', methods=['POST'])
def init_products_customers():
    """Initialize products and customers (idempotent)."""
    logger.info("Checking if products/customers already initialized...")
    try:
        client = bigquery.Client(project=PROJECT_ID)
        product_result = client.query(f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET_ID}.products`").result()
        product_count = next(product_result)[0]
        customer_result = client.query(f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET_ID}.customers`").result()
        customer_count = next(customer_result)[0]

        if product_count > 0 and customer_count > 0:
            logger.info(f"Both products ({product_count}) and customers ({customer_count}) already initialized.")
            return jsonify({
                "status": "already_initialized",
                "message": f"Products/customers already loaded ({product_count} products, {customer_count} customers).",
                "timestamp": datetime.utcnow().isoformat(),
            }), 200

        customers_loaded = 0
        if customer_count == 0:
            logger.info("Customers table empty. Fetching all customers...")
            customers = fetch_woo_data_all("/customers")
            if customers:
                customers_loaded = load_customers_to_bigquery(customers)
                logger.info(f"✓ Loaded {customers_loaded} customers")

        return jsonify({
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "customers_loaded": customers_loaded,
            "customers_total": customer_count + customers_loaded,
        }), 200
    except Exception as e:
        logger.error(f"✗ Init failed: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
