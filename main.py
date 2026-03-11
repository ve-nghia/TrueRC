"""
Cloud Run: WooCommerce → BigQuery Daily Sync
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

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "truerc-kpi-dashboard"
DATASET_ID = "woocommerce"
WOOCOMMERCE_URL = "https://www.truerc.ca"
WOO_API_BASE = f"{WOOCOMMERCE_URL}/wp-json/wc/v3"
LOCK_TTL_SECONDS = 900
MAX_RETRIES = 3
RETRY_BACKOFF = 2

def get_secret(secret_id: str) -> str:
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Failed to retrieve secret '{secret_id}': {e}")
        raise

def get_woo_headers() -> Dict[str, str]:
    consumer_key = get_secret("woo_consumer_key")
    consumer_secret = get_secret("woo_consumer_secret")
    credentials = base64.b64encode(f"{consumer_key}:{consumer_secret}".encode()).decode()
    return {"Authorization": f"Basic {credentials}", "Content-Type": "application/json"}

def load_woocommerce_sync_state() -> Dict[str, Any]:
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"SELECT last_order_id, batch_number, records_loaded_total FROM `{PROJECT_ID}.{DATASET_ID}.sync_state_woocommerce` WHERE id = 'current' LIMIT 1"
        result = client.query(query).result()
        rows = list(result)
        if rows:
            row = rows[0]
            return {"last_order_id": row.last_order_id, "batch_number": row.batch_number, "records_loaded_total": row.records_loaded_total}
    except Exception as e:
        logger.warning(f"Could not load sync_state: {e}")
    return {"last_order_id": None, "batch_number": 0, "records_loaded_total": 0}

def update_woocommerce_sync_state(last_order_id: int, batch_number: int, records_loaded_total: int) -> None:
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""MERGE INTO `{PROJECT_ID}.{DATASET_ID}.sync_state_woocommerce` T
        USING (SELECT @id as id) S ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET last_order_id = @last_id, batch_number = @batch_num, records_loaded_total = @records_total, updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (id, last_order_id, batch_number, records_loaded_total, updated_at) VALUES (@id, @last_id, @batch_num, @records_total, CURRENT_TIMESTAMP())"""
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", "current"),
            bigquery.ScalarQueryParameter("last_id", "INT64", last_order_id),
            bigquery.ScalarQueryParameter("batch_num", "INT64", batch_number),
            bigquery.ScalarQueryParameter("records_total", "INT64", records_loaded_total),
        ])
        client.query(query, job_config=job_config).result()
        logger.info(f"✓ Updated sync_state: batch={batch_number}, last_order_id={last_order_id}, total={records_loaded_total}")
    except Exception as e:
        logger.error(f"Failed to update sync_state: {e}")
        raise

def update_sync_metadata(table_name: str, records_loaded: int) -> None:
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""MERGE INTO `{PROJECT_ID}.{DATASET_ID}.sync_metadata` T
        USING (SELECT @table_name as sync_name) S ON T.sync_name = S.sync_name
        WHEN MATCHED THEN UPDATE SET last_sync_time = CURRENT_TIMESTAMP(), records_loaded = @records, updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (sync_name, last_sync_time, records_loaded, updated_at) VALUES (@table_name, CURRENT_TIMESTAMP(), @records, CURRENT_TIMESTAMP())"""
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
            bigquery.ScalarQueryParameter("records", "INT64", records_loaded),
        ])
        client.query(query, job_config=job_config).result()
        logger.info(f"✓ Updated sync_metadata for {table_name}: {records_loaded} records")
    except Exception as e:
        logger.error(f"Failed to update sync_metadata: {e}")

def fetch_woo_data_batch(endpoint: str, page: int, per_page: int = 100, after: Optional[str] = None, orderby: Optional[str] = "date", order: Optional[str] = "asc") -> List[Dict]:
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
                raise ValueError(f"Expected JSON, got {response.headers.get('content-type')}")
            response.raise_for_status()
            records = response.json()
            logger.info(f"✓ Fetched {len(records)} records (page {page})")
            return records
        except requests.exceptions.HTTPError as e:
            if response.status_code in [429, 503] and attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF ** (attempt + 1)
                logger.warning(f"Rate limited, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            logger.error(f"HTTP error {response.status_code}: {e}")
            raise
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF ** (attempt + 1)
                logger.warning(f"Failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
                continue
            raise
    return []

def fetch_woo_data_all(endpoint: str, per_page: int = 100, after: Optional[str] = None, orderby: Optional[str] = "date", order: Optional[str] = "asc") -> List[Dict]:
    all_records = []
    page = 1
    while True:
        records = fetch_woo_data_batch(endpoint, page, per_page, after, orderby, order)
        if not records: break
        all_records.extend(records)
        if len(records) < per_page: break
        page += 1
    logger.info(f"✓ Total {endpoint} records fetched: {len(all_records)}")
    return all_records

def load_orders_to_bigquery(orders: List[Dict]) -> int:
    if not orders: return 0
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.orders"
    transformed = []
    for order in orders:
        try:
            shipping_lines = order.get("shipping_lines", [])
            shipping_method = shipping_lines[0].get("method_title") if shipping_lines else None
            refunds = order.get("refunds", [])
            refund_total = sum(float(r.get("total", 0)) for r in refunds) if refunds else 0.0
            billing = order.get("billing", {})
            shipping = order.get("shipping", {})
            def safe_float(val):
                try:
                    if val is None or val == "": return None
                    f = float(val)
                    return f if f != 0 else None
                except (ValueError, TypeError):
                    return None
            
            transformed.append({
                "order_id": order.get("id"),
                "order_number": order.get("number"),
                "status": order.get("status"),
                "currency": order.get("currency"),
                "date_created": order.get("date_created"),
                "date_modified": order.get("date_modified"),
                "date_paid": order.get("date_paid"),
                "date_completed": order.get("date_completed"),
                "customer_id": order.get("customer_id"),
                "payment_method": order.get("payment_method"),
                "payment_method_title": order.get("payment_method_title"),
                "transaction_id": order.get("transaction_id"),
                "subtotal": safe_float(order.get("subtotal")),
                "cart_tax": safe_float(order.get("cart_tax")),
                "shipping_total": safe_float(order.get("shipping_total")),
                "shipping_tax": safe_float(order.get("shipping_tax")),
                "discount_total": safe_float(order.get("discount_total")),
                "discount_tax": safe_float(order.get("discount_tax")),
                "total_tax": safe_float(order.get("total_tax")),
                "total": safe_float(order.get("total")),
                "shipping_method": shipping_method,
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
                "refund_total": refund_total if refund_total > 0 else None,
                "customer_note": order.get("customer_note"),
                "created_via": order.get("created_via"),
                "updated_at": datetime.utcnow().isoformat(),
            })
        except Exception as e:
            logger.warning(f"Failed to transform order {order.get('id')}: {e}")
    if not transformed: return 0
    temp_table = f"{PROJECT_ID}.{DATASET_ID}.temp_orders_{int(datetime.utcnow().timestamp())}"
    try:
        schema = [
            bigquery.SchemaField("order_id", "INTEGER"),
            bigquery.SchemaField("order_number", "INTEGER"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("date_created", "TIMESTAMP"),
            bigquery.SchemaField("date_modified", "TIMESTAMP"),
            bigquery.SchemaField("date_paid", "TIMESTAMP"),
            bigquery.SchemaField("date_completed", "TIMESTAMP"),
            bigquery.SchemaField("customer_id", "INTEGER"),
            bigquery.SchemaField("payment_method", "STRING"),
            bigquery.SchemaField("payment_method_title", "STRING"),
            bigquery.SchemaField("transaction_id", "STRING"),
            bigquery.SchemaField("subtotal", "FLOAT64"),
            bigquery.SchemaField("cart_tax", "FLOAT64"),
            bigquery.SchemaField("shipping_total", "FLOAT64"),
            bigquery.SchemaField("shipping_tax", "FLOAT64"),
            bigquery.SchemaField("discount_total", "FLOAT64"),
            bigquery.SchemaField("discount_tax", "FLOAT64"),
            bigquery.SchemaField("total_tax", "FLOAT64"),
            bigquery.SchemaField("total", "FLOAT64"),
            bigquery.SchemaField("shipping_method", "STRING"),
            bigquery.SchemaField("billing_first_name", "STRING"),
            bigquery.SchemaField("billing_last_name", "STRING"),
            bigquery.SchemaField("billing_company", "STRING"),
            bigquery.SchemaField("billing_address_1", "STRING"),
            bigquery.SchemaField("billing_address_2", "STRING"),
            bigquery.SchemaField("billing_city", "STRING"),
            bigquery.SchemaField("billing_state", "STRING"),
            bigquery.SchemaField("billing_postcode", "STRING"),
            bigquery.SchemaField("billing_country", "STRING"),
            bigquery.SchemaField("billing_email", "STRING"),
            bigquery.SchemaField("billing_phone", "STRING"),
            bigquery.SchemaField("shipping_first_name", "STRING"),
            bigquery.SchemaField("shipping_last_name", "STRING"),
            bigquery.SchemaField("shipping_company", "STRING"),
            bigquery.SchemaField("shipping_address_1", "STRING"),
            bigquery.SchemaField("shipping_address_2", "STRING"),
            bigquery.SchemaField("shipping_city", "STRING"),
            bigquery.SchemaField("shipping_state", "STRING"),
            bigquery.SchemaField("shipping_postcode", "STRING"),
            bigquery.SchemaField("shipping_country", "STRING"),
            bigquery.SchemaField("shipping_phone", "STRING"),
            bigquery.SchemaField("refund_total", "FLOAT64"),
            bigquery.SchemaField("customer_note", "STRING"),
            bigquery.SchemaField("created_via", "STRING"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        load_job = client.load_table_from_json(transformed, temp_table, job_config=job_config)
        load_job.result()
        cols = ["order_number", "status", "currency", "date_created", "date_modified", "date_paid", "date_completed", "customer_id", "payment_method", "payment_method_title", "transaction_id", "subtotal", "cart_tax", "shipping_total", "shipping_tax", "discount_total", "discount_tax", "total_tax", "total", "shipping_method", "billing_first_name", "billing_last_name", "billing_company", "billing_address_1", "billing_address_2", "billing_city", "billing_state", "billing_postcode", "billing_country", "billing_email", "billing_phone", "shipping_first_name", "shipping_last_name", "shipping_company", "shipping_address_1", "shipping_address_2", "shipping_city", "shipping_state", "shipping_postcode", "shipping_country", "shipping_phone", "refund_total", "customer_note", "created_via", "updated_at"]
        update_set = ", ".join([f"{col} = S.{col}" for col in cols])
        insert_cols = "order_id, " + ", ".join(cols)
        insert_vals = "S.order_id, " + ", ".join([f"S.{col}" for col in cols])
        merge_query = f"""MERGE INTO `{table_id}` T USING `{temp_table}` S ON T.order_id = S.order_id
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"""
        client.query(merge_query).result()
        logger.info(f"✓ MERGE completed for orders ({len(transformed)} rows)")
        return len(transformed)
    finally:
        try:
            client.delete_table(temp_table, not_found_ok=True)
        except Exception as e:
            logger.warning(f"Failed to delete temp table: {e}")

def load_order_items_to_bigquery(orders: List[Dict]) -> int:
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.order_items"
    all_items = []
    for order in orders:
        order_id = order.get("id")
        for item in order.get("line_items", []):
            try:
                all_items.append({
                    "order_item_id": f"{order_id}-{item.get('id')}",
                    "order_id": order_id,
                    "product_id": item.get("product_id"),
                    "product_name": item.get("name"),
                    "quantity": int(item.get("quantity", 0)),
                    "price": float(item.get("price", 0)),
                    "total": float(item.get("total", 0)),
                    "updated_at": datetime.utcnow().isoformat(),
                })
            except Exception as e:
                logger.warning(f"Failed to transform order item: {e}")
    if not all_items: return 0
    temp_table = f"{PROJECT_ID}.{DATASET_ID}.temp_order_items_{int(datetime.utcnow().timestamp())}"
    try:
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, autodetect=True)
        load_job = client.load_table_from_json(all_items, temp_table, job_config=job_config)
        load_job.result()
        merge_query = f"""MERGE INTO `{table_id}` T USING `{temp_table}` S ON T.order_item_id = S.order_item_id
        WHEN MATCHED THEN UPDATE SET quantity = S.quantity, price = S.price, total = S.total, updated_at = S.updated_at
        WHEN NOT MATCHED THEN INSERT (order_item_id, order_id, product_id, product_name, quantity, price, total, updated_at) VALUES (S.order_item_id, S.order_id, S.product_id, S.product_name, S.quantity, S.price, S.total, S.updated_at)"""
        client.query(merge_query).result()
        logger.info(f"✓ MERGE completed for order items ({len(all_items)} rows)")
        return len(all_items)
    finally:
        try:
            client.delete_table(temp_table, not_found_ok=True)
        except Exception as e:
            logger.warning(f"Failed to delete temp table: {e}")

def load_customers_to_bigquery(customers: List[Dict]) -> int:
    if not customers: return 0
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.customers"
    transformed = []
    for customer in customers:
        try:
            billing = customer.get("billing", {})
            shipping = customer.get("shipping", {})
            first_name = customer.get("first_name") or billing.get("first_name") or shipping.get("first_name") or None
            last_name = customer.get("last_name") or billing.get("last_name") or shipping.get("last_name") or None
            transformed.append({
                "customer_id": customer.get("id"),
                "email": customer.get("email"),
                "first_name": first_name,
                "last_name": last_name,
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
    if not transformed: return 0
    temp_table = f"{PROJECT_ID}.{DATASET_ID}.temp_customers_{int(datetime.utcnow().timestamp())}"
    try:
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, autodetect=True)
        load_job = client.load_table_from_json(transformed, temp_table, job_config=job_config)
        load_job.result()
        merge_cols = "email, first_name, last_name, username, role, is_paying_customer, date_modified, billing_first_name, billing_last_name, billing_company, billing_address_1, billing_address_2, billing_city, billing_state, billing_postcode, billing_country, billing_email, billing_phone, shipping_first_name, shipping_last_name, shipping_company, shipping_address_1, shipping_address_2, shipping_city, shipping_state, shipping_postcode, shipping_country, shipping_phone, updated_at"
        merge_vals = "S.email, S.first_name, S.last_name, S.username, S.role, S.is_paying_customer, S.date_modified, S.billing_first_name, S.billing_last_name, S.billing_company, S.billing_address_1, S.billing_address_2, S.billing_city, S.billing_state, S.billing_postcode, S.billing_country, S.billing_email, S.billing_phone, S.shipping_first_name, S.shipping_last_name, S.shipping_company, S.shipping_address_1, S.shipping_address_2, S.shipping_city, S.shipping_state, S.shipping_postcode, S.shipping_country, S.shipping_phone, S.updated_at"
        merge_query = f"""MERGE INTO `{table_id}` T USING `{temp_table}` S ON T.customer_id = S.customer_id
        WHEN MATCHED THEN UPDATE SET {merge_cols}
        WHEN NOT MATCHED THEN INSERT (customer_id, {merge_cols}) VALUES (S.customer_id, {merge_vals})"""
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
    logger.info("Checking if products/customers already initialized...")
    try:
        client = bigquery.Client(project=PROJECT_ID)
        product_result = client.query(f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET_ID}.products`").result()
        product_count = next(product_result)[0]
        customer_result = client.query(f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET_ID}.customers`").result()
        customer_count = next(customer_result)[0]
        if product_count > 0 and customer_count > 0:
            return jsonify({"status": "already_initialized", "products": product_count, "customers": customer_count, "timestamp": datetime.utcnow().isoformat()}), 200
        customers_loaded = 0
        if customer_count == 0:
            logger.info("Customers table empty. Fetching...")
            customers = fetch_woo_data_all("/customers", orderby="id", order="asc")
            if customers:
                customers_loaded = load_customers_to_bigquery(customers)
                logger.info(f"✓ Loaded {customers_loaded} customers")
        return jsonify({"status": "success", "customers_loaded": customers_loaded, "customers_total": customer_count + customers_loaded, "timestamp": datetime.utcnow().isoformat()}), 200
    except Exception as e:
        logger.error(f"✗ Init failed: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route('/', methods=['POST'])
def sync_woocommerce():
    logger.info("Starting WooCommerce → BigQuery sync (order backfill)...")
    db = firestore.Client(project=PROJECT_ID)
    lock_ref = db.collection('sync_locks').document('woocommerce-sync')
    try:
        lock_doc = lock_ref.get()
        if lock_doc.exists:
            started_at = datetime.fromisoformat(lock_doc.get('started_at'))
            elapsed = (datetime.utcnow() - started_at).total_seconds()
            if elapsed < LOCK_TTL_SECONDS:
                logger.info(f"Job already running (started {elapsed}s ago), skipping")
                return jsonify({"status": "skipped", "message": "Job already running", "elapsed_seconds": elapsed}), 200
    except Exception as e:
        logger.warning(f"Could not check lock: {e}")
    try:
        lock_ref.set({"started_at": datetime.utcnow().isoformat()})
        logger.info("✓ Lock acquired")
    except Exception as e:
        logger.error(f"Failed to acquire lock: {e}")
        return jsonify({"status": "error", "message": "Could not acquire lock"}), 500
    try:
        sync_state = load_woocommerce_sync_state()
        current_batch_number = sync_state["batch_number"]
        records_loaded_total = sync_state["records_loaded_total"]
        logger.info(f"Resuming from batch {current_batch_number}, total: {records_loaded_total}")
        batch_size = 100
        page_number = current_batch_number + 1
        logger.info(f"Fetching batch {current_batch_number} (page {page_number}, size {batch_size})...")
        batch_orders = fetch_woo_data_batch("/orders", page=page_number, per_page=batch_size, orderby="date", order="asc")
        if not batch_orders:
            logger.info(f"No orders for batch {current_batch_number}, initial load complete")
            return jsonify({"status": "success", "batch": current_batch_number, "records_loaded": 0, "total_records": records_loaded_total, "message": "Initial load complete"}), 200
        logger.info(f"Processing {len(batch_orders)} orders...")
        orders_loaded = load_orders_to_bigquery(batch_orders)
        update_sync_metadata("orders", orders_loaded)
        logger.info("Extracting order items...")
        items_loaded = load_order_items_to_bigquery(batch_orders)
        update_sync_metadata("order_items", items_loaded)
        last_order_id = batch_orders[-1].get("id") if batch_orders else None
        next_batch_number = current_batch_number + 1
        new_total = records_loaded_total + orders_loaded
        update_woocommerce_sync_state(last_order_id, next_batch_number, new_total)
        return jsonify({"status": "success", "batch": current_batch_number, "orders_loaded": orders_loaded, "items_loaded": items_loaded, "total_loaded": new_total, "next_batch": next_batch_number, "timestamp": datetime.utcnow().isoformat()}), 200
    except Exception as e:
        logger.error(f"✗ Sync failed: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500
    finally:
        try:
            lock_ref.delete()
            logger.info("✓ Lock released")
        except Exception as e:
            logger.warning(f"Failed to release lock: {e}")

@app.route('/sync-all', methods=['POST'])
def sync_all():
    logger.info("Starting daily sync (orders, customers, products modified/added in last 24h)...")
    try:
        lookback = (datetime.utcnow() - timedelta(days=1)).isoformat()
        logger.info(f"Fetching updates since {lookback}...")
        
        # Fetch orders modified in last 24h
        logger.info("Fetching orders...")
        orders = fetch_woo_data_all("/orders", after=lookback, orderby="modified", order="asc")
        orders_loaded = load_orders_to_bigquery(orders) if orders else 0
        update_sync_metadata("orders", orders_loaded)
        logger.info(f"✓ Loaded {orders_loaded} orders")
        
        # Extract order items
        logger.info("Extracting order items...")
        items_loaded = load_order_items_to_bigquery(orders) if orders else 0
        update_sync_metadata("order_items", items_loaded)
        logger.info(f"✓ Loaded {items_loaded} order items")
        
        # Fetch customers created in last 24h
        logger.info("Fetching new customers...")
        customers = fetch_woo_data_all("/customers", after=lookback, orderby="date_created", order="asc")
        customers_loaded = load_customers_to_bigquery(customers) if customers else 0
        update_sync_metadata("customers", customers_loaded)
        logger.info(f"✓ Loaded {customers_loaded} customers")
        
        # Fetch products created in last 24h
        logger.info("Fetching new products...")
        products = fetch_woo_data_all("/products", after=lookback, orderby="date_created", order="asc")
        products_loaded = load_products_to_bigquery(products) if products else 0
        update_sync_metadata("products", products_loaded)
        logger.info(f"✓ Loaded {products_loaded} products")
        
        return jsonify({"status": "success", "orders": orders_loaded, "order_items": items_loaded, "customers": customers_loaded, "products": products_loaded, "timestamp": datetime.utcnow().isoformat()}), 200
    except Exception as e:
        logger.error(f"✗ Daily sync failed: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
