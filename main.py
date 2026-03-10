"""
Cloud Run: WooCommerce → BigQuery Daily Sync

Fetches orders, order items, products, and customers from WooCommerce REST API
and loads to BigQuery for KPI reporting and trending analysis.

Incremental daily pulls using sync_metadata cursor.
"""

from flask import Flask, jsonify
import logging
import json
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests
from google.cloud import bigquery
from google.cloud import secretmanager
from google.cloud import firestore

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


def load_woocommerce_sync_state() -> Dict[str, Any]:
    """Load resumable cursor from sync_state_woocommerce table."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
        SELECT last_order_id, batch_number, records_loaded_total
        FROM `{PROJECT_ID}.{DATASET_ID}.sync_state_woocommerce`
        WHERE id = 'current'
        LIMIT 1
        """
        result = client.query(query).result()
        rows = list(result)
        if rows:
            row = rows[0]
            return {
                "last_order_id": row.last_order_id,
                "batch_number": row.batch_number,
                "records_loaded_total": row.records_loaded_total,
            }
    except Exception as e:
        logger.warning(f"Could not load woocommerce sync_state: {e}")
    
    return {
        "last_order_id": None,
        "batch_number": 0,
        "records_loaded_total": 0,
    }


def update_woocommerce_sync_state(last_order_id: int, batch_number: int, records_loaded_total: int) -> None:
    """Update sync state after successful batch."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
        MERGE INTO `{PROJECT_ID}.{DATASET_ID}.sync_state_woocommerce` T
        USING (SELECT @id as id) S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET last_order_id = @last_id, batch_number = @batch_num, records_loaded_total = @records_total, updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (id, last_order_id, batch_number, records_loaded_total, updated_at)
          VALUES (@id, @last_id, @batch_num, @records_total, CURRENT_TIMESTAMP())
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", "current"),
                bigquery.ScalarQueryParameter("last_id", "INT64", last_order_id),
                bigquery.ScalarQueryParameter("batch_num", "INT64", batch_number),
                bigquery.ScalarQueryParameter("records_total", "INT64", records_loaded_total),
            ]
        )
        client.query(query, job_config=job_config).result()
        logger.info(f"✓ Updated woocommerce sync_state: batch={batch_number}, last_order_id={last_order_id}, total={records_loaded_total}")
    except Exception as e:
        logger.error(f"Failed to update woocommerce sync_state: {e}")
        raise


def get_last_sync_time(table_name: str) -> Optional[str]:
    """Get last successful sync timestamp from sync_metadata."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
        SELECT last_sync_time
        FROM `{PROJECT_ID}.{DATASET_ID}.sync_metadata`
        WHERE sync_name = @table_name
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
            ]
        )
        result = client.query(query, job_config=job_config).result()
        rows = list(result)
        if rows:
            return rows[0].last_sync_time.isoformat() if rows[0].last_sync_time else None
        return None
    except Exception as e:
        logger.warning(f"Could not retrieve last sync time for {table_name}: {e}")
        return None


def update_sync_metadata(table_name: str, records_loaded: int) -> None:
    """Update sync_metadata after successful load."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
        MERGE INTO `{PROJECT_ID}.{DATASET_ID}.sync_metadata` T
        USING (SELECT @table_name as sync_name) S
        ON T.sync_name = S.sync_name
        WHEN MATCHED THEN
          UPDATE SET last_sync_time = CURRENT_DATETIME(), records_loaded = @records, updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (sync_name, last_sync_time, records_loaded, updated_at)
          VALUES (@table_name, CURRENT_DATETIME(), @records, CURRENT_TIMESTAMP())
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
                bigquery.ScalarQueryParameter("records", "INT64", records_loaded),
            ]
        )
        client.query(query, job_config=job_config).result()
        logger.info(f"✓ Updated sync_metadata for {table_name}: {records_loaded} records")
    except Exception as e:
        logger.error(f"Failed to update sync_metadata for {table_name}: {e}")


def fetch_woo_data(endpoint: str, per_page: int = 100, after: Optional[str] = None) -> List[Dict]:
    """Fetch paginated data from WooCommerce API."""
    url = f"{WOO_API_BASE}{endpoint}"
    headers = get_woo_headers()
    all_records = []
    page = 1

    while True:
        params = {
            "per_page": per_page,
            "page": page,
        }
        if after:
            params["after"] = after

        try:
            logger.info(f"Fetching {endpoint} page {page}...")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            records = response.json()

            if not records:
                logger.info(f"No more records on page {page}")
                break

            all_records.extend(records)
            logger.info(f"✓ Fetched {len(records)} records (page {page}, total so far: {len(all_records)})")

            # Check for next page
            if len(records) < per_page:
                break

            page += 1

        except Exception as e:
            logger.error(f"Failed to fetch {endpoint} page {page}: {e}")
            break

    logger.info(f"✓ Total {endpoint} records fetched: {len(all_records)}")
    return all_records


def load_orders_to_bigquery(orders: List[Dict]) -> int:
    """Transform and load orders to BigQuery."""
    if not orders:
        return 0

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.orders"

    transformed = []
    for order in orders:
        try:
            transformed.append({
                "order_id": order.get("id"),
                "date_created": order.get("date_created"),
                "total": float(order.get("total", 0)),
                "status": order.get("status"),
                "customer_id": order.get("customer_id"),
                "updated_at": datetime.utcnow().isoformat(),
            })
        except Exception as e:
            logger.warning(f"Failed to transform order {order.get('id')}: {e}")
            continue

    # Create temp table and MERGE
    temp_table = f"{PROJECT_ID}.{DATASET_ID}.temp_orders_{int(datetime.utcnow().timestamp())}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    load_job = client.load_table_from_json(transformed, temp_table, job_config=job_config)
    load_job.result()
    logger.info(f"✓ Loaded {len(transformed)} orders to temp table")

    # MERGE
    merge_query = f"""
    MERGE INTO `{table_id}` T
    USING `{temp_table}` S
    ON T.order_id = S.order_id
    WHEN MATCHED THEN
      UPDATE SET date_created = S.date_created, total = S.total, status = S.status, customer_id = S.customer_id, updated_at = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT (order_id, date_created, total, status, customer_id, updated_at)
      VALUES (order_id, date_created, total, status, customer_id, updated_at)
    """
    merge_job = client.query(merge_query)
    merge_job.result()
    logger.info(f"✓ MERGE completed for orders")

    client.delete_table(temp_table)
    return len(transformed)


def load_order_items_to_bigquery(orders: List[Dict]) -> int:
    """Extract and load order items (line items) to BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.order_items"

    all_items = []
    for order in orders:
        order_id = order.get("id")
        line_items = order.get("line_items", [])

        for item in line_items:
            try:
                all_items.append({
                    "order_item_id": f"{order_id}-{item.get('id')}",  # Composite key
                    "order_id": order_id,
                    "product_id": item.get("product_id"),
                    "product_name": item.get("name"),
                    "quantity": int(item.get("quantity", 0)),
                    "price": float(item.get("price", 0)),
                    "total": float(item.get("total", 0)),
                    "updated_at": datetime.utcnow().isoformat(),
                })
            except Exception as e:
                logger.warning(f"Failed to transform order item {item.get('id')}: {e}")
                continue

    if not all_items:
        return 0

    # Create temp table and MERGE
    temp_table = f"{PROJECT_ID}.{DATASET_ID}.temp_order_items_{int(datetime.utcnow().timestamp())}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    load_job = client.load_table_from_json(all_items, temp_table, job_config=job_config)
    load_job.result()
    logger.info(f"✓ Loaded {len(all_items)} order items to temp table")

    # MERGE
    merge_query = f"""
    MERGE INTO `{table_id}` T
    USING `{temp_table}` S
    ON T.order_item_id = S.order_item_id
    WHEN MATCHED THEN
      UPDATE SET quantity = S.quantity, price = S.price, total = S.total, updated_at = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT (order_item_id, order_id, product_id, product_name, quantity, price, total, updated_at)
      VALUES (order_item_id, order_id, product_id, product_name, quantity, price, total, updated_at)
    """
    merge_job = client.query(merge_query)
    merge_job.result()
    logger.info(f"✓ MERGE completed for order items")

    client.delete_table(temp_table)
    return len(all_items)


def load_products_to_bigquery(products: List[Dict]) -> int:
    """Transform and load products to BigQuery."""
    if not products:
        return 0

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.products"

    transformed = []
    for product in products:
        try:
            # Extract first category if available
            categories = product.get("categories", [])
            category = categories[0].get("name") if categories else None

            transformed.append({
                "product_id": product.get("id"),
                "name": product.get("name"),
                "sku": product.get("sku"),
                "price": float(product.get("price", 0)),
                "category": category,
                "stock_status": product.get("stock_status"),
                "updated_at": datetime.utcnow().isoformat(),
            })
        except Exception as e:
            logger.warning(f"Failed to transform product {product.get('id')}: {e}")
            continue

    # Create temp table and MERGE
    temp_table = f"{PROJECT_ID}.{DATASET_ID}.temp_products_{int(datetime.utcnow().timestamp())}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    load_job = client.load_table_from_json(transformed, temp_table, job_config=job_config)
    load_job.result()
    logger.info(f"✓ Loaded {len(transformed)} products to temp table")

    # MERGE
    merge_query = f"""
    MERGE INTO `{table_id}` T
    USING `{temp_table}` S
    ON T.product_id = S.product_id
    WHEN MATCHED THEN
      UPDATE SET name = S.name, sku = S.sku, price = S.price, category = S.category, stock_status = S.stock_status, updated_at = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT (product_id, name, sku, price, category, stock_status, updated_at)
      VALUES (product_id, name, sku, price, category, stock_status, updated_at)
    """
    merge_job = client.query(merge_query)
    merge_job.result()
    logger.info(f"✓ MERGE completed for products")

    client.delete_table(temp_table)
    return len(transformed)


def load_customers_to_bigquery(customers: List[Dict]) -> int:
    """Transform and load customers to BigQuery."""
    if not customers:
        return 0

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.customers"

    transformed = []
    for customer in customers:
        try:
            transformed.append({
                "customer_id": customer.get("id"),
                "email": customer.get("email"),
                "first_name": customer.get("first_name"),
                "last_name": customer.get("last_name"),
                "total_spent": float(customer.get("total_spent", 0)),
                "updated_at": datetime.utcnow().isoformat(),
            })
        except Exception as e:
            logger.warning(f"Failed to transform customer {customer.get('id')}: {e}")
            continue

    # Create temp table and MERGE
    temp_table = f"{PROJECT_ID}.{DATASET_ID}.temp_customers_{int(datetime.utcnow().timestamp())}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    load_job = client.load_table_from_json(transformed, temp_table, job_config=job_config)
    load_job.result()
    logger.info(f"✓ Loaded {len(transformed)} customers to temp table")

    # MERGE
    merge_query = f"""
    MERGE INTO `{table_id}` T
    USING `{temp_table}` S
    ON T.customer_id = S.customer_id
    WHEN MATCHED THEN
      UPDATE SET email = S.email, first_name = S.first_name, last_name = S.last_name, total_spent = S.total_spent, updated_at = S.updated_at
    WHEN NOT MATCHED THEN
      INSERT (customer_id, email, first_name, last_name, total_spent, updated_at)
      VALUES (customer_id, email, first_name, last_name, total_spent, updated_at)
    """
    merge_job = client.query(merge_query)
    merge_job.result()
    logger.info(f"✓ MERGE completed for customers")

    client.delete_table(temp_table)
    return len(transformed)


@app.route('/', methods=['POST'])
def sync_woocommerce():
    """Main Cloud Run endpoint: Sync WooCommerce data to BigQuery (batch processing)."""
    logger.info("Starting WooCommerce → BigQuery sync (batch mode)...")

    # Check if job is already running
    db = firestore.Client()
    lock_ref = db.collection('sync_locks').document('woocommerce-sync')
    lock = lock_ref.get()

    if lock.exists:
        logger.info("Job already running, skipping this interval")
        return jsonify({"status": "skipped", "message": "Job already running"}), 200

    # Create lock
    lock_ref.set({"started_at": datetime.utcnow().isoformat()})
    logger.info("Lock acquired, starting sync...")

    try:
        # Load sync state for batch processing
        sync_state = load_woocommerce_sync_state()
        current_batch_number = sync_state["batch_number"]
        records_loaded_total = sync_state["records_loaded_total"]
        logger.info(f"Resuming from batch {current_batch_number}, total loaded: {records_loaded_total}")

        # Batch parameters
        batch_size = 100
        start_offset = current_batch_number * batch_size

        # 1. Fetch ONE batch of orders
        logger.info(f"Fetching batch {current_batch_number} (offset={start_offset}, size={batch_size})...")
        all_orders = fetch_woo_data("/orders", per_page=batch_size)
        
        # Paginate manually to get exact batch
        batch_orders = all_orders[start_offset:start_offset + batch_size] if all_orders else []

        if not batch_orders:
            logger.warning(f"No orders for batch {current_batch_number}, initial load complete")
            result = {
                "status": "success",
                "timestamp": datetime.utcnow().isoformat(),
                "batch": current_batch_number,
                "records_loaded": 0,
                "total_records": records_loaded_total,
                "message": "Initial load complete, switching to incremental mode",
            }
            logger.info(f"✓ Sync complete: {json.dumps(result, indent=2)}")
            return jsonify(result), 200

        # 2. Process this batch
        logger.info(f"Processing {len(batch_orders)} orders in batch {current_batch_number}...")
        
        orders_loaded = load_orders_to_bigquery(batch_orders)
        update_sync_metadata("orders", orders_loaded)

        # 3. Extract order items from this batch
        logger.info(f"Extracting order items from batch...")
        items_loaded = load_order_items_to_bigquery(batch_orders)
        update_sync_metadata("order_items", items_loaded)

        # 4. Fetch products incrementally (not batched - small dataset)
        if current_batch_number == 0:  # Only on first run
            logger.info("Fetching all products...")
            products = fetch_woo_data("/products")
            products_loaded = load_products_to_bigquery(products)
            update_sync_metadata("products", products_loaded)
        else:
            products_loaded = 0

        # 5. Fetch customers incrementally (not batched - small dataset)
        if current_batch_number == 0:  # Only on first run
            logger.info("Fetching all customers...")
            customers = fetch_woo_data("/customers")
            customers_loaded = load_customers_to_bigquery(customers)
            update_sync_metadata("customers", customers_loaded)
        else:
            customers_loaded = 0

        # Update batch tracking
        last_order_id = batch_orders[-1].get("id") if batch_orders else None
        next_batch_number = current_batch_number + 1
        new_total = records_loaded_total + orders_loaded
        update_woocommerce_sync_state(last_order_id, next_batch_number, new_total)

        result = {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "batch": current_batch_number,
            "records_loaded_this_batch": orders_loaded,
            "order_items_loaded": items_loaded,
            "products_loaded": products_loaded,
            "customers_loaded": customers_loaded,
            "total_records_loaded": new_total,
            "next_batch": next_batch_number,
            "message": f"Batch {current_batch_number} complete ({orders_loaded} orders processed)",
        }

        logger.info(f"✓ Batch sync complete: {json.dumps(result, indent=2)}")
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"✗ Sync failed: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e),
        }), 500

    finally:
        # Always delete lock when done
        try:
            db.collection('sync_locks').document('woocommerce-sync').delete()
            logger.info("Lock released")
        except:
            pass


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
