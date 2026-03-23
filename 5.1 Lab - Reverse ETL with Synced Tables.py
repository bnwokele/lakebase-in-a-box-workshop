# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 6: Reverse ETL — Serving Lakehouse Data to Applications
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **The Challenge:**
# MAGIC DataCart's marketing team has prepared Spring Sale promotions in the data warehouse —
# MAGIC product discounts, sale badges, and limited-time offers computed by analytics pipelines.
# MAGIC These promotions need to appear on the customer-facing storefront **instantly**, without
# MAGIC requiring application code changes or manual database inserts.
# MAGIC
# MAGIC **The Lakebase Solution: Synced Tables (Reverse ETL)**
# MAGIC Lakebase synced tables create a managed copy of a Unity Catalog Delta table inside
# MAGIC Lakebase Postgres. The data flows automatically from the lakehouse to the application
# MAGIC database, enabling sub-10ms queries from the storefront.
# MAGIC
# MAGIC ## How It Works
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                      DATA LAKEHOUSE                            │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐   │
# MAGIC │  │  Unity Catalog: serverless_stable_339b90_catalog        │   │
# MAGIC │  │  └── ecommerce.promotions (Delta table)                 │   │
# MAGIC │  │       • badge_text, discount_pct, sale_price            │   │
# MAGIC │  │       • Updated by marketing analytics pipelines        │   │
# MAGIC │  └─────────────────────────┬───────────────────────────────┘   │
# MAGIC │                            │ Synced Table (Reverse ETL)        │
# MAGIC │                            ▼                                   │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐   │
# MAGIC │  │  Lakebase: production branch                            │   │
# MAGIC │  │  └── ecommerce.promotions (Postgres, read-only)         │   │
# MAGIC │  │       • Auto-synced from Delta table                    │   │
# MAGIC │  │       • Sub-10ms queries for the storefront             │   │
# MAGIC │  └─────────────────────────┬───────────────────────────────┘   │
# MAGIC │                            │                                   │
# MAGIC └────────────────────────────┼───────────────────────────────────┘
# MAGIC                              ▼
# MAGIC                    ┌──────────────────┐
# MAGIC                    │ DataCart          │
# MAGIC                    │ Storefront       │
# MAGIC                    │ • Sale badges    │
# MAGIC                    │ • Discount prices│
# MAGIC                    │ • Promo banners  │
# MAGIC                    └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - How to create a Delta table in Unity Catalog for application data
# MAGIC - How to set up a **synced table** that flows data from the lakehouse to Lakebase
# MAGIC - How the storefront **auto-detects** the new promotions table and displays sale badges
# MAGIC - How updating the Delta table propagates changes to the live application
# MAGIC
# MAGIC > 📖 **Docs**: [Synced Tables](https://docs.databricks.com/aws/en/oltp/projects/sync-tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Install Dependencies & Configure

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time
import psycopg2

w = WorkspaceClient()

current_user = w.current_user.me()
db_user = current_user.user_name
username_prefix = db_user.split("@")[0].replace(".", "-")
project_name = f"lakebase-branching-workshop-{username_prefix}"

# Unity Catalog configuration
UC_CATALOG = "add-your-catalog"
UC_SCHEMA = "add-your-schema"
UC_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.promotions"

# Lakebase configuration
db_schema = "ecommerce"

print(f"✅ SDK initialized")
print(f"   User:         {db_user}")
print(f"   UC Table:     {UC_TABLE}")
print(f"   Lakebase:     {project_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create the Promotions Delta Table
# MAGIC
# MAGIC The marketing team maintains a `promotions` table in Unity Catalog. This table
# MAGIC contains active promotional offers — sale badges, discount percentages, and sale prices
# MAGIC for products in the Spring Sale.
# MAGIC
# MAGIC We create this as a Delta table so it integrates with the lakehouse ecosystem
# MAGIC (governance, lineage, versioning) while being syncable to Lakebase for low-latency serving.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_TABLE} (
        id INT,
        product_id INT,
        badge_text STRING,
        discount_pct DECIMAL(5,2),
        sale_price DECIMAL(10,2),
        promo_type STRING,
        is_active BOOLEAN,
        start_date TIMESTAMP,
        end_date TIMESTAMP
    )
    USING DELTA
    COMMENT 'Spring Sale promotions - synced to Lakebase for storefront display'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print(f"✅ Created Delta table: {UC_TABLE}")
print(f"   Change Data Feed enabled (required for Triggered/Continuous sync modes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Seed Spring Sale Promotions
# MAGIC
# MAGIC The marketing team has identified 12 products for the Spring Sale with varying
# MAGIC discount levels. These span multiple categories to create a visually rich storefront.

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime, timedelta

now = datetime.now()
end_date = now + timedelta(days=14)  # 2-week sale

promotions = [
    # Electronics deals
    Row(id=1,  product_id=1,  badge_text="SPRING SALE",   discount_pct=20.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=2,  product_id=2,  badge_text="HOT DEAL",      discount_pct=30.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=3,  product_id=6,  badge_text="SPRING SALE",   discount_pct=15.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    # Clothing deals
    Row(id=4,  product_id=11, badge_text="CLEARANCE",     discount_pct=40.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=5,  product_id=14, badge_text="SPRING SALE",   discount_pct=25.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    # Books deals
    Row(id=6,  product_id=21, badge_text="LIMITED TIME",  discount_pct=10.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=7,  product_id=23, badge_text="SPRING SALE",   discount_pct=20.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    # Home deals
    Row(id=8,  product_id=31, badge_text="FLASH SALE",    discount_pct=35.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=9,  product_id=33, badge_text="SPRING SALE",   discount_pct=15.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    # Sports deals
    Row(id=10, product_id=41, badge_text="HOT DEAL",      discount_pct=25.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=11, product_id=45, badge_text="SPRING SALE",   discount_pct=20.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=12, product_id=50, badge_text="CLEARANCE",     discount_pct=50.00, sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
]

df = spark.createDataFrame(promotions)

# Compute sale_price from product prices in Lakebase
# For now, write without sale_price — we'll compute it after sync or in a separate step
df.write.mode("overwrite").saveAsTable(UC_TABLE)

print(f"✅ Seeded {len(promotions)} Spring Sale promotions")
display(spark.table(UC_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Compute Sale Prices
# MAGIC
# MAGIC We need the actual product prices from Lakebase to compute sale prices.
# MAGIC Let's connect and update the Delta table with pre-computed sale prices.

# COMMAND ----------

def connect_to_branch(branch_id, wait_seconds=300):
    from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, Duration as Dur
    branch_full = f"projects/{project_name}/branches/{branch_id}"
    min_cu, max_cu, suspend_timeout_seconds = 0.5, 4.0, 1800
    endpoints = list(w.postgres.list_endpoints(parent=branch_full))
    if not endpoints:
        ep_id = f"ep-{branch_id}"
        print(f"🔄 Creating compute endpoint for branch '{branch_id}'...")
        w.postgres.create_endpoint(
            parent=branch_full,
            endpoint=Endpoint(spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=min_cu,
                autoscaling_limit_max_cu=max_cu,
                suspend_timeout_duration=Dur(seconds=suspend_timeout_seconds)
            )),
            endpoint_id=ep_id
        ).wait()
        endpoints = list(w.postgres.list_endpoints(parent=branch_full))
    ep = endpoints[0]
    if not ep.status or not ep.status.hosts or not ep.status.hosts.host:
        for i in range(wait_seconds // 10):
            time.sleep(10)
            endpoints = list(w.postgres.list_endpoints(parent=branch_full))
            ep = endpoints[0]
            if ep.status and ep.status.hosts and ep.status.hosts.host:
                break
    host = ep.status.hosts.host
    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    conn = psycopg2.connect(host=host, port=5432, dbname="databricks_postgres",
                            user=db_user, password=cred.token, sslmode="require")
    conn.autocommit = True
    print(f"✅ Connected to branch '{branch_id}'")
    return conn, host, ep.name

conn_prod, _, _ = connect_to_branch("production")

# COMMAND ----------

conn_prod, _, _ = connect_to_branch("production")

# COMMAND ----------

# Get product prices from Lakebase
with conn_prod.cursor() as cur:
    cur.execute(f"SELECT id, price FROM {db_schema}.products ORDER BY id")
    product_prices = {row[0]: float(row[1]) for row in cur.fetchall()}

# Update Delta table with computed sale prices
from pyspark.sql.functions import col, round as spark_round, lit

promo_df = spark.table(UC_TABLE)
# Create a mapping DataFrame
price_rows = [Row(product_id=pid, original_price=price) for pid, price in product_prices.items()]
prices_df = spark.createDataFrame(price_rows)

# Join and compute sale_price
updated_df = (
    promo_df.join(prices_df, "product_id", "left")
    .withColumn("sale_price",
        spark_round(col("original_price") * (1 - col("discount_pct") / 100), 2)
    )
    .drop("original_price")
)

updated_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UC_TABLE)

print("✅ Sale prices computed and saved!")
display(spark.sql(f"""
    SELECT id, product_id, badge_text, discount_pct, sale_price, is_active
    FROM {UC_TABLE}
    ORDER BY discount_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create a Branch and Sync the Promotions Table
# MAGIC
# MAGIC Following the branching pattern from earlier labs, we'll first sync the promotions
# MAGIC table to a **dev branch** to verify it works before promoting to production.
# MAGIC
# MAGIC ### Step 4a: Create the `dev-promotions` Branch

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

PROMO_BRANCH = "dev-promotions"

# Get production branch
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
prod_branch = next(b for b in branches if b.status and b.status.default)

# Clean up from previous runs
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{PROMO_BRANCH}").wait()
    print(f"🧹 Cleaned up existing branch '{PROMO_BRANCH}'")
except Exception:
    pass

print(f"🔄 Creating branch '{PROMO_BRANCH}' from production...")
w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch.name,
        ttl=Duration(seconds=172800)
    )),
    branch_id=PROMO_BRANCH
).wait()
print(f"✅ Branch '{PROMO_BRANCH}' created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4b: Create the Synced Table to the Dev Branch
# MAGIC
# MAGIC Now set up the **reverse ETL pipeline** that syncs the Delta table to Lakebase.
# MAGIC
# MAGIC **Follow these steps in the Databricks UI:**
# MAGIC
# MAGIC 1. Navigate to **Catalog** in the left sidebar
# MAGIC 2. Browse to `serverless_stable_339b90_catalog` > `ecommerce` > `promotions`
# MAGIC 3. Click on the `promotions` table
# MAGIC 4. Click **Create** > **Synced table**
# MAGIC 5. In the dialog:
# MAGIC    - **Table name**: input **promotions_synced_dev**
# MAGIC    - **Database type**: Select **Lakebase Serverless (Autoscaling)**
# MAGIC    - **Project**: Select your workshop project (`lakebase-branching-workshop-<username>`)
# MAGIC    - **Branch**: Select **dev-promotions**
# MAGIC    - **Sync mode**: Select **Snapshot** (one-time full copy, simplest for demo)
# MAGIC    - **Primary key**: Verify `id` is selected
# MAGIC 6. Click **Create**
# MAGIC
# MAGIC > **Sync modes explained:**
# MAGIC > - **Snapshot**: One-time full copy. Refresh manually or via SDK. Best when data changes infrequently.
# MAGIC > - **Triggered**: Scheduled or on-demand incremental updates. Requires Change Data Feed on source.
# MAGIC > - **Continuous**: Real-time streaming with seconds of latency. Requires Change Data Feed.
# MAGIC >
# MAGIC > We enabled Change Data Feed in Step 1, so you can switch to Triggered or Continuous later.
# MAGIC
# MAGIC **Wait for the sync to complete before continuing.** You can check status in the Catalog UI
# MAGIC or by querying the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify the Sync on the Dev Branch
# MAGIC
# MAGIC Once the synced table is created, the `promotions` table should appear in the
# MAGIC Lakebase `ecommerce` schema on the `dev-promotions` branch. Let's verify.

# COMMAND ----------

conn_branch, _, _ = connect_to_branch(PROMO_BRANCH)

with conn_branch.cursor() as cur:
    cur.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{db_schema}' AND table_name = 'promotions_synced_dev'
    """)
    exists = cur.fetchone()

    if exists:
        cur.execute(f"""
            SELECT id, product_id, badge_text, discount_pct, sale_price, is_active
            FROM {db_schema}.promotions_synced_dev
            WHERE is_active = true
            ORDER BY discount_pct DESC
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        print(f"✅ Synced table verified on '{PROMO_BRANCH}'! {len(rows)} active promotions:\n")
        for row in rows:
            r = dict(zip(cols, row))
            print(f"   Product {r['product_id']:3d} | {r['badge_text']:14s} | -{r['discount_pct']}% | Sale: ${r['sale_price']}")
    else:
        print("⏳ Promotions table not found yet on the dev branch.")
        print("   Make sure you completed Step 4b (create synced table in the UI).")
        print("   The sync may still be in progress — wait a moment and re-run this cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Promote to Production
# MAGIC
# MAGIC The promotions look good on the dev branch. Now create **another synced table**
# MAGIC targeting the **production** branch so the storefront can see the promotions.
# MAGIC
# MAGIC **Follow the same steps as Step 4b, but select the `production` branch:**
# MAGIC
# MAGIC 1. Navigate to **Catalog** > `serverless_stable_339b90_catalog` > `ecommerce` > `promotions`
# MAGIC 2. Click **Create** > **Synced table**
# MAGIC 3. In the dialog:
# MAGIC    - **Table name**: input **promotions_synced_prod**
# MAGIC    - **Database type**: **Lakebase Serverless (Autoscaling)**
# MAGIC    - **Project**: Your workshop project
# MAGIC    - **Branch**: Select **production** (not dev-promotions)
# MAGIC    - **Sync mode**: **Snapshot**
# MAGIC    - **Primary key**: `id`
# MAGIC 4. Click **Create** and wait for the sync to complete
# MAGIC
# MAGIC > This follows the same branch-first pattern from the earlier labs:
# MAGIC > test on a branch, validate, then promote to production.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Grant SP Access to the Synced Table
# MAGIC
# MAGIC **This is a critical step.** Synced tables are created by the Lakebase sync pipeline —
# MAGIC a different internal role than your user account. This means the `ALTER DEFAULT PRIVILEGES`
# MAGIC grants from Lab 1.2 **do not apply** to synced tables, because those defaults only cover
# MAGIC tables created by your user.
# MAGIC
# MAGIC We need to re-run `GRANT ALL ON ALL TABLES` to include the newly synced `promotions` table.
# MAGIC Without this, the storefront's service principal can't read the promotions and the
# MAGIC "Spring Sale Deals" section won't appear.
# MAGIC
# MAGIC > **Key takeaway:** After every new synced table is created, re-grant table permissions
# MAGIC > to the app's SP. This is a one-time step per synced table.

# COMMAND ----------

# Get the app's SP client ID
APP_NAME = "datacart-storefront"
app_info = w.apps.get(APP_NAME)
SP_CLIENT_ID = app_info.service_principal_client_id
print(f"App SP: {SP_CLIENT_ID}")

# Connect as the project owner to grant permissions
conn_prod, _, _ = connect_to_branch("production")

with conn_prod.cursor() as cur:
    sp_role = f'"{SP_CLIENT_ID}"'

    # Re-grant ALL on ALL tables — this picks up the new synced table
    cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {db_schema} TO {sp_role};")
    print(f"✅ Granted ALL on ALL tables in {db_schema} (includes synced tables)")

    cur.execute(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {db_schema} TO {sp_role};")
    print(f"✅ Granted ALL on ALL sequences in {db_schema}")

print(f"\n🎉 SP can now read the promotions synced table!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify Promotions on Production

# COMMAND ----------

# Find the promotions table (may be named 'promotions' or 'promotions_synced_prod')
with conn_prod.cursor() as cur:
    cur.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{db_schema}' AND table_name LIKE '%promotions%'
        ORDER BY table_name
    """)
    promo_tables = [r[0] for r in cur.fetchall()]
    print(f"📋 Promotions tables found: {promo_tables}")

    if promo_tables:
        promo_table = promo_tables[0]
        cur.execute(f"""
            SELECT id, product_id, badge_text, discount_pct, sale_price, is_active
            FROM {db_schema}.{promo_table}
            WHERE is_active = true
            ORDER BY discount_pct DESC
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        print(f"\n✅ Promotions live on production! {len(rows)} active promotions in '{promo_table}':\n")
        for row in rows:
            r = dict(zip(cols, row))
            print(f"   Product {r['product_id']:3d} | {r['badge_text']:14s} | -{r['discount_pct']}% | Sale: ${r['sale_price']}")
    else:
        print("⏳ No promotions table found on production yet.")
        print("   Make sure you completed Step 6 (sync to production branch).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storefront Checkpoint: Spring Sale Goes Live!
# MAGIC
# MAGIC Open the **DataCart Storefront** and observe the promotions appearing:
# MAGIC
# MAGIC 1. **Homepage** — A new "Spring Sale Deals" section shows promoted products with discount badges
# MAGIC 2. **Product cards** — Sale badges (e.g., "SPRING SALE -20%") appear on promoted products
# MAGIC 3. **Product cards** — Original prices are crossed out with sale prices shown
# MAGIC 4. **Product detail** — Promotion alert with discount details
# MAGIC 5. **Cart** — Promoted items show the discounted sale price
# MAGIC
# MAGIC > The storefront auto-detected the new `promotions` table within 30 seconds.
# MAGIC > No app redeployment was needed — the reverse ETL pipeline handled everything.
# MAGIC
# MAGIC > **Key insight:** The marketing team updated a Delta table in the lakehouse.
# MAGIC > The synced table pipeline pushed the data to Lakebase. The storefront detected
# MAGIC > the new table and rendered promotions. **Zero application code changes required.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Update Promotions (Simulate Marketing Campaign Change)
# MAGIC
# MAGIC The marketing team decides to add a **flash sale** on more products and increase
# MAGIC the discount on an existing promotion. Let's update the Delta table and trigger a re-sync.

# COMMAND ----------

from pyspark.sql.functions import when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType, TimestampType
from datetime import datetime, timedelta
from decimal import Decimal

now = datetime.now()
end_date = now + timedelta(days=14) # 2-week sale

# Add new flash sale promotions
new_promos = [
    Row(id=13, product_id=3, badge_text="FLASH SALE", discount_pct=Decimal("45.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=14, product_id=7, badge_text="FLASH SALE", discount_pct=Decimal("35.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=15, product_id=35, badge_text="MEGA DEAL", discount_pct=Decimal("60.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
]

schema = StructType([
    StructField("id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("badge_text", StringType()),
    StructField("discount_pct", DecimalType(5, 2)),
    StructField("sale_price", DecimalType(10, 2)),
    StructField("promo_type", StringType()),
    StructField("is_active", BooleanType()),
    StructField("start_date", TimestampType()),
    StructField("end_date", TimestampType()),
])

new_df = spark.createDataFrame(new_promos, schema=schema)

# Compute sale prices for new promos
new_with_prices = (
    new_df.join(
        spark.createDataFrame([Row(product_id=pid, original_price=price) for pid, price in product_prices.items()]),
        "product_id", "left"
    )
    .withColumn("sale_price", spark_round(col("original_price") * (1 - col("discount_pct") / 100), 2))
    .drop("original_price")
)

# Append new promos to the existing table
new_with_prices.write.mode("append").saveAsTable(UC_TABLE)

# Also update an existing promo - increase Product 1 discount from 20% to 35%
spark.sql(f"""
    UPDATE {UC_TABLE}
    SET discount_pct = 35.00,
        badge_text = 'MEGA DEAL',
        sale_price = ROUND(sale_price / (1 - 0.20) * (1 - 0.35), 2)
    WHERE id = 1
""")

print("\u2705 Marketing team updated promotions:")
print("   \u2022 Added 3 new flash sale products")
print("   \u2022 Increased Product 1 discount from 20% to 35%")
display(spark.table(UC_TABLE).orderBy("discount_pct", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Trigger Re-Sync
# MAGIC
# MAGIC For **Snapshot** mode, we need to manually trigger a refresh. For **Triggered** or
# MAGIC **Continuous** modes, this would happen automatically.

# COMMAND ----------

SYNCED_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.promotions_synced_prod"

# COMMAND ----------

# Trigger the sync pipeline to pick up the changes
try:
    table_info = w.database.get_synced_database_table(name=SYNCED_TABLE)
    pipeline_id = table_info.data_synchronization_status.pipeline_id
    print(f"🔄 Triggering sync pipeline: {pipeline_id}")
    w.pipelines.start_update(pipeline_id=pipeline_id)
    print("✅ Sync triggered! Waiting for completion...")

    # Wait for the sync to complete
    for i in range(30):
        time.sleep(10)
        table_info = w.database.get_synced_database_table(name=UC_TABLE)
        status = table_info.data_synchronization_status
        if status and status.last_sync_time:
            print(f"\n✅ Sync completed!")
            break
        print(f"   Still syncing... ({(i+1)*10}s)")
except Exception as e:
    print(f"⚠️ Could not trigger sync automatically: {e}")
    print("   You can trigger a refresh manually in the Catalog UI:")
    print(f"   Navigate to {UC_TABLE} → Synced table tab → Refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Verify Updated Promotions in Lakebase

# COMMAND ----------

conn_prod, _, _ = connect_to_branch("production")

with conn_prod.cursor() as cur:
    cur.execute(f"""
        SELECT id, product_id, badge_text, discount_pct, sale_price, is_active
        FROM {db_schema}.promotions_synced_prod
        WHERE is_active = true
        ORDER BY discount_pct DESC
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

print(f"✅ Updated promotions synced to Lakebase! {len(rows)} active promotions:\n")
for row in rows:
    r = dict(zip(cols, row))
    print(f"   Product {r['product_id']:3d} | {r['badge_text']:14s} | -{r['discount_pct']}% | Sale: ${r['sale_price']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storefront Checkpoint: Updated Promotions
# MAGIC
# MAGIC Refresh the **DataCart Storefront** and observe:
# MAGIC
# MAGIC - **3 new products** now show flash sale badges
# MAGIC - **Product 1** now shows "MEGA DEAL -35%" instead of "SPRING SALE -20%"
# MAGIC - The storefront updated **without any code changes or redeployment**
# MAGIC
# MAGIC > This is the power of reverse ETL: your analytics team modifies a Delta table,
# MAGIC > the sync pipeline pushes it to Lakebase, and the application reflects the change
# MAGIC > automatically. The storefront code never changed — it just queries the same
# MAGIC > database and renders whatever promotions are active.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Cleanup (Optional)
# MAGIC
# MAGIC > ⚠️ Uncomment to remove the synced table and Delta table.

# COMMAND ----------

# Uncomment to clean up:
# spark.sql(f"DROP TABLE IF EXISTS {UC_TABLE}")
# print(f"🗑️ Dropped Delta table: {UC_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |------|---------------|
# MAGIC | **Create Delta table** | `promotions` table in Unity Catalog with Spring Sale data |
# MAGIC | **Create synced table** | Reverse ETL pipeline syncs Delta → Lakebase Postgres |
# MAGIC | **Verify sync** | Queried `ecommerce.promotions` directly in Lakebase |
# MAGIC | **Storefront impact** | Sale badges, discount prices, promo banners appeared automatically |
# MAGIC | **Update & re-sync** | Added new promotions, triggered refresh, storefront updated |
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC 1. **Synced tables bridge the lakehouse and application layers** — analytics data serves live applications
# MAGIC 2. **No application code changes needed** — the storefront's schema detector picks up new tables automatically
# MAGIC 3. **Change Data Feed** enables incremental sync for near real-time updates
# MAGIC 4. **Unity Catalog governance** applies — access control, lineage, and auditing on the source data
# MAGIC 5. **Sub-10ms query latency** — Lakebase serves the synced data with OLTP-grade performance
