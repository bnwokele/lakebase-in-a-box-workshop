# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Notebook 00: Setup Lakebase Project & Seed Data
# MAGIC
# MAGIC This notebook creates a **Lakebase Autoscaling** project and seeds it with Datacarts
# MAGIC e-commerce database.
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 1. Creates a new Lakebase project with autoscaling compute
# MAGIC 2. Waits for the project to become active
# MAGIC 3. Connects via OAuth token authentication (fully automated)
# MAGIC 4. Seeds 4 tables with realistic e-commerce data
# MAGIC 5. Verifies everything is ready
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - **Cluster**: Any Databricks cluster with Python 3.10+
# MAGIC - **Region**: Workspace must be in a supported region:
# MAGIC   `us-east-1`, `us-east-2`, `eu-central-1`, `eu-west-1`, `eu-west-2`,
# MAGIC   `ap-south-1`, `ap-southeast-1`, `ap-southeast-2`
# MAGIC
# MAGIC ## Architecture After Setup
# MAGIC ```
# MAGIC Lakebase Project: lakebase-branching-<username>
# MAGIC └── production (default branch)
# MAGIC     └── ecommerce (schema)
# MAGIC         ├── customers   (100 rows)
# MAGIC         ├── products    (50 rows)
# MAGIC         └── orders      (200 rows)
# MAGIC ```
# MAGIC
# MAGIC > 📖 **Docs**: [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) | [API Reference](https://docs.databricks.com/api/workspace/postgres)

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize SDK & Configuration
# MAGIC
# MAGIC The `WorkspaceClient` auto-authenticates when running inside a Databricks notebook —
# MAGIC no tokens or secrets needed.
# MAGIC
# MAGIC The project name is derived from your Databricks username to keep it unique per user.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Derive project name from the current user's identity
current_user = w.current_user.me()
db_user = current_user.user_name
username_prefix = db_user.split("@")[0].replace(".", "-")
project_name = f"lakebase-branching-workshop-{username_prefix}"

# Fixed configuration
db_schema = "ecommerce"
min_cu = 0.5
max_cu = 4.0
suspend_timeout_seconds = 1800

print(f"✅ SDK initialized")
print(f"   Workspace: {w.config.host}")
print(f"   User:      {db_user}")
print(f"")
print("📋 Configuration:")
print(f"   Project Name:      {project_name}")
print(f"   DB Schema:         {db_schema}")
print(f"   Min CU:            {min_cu}")
print(f"   Max CU:            {max_cu}")
print(f"   Suspend Timeout:   {suspend_timeout_seconds}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Lakebase Project
# MAGIC
# MAGIC We'll create a new Lakebase project with autoscaling compute.
# MAGIC
# MAGIC **What happens under the hood:**
# MAGIC - A new PostgreSQL 17 instance is provisioned
# MAGIC - A default `production` branch is created automatically
# MAGIC - A compute endpoint is attached to the `production` branch
# MAGIC - Autoscaling is configured (0.5 – 4.0 CU)
# MAGIC - The compute auto-suspends after 60s of idle time
# MAGIC
# MAGIC > ⏱️ It may take a few moments for your compute to activate.

# COMMAND ----------

from databricks.sdk.service.postgres import (
    Project, ProjectSpec, ProjectDefaultEndpointSettings, Duration
)

# Check if the project already exists
existing_projects = list(w.postgres.list_projects())
project_exists = any(
    p.name == f"projects/{project_name}" for p in existing_projects
)

if project_exists:
    print(f"ℹ️  Project '{project_name}' already exists — skipping creation.")
    print(f"   If you want a fresh start, run 99_Cleanup first.")
else:
    print(f"🔄 Creating project '{project_name}'...")
    print(f"   PostgreSQL version: 17")
    print(f"   Compute: {min_cu} – {max_cu} CU, auto-suspend after {suspend_timeout_seconds}s")
    
    result = w.postgres.create_project(
        project=Project(spec=ProjectSpec(
            display_name=project_name,
            pg_version=17,
            default_endpoint_settings=ProjectDefaultEndpointSettings(
                autoscaling_limit_min_cu=min_cu,
                autoscaling_limit_max_cu=max_cu,
                suspend_timeout_duration=Duration(seconds=suspend_timeout_seconds)
            )
        )),
        project_id=project_name
    ).wait()
    
    print(f"\n✅ Project '{project_name}' created successfully!")

# Get project UID and display the Lakebase UI link
project_obj = next(
    p for p in w.postgres.list_projects()
    if p.name == f"projects/{project_name}"
)
project_uid = project_obj.uid
workspace_host = w.config.host.rstrip("/")
lakebase_url = f"{workspace_host}/lakebase/projects/{project_uid}"

print(f"\n🔗 Lakebase UI: {lakebase_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2b: Verify Project & Get Main Branch
# MAGIC
# MAGIC Every Lakebase project comes with a default `production` branch. Let's confirm it exists
# MAGIC and get its compute endpoint (we'll need the host to connect via `psycopg2`).

# COMMAND ----------

import time

# List branches — the default 'production' branch should exist
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))

print(f"📋 Branches in '{project_name}':")
for b in branches:
    branch_id = b.name.split("/branches/")[-1]
    is_default = "⭐ default" if b.status and b.status.default else ""
    print(f"   • {branch_id} {is_default}")

# Get the production branch (the default one, or fallback to the first)
prod_branch = next(
    (b for b in branches if b.status and b.status.default),
    branches[0]
)
prod_branch_name = prod_branch.name
print(f"\n✅ Production branch: {prod_branch_name}")

# COMMAND ----------

# Get the compute endpoint for the production branch
endpoints = list(w.postgres.list_endpoints(parent=prod_branch_name))

if not endpoints:
    print("⏳ Compute endpoint not ready yet. Waiting...")
    for i in range(30):
        time.sleep(10)
        endpoints = list(w.postgres.list_endpoints(parent=prod_branch_name))
        if endpoints:
            break
        print(f"   Still waiting... ({(i+1)*10}s)")

if endpoints:
    prod_endpoint = endpoints[0]
    prod_endpoint_name = prod_endpoint.name
    prod_host = prod_endpoint.status.hosts.host
    print(f"✅ Compute endpoint ready!")
    print(f"   Endpoint: {prod_endpoint_name}")
    print(f"   Host: {prod_host}")
    print(f"   Port: 5432")
    print(f"   Database: databricks_postgres")
else:
    raise Exception("Compute endpoint not available after 5 minutes. Check the Lakebase UI for project status.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Connect to the Database
# MAGIC
# MAGIC Lakebase supports **OAuth token-based authentication** — your Databricks identity is used
# MAGIC to generate short-lived database tokens. No passwords to manage!
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. When you create a project, a Postgres role for your Databricks identity is **automatically created**
# MAGIC 2. This role owns the default `databricks_postgres` database and is a member of `databricks_superuser`
# MAGIC 3. The SDK generates an OAuth token using `generate_database_credential`
# MAGIC 4. We connect via `psycopg2` using the token as the password
# MAGIC
# MAGIC > 💡 **Token lifetime**: Tokens auto-expire, so they're generated fresh each time.
# MAGIC > This is more secure than static passwords and fully automated.
# MAGIC
# MAGIC > 📖 **Docs**: [Query with Python in notebooks](https://docs.databricks.com/aws/en/oltp/projects/notebooks-python)

# COMMAND ----------

import psycopg2

# Generate a fresh OAuth token
cred = w.postgres.generate_database_credential(endpoint=prod_endpoint_name)
db_token = cred.token
print(f"🔑 OAuth token generated (expires: {cred.expire_time})")

# Connect to the database
try:
    conn = psycopg2.connect(
        host=prod_host,
        port=5432,
        dbname="databricks_postgres",
        user=db_user,
        password=db_token,
        sslmode="require"
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]

    print(f"✅ Connected to Lakebase!")
    print(f"   PostgreSQL: {version[:60]}...")
    print(f"   Host: {prod_host}")
    print(f"   User: {db_user}")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print(f"\n   Troubleshooting:")
    print(f"   1. Is the endpoint active? Check the Lakebase UI.")
    print(f"   2. Does your user have permissions on this project?")
    print(f"   3. Check the Lakebase UI → Roles tab to verify your role exists.")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Seed the E-Commerce Schema
# MAGIC
# MAGIC We'll create 3 tables that model a simple e-commerce application:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐     ┌──────────────┐
# MAGIC │  customers   │     │   products   │
# MAGIC │──────────────│     │──────────────│
# MAGIC │ id (PK)      │     │ id (PK)      │
# MAGIC │ name         │     │ name         │
# MAGIC │ email        │     │ price        │
# MAGIC │ created_at   │     │ category     │
# MAGIC └──────────────┘     └──────────────┘
# MAGIC                            
# MAGIC             ┌──────────────┐ 
# MAGIC             │   orders     │
# MAGIC             │──────────────│
# MAGIC             │ id (PK)      │
# MAGIC             │ customer_id  │───→ customers.id
# MAGIC             │ quantity        │
# MAGIC             │ total    │
# MAGIC             │ currency     │
# MAGIC             │ order_date    │
# MAGIC             │ status       │
# MAGIC             │ created_at   │
# MAGIC             └──────────────┘
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC > 💡 This schema is intentionally simple — the scenarios will evolve it
# MAGIC > (adding columns, backfilling data) to demonstrate branching workflows.

# COMMAND ----------

# --- Schema SQL (embedded for portability) ---

SEED_SCHEMA_SQL = f"""
-- Create schema (avoids permission issues on 'public')
CREATE SCHEMA IF NOT EXISTS {db_schema};

-- Set search path so all subsequent commands use this schema
SET search_path TO {db_schema};

-- Drop tables if they exist (idempotent)
DROP TABLE IF EXISTS {db_schema}.order_items CASCADE;
DROP TABLE IF EXISTS {db_schema}.orders CASCADE;
DROP TABLE IF EXISTS {db_schema}.products CASCADE;
DROP TABLE IF EXISTS {db_schema}.customers CASCADE;

-- Customers
CREATE TABLE {db_schema}.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Products
CREATE TABLE {db_schema}.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50)
);

-- Orders
CREATE TABLE IF NOT EXISTS {db_schema}.orders (
    id          SERIAL PRIMARY KEY,
    customer_id     INT             NOT NULL REFERENCES {db_schema}.customers,
    product_id  INT             NOT NULL REFERENCES {db_schema}.products,
    quantity    INT             NOT NULL DEFAULT 1,
    total NUMERIC(10, 2)  NOT NULL,
    currency    VARCHAR(3)      NOT NULL DEFAULT 'USD',
    order_date  TIMESTAMP       NOT NULL DEFAULT NOW(),
    status      VARCHAR(20)     NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled'))
);
"""

with conn.cursor() as cur:
    cur.execute(SEED_SCHEMA_SQL)

print(f"✅ Schema '{db_schema}' created with tables:")
print(f"   • {db_schema}.customers")
print(f"   • {db_schema}.products")
print(f"   • {db_schema}.orders")
print(f"   • {db_schema}.order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed Sample Data
# MAGIC
# MAGIC We'll insert realistic e-commerce data:
# MAGIC - **100 customers** with unique names and emails
# MAGIC - **50 products** across 5 categories (Electronics, Clothing, Books, Home, Sports)
# MAGIC - **200 orders** with varying statuses (pending, confirmed, shipped, delivered)
# MAGIC - **~500 order items** linking orders to products
# MAGIC
# MAGIC > 💡 This data will be used across all scenarios. Scenario 2 will add a
# MAGIC > `loyalty_tier` column and backfill it based on order history.

# COMMAND ----------

import random

random.seed(42)  # Reproducible data

with conn.cursor() as cur:

    # --- Customers (100) ---
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", 
                   "Henry", "Iris", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia",
                   "Paul", "Quinn", "Ruby", "Sam", "Tara", "Uma", "Victor", "Wendy",
                   "Xander", "Yara", "Zach", "Amber", "Blake", "Cora", "Derek",
                   "Elena", "Felix", "Gina", "Hugo", "Isla", "Jake", "Kira", "Liam",
                   "Maya", "Nate", "Opal", "Pete", "Rosa", "Sean", "Tina", "Uri",
                   "Vera", "Wade", "Xena", "Yuri"]
    
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                  "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
                  "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
                  "Jackson", "Martin"]
    
    customers = []
    for i in range(100):
        first = first_names[i % len(first_names)]
        last = last_names[i % len(last_names)]
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}.{i}@example.com"
        customers.append((name, email))
    
    cur.executemany(
        f"INSERT INTO {db_schema}.customers (name, email) VALUES (%s, %s)",
        customers
    )
    print(f"✅ Inserted {len(customers)} customers")

    # --- Products (50) ---
    categories = {
        "Electronics": ["Laptop", "Headphones", "Phone Case", "USB Cable", "Webcam",
                        "Keyboard", "Mouse", "Monitor", "Tablet", "Speaker"],
        "Clothing": ["T-Shirt", "Jeans", "Sneakers", "Jacket", "Hat",
                     "Scarf", "Socks", "Belt", "Hoodie", "Shorts"],
        "Books": ["Python Guide", "SQL Mastery", "Data Engineering", "ML Handbook", "Cloud Atlas",
                  "Clean Code", "System Design", "Algorithms", "DevOps Handbook", "AI Ethics"],
        "Home": ["Desk Lamp", "Coffee Mug", "Plant Pot", "Cushion", "Candle",
                 "Picture Frame", "Clock", "Vase", "Blanket", "Coaster"],
        "Sports": ["Yoga Mat", "Water Bottle", "Resistance Band", "Jump Rope", "Dumbbell",
                   "Tennis Ball", "Running Socks", "Gym Bag", "Towel", "Foam Roller"]
    }
    
    products = []
    for category, items in categories.items():
        for item in items:
            price = round(random.uniform(5.99, 299.99), 2)
            products.append((item, price, category))
    
    cur.executemany(
        f"INSERT INTO {db_schema}.products (name, price, category) VALUES (%s, %s, %s)",
        products
    )
    print(f"✅ Inserted {len(products)} products")

    # --- Orders (200) ---
    cur.execute(f"""
    INSERT INTO {db_schema}.orders (customer_id, product_id, quantity, total, currency, order_date, status) VALUES
        (1,  1, 1, 1299.99, 'USD', '2024-03-01 10:05:00', 'delivered'),
        (1,  2, 1,   89.99, 'USD', '2024-03-05 14:22:00', 'delivered'),
        (2,  4, 1,  129.99, 'USD', '2024-03-08 09:00:00', 'shipped'),
        (3,  3, 1,  449.99, 'EUR', '2024-03-10 11:30:00', 'confirmed'),
        (4,  5, 2,  119.98, 'EUR', '2024-03-12 16:45:00', 'delivered'),
        (5,  2, 1,   89.99, 'GBP', '2024-03-15 08:10:00', 'shipped'),
        (6,  6, 3,  119.97, 'AED', '2024-03-16 12:00:00', 'pending'),
        (7,  1, 1, 1299.99, 'JPY', '2024-03-18 07:30:00', 'confirmed'),
        (8, 13, 2,  109.98, 'EUR', '2024-03-19 15:15:00', 'delivered'),
        (9, 10, 1,   99.99, 'EUR', '2024-03-20 10:00:00', 'shipped'),
        (10, 7, 1,   24.99, 'INR', '2024-03-21 13:30:00', 'delivered'),
        (11, 8, 1,   49.99, 'BRL', '2024-03-22 09:45:00', 'confirmed'),
        (12, 9, 2,   69.98, 'CNY', '2024-03-23 18:20:00', 'pending'),
        (1, 11, 1,   29.99, 'USD', '2024-03-24 11:05:00', 'shipped'),
        (2, 12, 2,   39.98, 'USD', '2024-03-25 14:00:00', 'delivered'),
        (3, 15, 1,   29.99, 'EUR', '2024-03-26 16:30:00', 'pending'),
        (4, 14, 1,   69.99, 'EUR', '2024-03-27 08:00:00', 'confirmed'),
        (5,  4, 1,  129.99, 'GBP', '2024-03-28 12:45:00', 'shipped'),
        (6,  3, 1,  449.99, 'AED', '2024-03-29 10:10:00', 'confirmed'),
        (7,  5, 1,   59.99, 'JPY', '2024-03-30 07:50:00', 'pending'),
        (8,  1, 1, 1299.99, 'EUR', '2024-03-31 15:00:00', 'confirmed'),
        (9,  2, 2,  179.98, 'EUR', '2024-04-01 09:30:00', 'shipped')
    ON CONFLICT DO NOTHING;
    """
    )

    cur.execute(f"""
    SELECT o.id, u.name AS customer, p.name AS product, o.quantity,
           o.total, o.currency, o.status
    FROM orders o
    JOIN customers    u ON u.id = o.customer_id
    JOIN products p ON p.id = o.product_id
    ORDER BY o.id;
    """)
    cols, rows = [d[0] for d in cur.description], cur.fetchall()
    
print(f"✅ Inserted {len(rows)} orders") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Setup
# MAGIC
# MAGIC Let's confirm everything is in place — tables exist, data is populated,
# MAGIC and the project is ready for the scenario notebooks.

# COMMAND ----------

print("=" * 60)
print(f"  PROJECT SUMMARY: {project_name}")
print("=" * 60)

with conn.cursor() as cur:
    # Table row counts
    tables = ["customers", "products", "orders"]
    print(f"\n📊 Tables (schema: {db_schema}):")
    for table in tables:
        cur.execute(f"SELECT count(*) FROM {db_schema}.{table}")
        count = cur.fetchone()[0]
        print(f"   • {db_schema}.{table:20s} {count:>6} rows")

    # Sample data preview
    print("\n👤 Sample Customers (first 5):")
    cur.execute(f"SELECT id, name, email FROM {db_schema}.customers ORDER BY id LIMIT 5")
    for row in cur.fetchall():
        print(f"   {row[0]:3d} | {row[1]:20s} | {row[2]}")

    # Order stats
    print("\n📦 Order Status Distribution:")
    cur.execute(f"""
        SELECT status, count(*) as cnt, ROUND(AVG(total), 2) as avg_total
        FROM {db_schema}.orders GROUP BY status ORDER BY status
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:12s} {row[1]:4d} orders  (avg ${row[2]})")

    # Top categories
    print("\n🏷️  Product Categories:")
    cur.execute(f"""
        SELECT category, count(*) as cnt, 
               ROUND(MIN(price), 2) as min_price,
               ROUND(MAX(price), 2) as max_price
        FROM {db_schema}.products GROUP BY category ORDER BY category
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:15s} {row[1]:3d} products  (${row[2]} – ${row[3]})")

print("\n" + "=" * 60)
print(f"  ✅ Project '{project_name}' is READY!")
print("=" * 60)
conn.close()   
