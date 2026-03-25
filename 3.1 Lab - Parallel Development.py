# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 1: Parallel Development with Branching
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **The Challenge:**  
# MAGIC DataCart has three developers that need to make **schema changes simultaneously** to support the new features for the Spring Sale. 
# MAGIC
# MAGIC | Developer | Team | Task |
# MAGIC |-----------|------|------|
# MAGIC | Developer A | Loyalty Team | Add `loyalty_points` column, new `loyalty_members` table and `review` table|
# MAGIC | Developer B | Global Team | Add `exchange_rates` table + convert `currency` to a FK |
# MAGIC | Developer C | Performance Team | Add indexes to `products` for Spring Sale traffic surge |
# MAGIC
# MAGIC Traditional database workflows often create bottlenecks that slow down development and introduce avoidable risks. Here are some ways using traditional databases holds teams back during development -
# MAGIC - Schema changes can create friction during development (Developer A's DDL changes (full_name -> name) can break Developer B's code (dependent on full_name), because they are working on the **same copy** of the database
# MAGIC - Creating isolated environments can be expensive. Spinning up a replica of production — same data, same volume, same indexes — means paying for a second full-size instance, waiting >15 minutes for a snapshot restore, **and doing it again every time prod moves forward**. It just isn’t feasible!
# MAGIC - Testing against small, synthetic datasets often fails to catch edge cases that only exist in real-world data **leading to high-severity incidents in production**
# MAGIC
# MAGIC **The Lakebase Solution: Branching**  
# MAGIC Each developer creates an isolated **branch** — a zero-copy snapshot of production. They work independently, test their changes, and then perform migrations on the production branch after it has been validated on production like / scale data. The production branch is never touched during development.
# MAGIC
# MAGIC What Lakebase Offers -
# MAGIC - Quick branch creation (in seconds)
# MAGIC - Ephemeral branches that can expire after use (cutting compute cost)
# MAGIC - Ability to reset branch to parent state to keep prod and non-prod (dev, staging) branches in sync
# MAGIC - No data duplication (copy on write)
# MAGIC
# MAGIC
# MAGIC **[Technical Blog](https://community.databricks.com/t5/technical-blog/lakebase-branching-meets-docker-the-migration-safety-net-i-wish/ba-p/149945) to learn more about the great benefits of Lakebase Branching from an ex-backend engineer**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0: Install Dependencies & Configure Helpers

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

# Derive project name from the current user's identity
current_user = w.current_user.me()
db_user = current_user.user_name
username_prefix = db_user.split("@")[0].replace(".", "-")
project_name = f"lakebase-branching-workshop-{username_prefix}"

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

# MAGIC %md
# MAGIC ## Helper: Connect to Any Branch
# MAGIC
# MAGIC This function is used by all scenario notebooks (01–05) to connect to a specific branch.
# MAGIC It handles endpoint discovery, waiting, and OAuth token generation.

# COMMAND ----------

def connect_to_branch(branch_id, wait_seconds=300):
    """
    Connect to a Lakebase branch endpoint.
    Automatically creates a compute endpoint if none exists.
    
    Args:
        branch_id: Branch name (e.g. "dev-readonly", "feature-loyalty-tier")
        wait_seconds: Max seconds to wait for endpoint to become ready (default 300)
    
    Returns:
        tuple: (connection, host, endpoint_name)
    """
    from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, Duration as Dur

    branch_full = f"projects/{project_name}/branches/{branch_id}"
    
    # Check if an endpoint already exists
    endpoints = list(w.postgres.list_endpoints(parent=branch_full))
    
    if not endpoints:
        # Create a compute endpoint for this branch
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
        print(f"   ✅ Compute endpoint created!")
        endpoints = list(w.postgres.list_endpoints(parent=branch_full))
    
    # Wait for the endpoint host to be available
    ep = endpoints[0]
    if not ep.status or not ep.status.hosts or not ep.status.hosts.host:
        print(f"⏳ Waiting for endpoint to become ready...")
        for i in range(wait_seconds // 10):
            time.sleep(10)
            endpoints = list(w.postgres.list_endpoints(parent=branch_full))
            ep = endpoints[0]
            if ep.status and ep.status.hosts and ep.status.hosts.host:
                break
            print(f"   Still waiting... ({(i+1)*10}s)")
    
    if not ep.status or not ep.status.hosts or not ep.status.hosts.host:
        raise Exception(f"Endpoint not ready for branch '{branch_id}' after {wait_seconds}s")
    
    host = ep.status.hosts.host
    
    # Generate OAuth token and connect
    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    branch_conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname="databricks_postgres",
        user=db_user,
        password=cred.token,
        sslmode="require"
    )
    branch_conn.autocommit = True
    
    print(f"✅ Connected to branch '{branch_id}'")
    print(f"   Host: {host}")
    return branch_conn, host, ep.name

def delete_branch_safe(branch_id, max_retries=6, wait_between=30):
    """
    Delete a branch, retrying if the endpoint is still reconciling.
    
    Args:
        branch_id: Branch name (e.g. "dev-readonly")
        max_retries: Max number of retry attempts (default 6)
        wait_between: Seconds to wait between retries (default 30)
    """
    branch_full = f"projects/{project_name}/branches/{branch_id}"
    
    for attempt in range(max_retries):
        try:
            w.postgres.delete_branch(name=branch_full).wait()
            print(f"🗑️ Branch '{branch_id}' deleted.")
            return
        except Exception as e:
            if "reconciliation" in str(e).lower() and attempt < max_retries - 1:
                print(f"   ⏳ Endpoint still reconciling, retrying in {wait_between}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_between)
            else:
                raise

def print_table(cols, rows, max_rows=30):
    if not cols:
        print("(no results)")
        return
    widths = [max(len(str(c)), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(cols)]
    sep    = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    print(sep)
    print("|" + "|".join(f" {c:<{widths[i]}} " for i, c in enumerate(cols)) + "|")
    print(sep)
    for row in rows[:max_rows]:
        print("|" + "|".join(f" {str(v):<{widths[i]}} " for i, v in enumerate(row)) + "|")
    print(sep)

print("🔧 print_table, connect_to_branch() and delete_branch_safe() helpers defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 👤 Developer A — Loyalty Team
# MAGIC
# MAGIC **Goal:** Add a `loyalty_points` column to the `users` table and create a new `loyalty_members` table to support DataCart's Spring Sale loyalty program.
# MAGIC
# MAGIC ### Task A-1: Create Branch `dev-loyalty-reviews`
# MAGIC
# MAGIC Developer A creates an isolated branch from `production`. This is a **zero-copy snapshot** — no data is duplicated on disk. The branch diverges only as changes are made.

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

BRANCH_NAME = "dev-loyalty-reviews"

# Fixed configuration
db_schema = "ecommerce"
min_cu = 0.5
max_cu = 4.0
suspend_timeout_seconds = 1800

# Clean up from previous runs
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{BRANCH_NAME}").wait()
    print(f"🧹 Cleaned up existing branch '{BRANCH_NAME}'")
except Exception:
    pass

# Create your feature branch
print(f"\n🔄 Creating branch '{BRANCH_NAME}' from production...")
w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        ttl=Duration(seconds=172800)  # 48-hour TTL
    )),
    branch_id=BRANCH_NAME
).wait()
print(f"✅ Branch '{BRANCH_NAME}' created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task A-2: Add `loyalty_points` Column to `Customers`
# MAGIC
# MAGIC Developer A runs their DDL migration on the isolated `dev-loyalty-reviews` branch. This change is **invisible to production** and to the other developers' branches.

# COMMAND ----------

# connect to dev-loyalty-reviews branch
conn_loyalty, _, _ = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

print("🔧 Developer A: Adding loyalty features to 'dev-loyalty-reviews' branch...\n")

# Add loyalty_points column to users
with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    ALTER TABLE {db_schema}.customers
    ADD COLUMN IF NOT EXISTS loyalty_points INT NOT NULL DEFAULT 0;
""")

# Backfill some loyalty points based on order history
with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    UPDATE {db_schema}.customers u
    SET loyalty_points = (
        SELECT COALESCE(SUM(FLOOR(o.total)::INT), 0)
        FROM {db_schema}.orders o WHERE o.customer_id = u.id
    );
""") 

print("✅ Added 'loyalty_points' column and backfilled from order history.")

# Show updated users
with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    SELECT id, name, loyalty_points
    FROM {db_schema}.customers
    ORDER BY loyalty_points DESC
    LIMIT 10;
""")
    cols, rows = [d[0] for d in cur.description], cur.fetchall()
print("\n🏆 Users with loyalty points (dev-loyalty-reviews branch):")
print_table(cols, rows)
conn_loyalty.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task A-3: Create the `loyalty_members` Table
# MAGIC
# MAGIC Developer A also creates an entirely new table — this schema change exists **only on the `dev-loyalty-reviews` branch**.

# COMMAND ----------

# connect to dev-loyalty-reviews branch
conn_loyalty, _, _ = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

# Create loyalty_members table for customers with enough points
with conn_loyalty.cursor() as cur:
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {db_schema}.loyalty_members (
        id              SERIAL PRIMARY KEY,
        email           VARCHAR(255) NOT NULL REFERENCES {db_schema}.customers(email),
        tier            VARCHAR(20) NOT NULL DEFAULT 'Bronze'
            CHECK (tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')),
        enrolled_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
        total_earned    INT         NOT NULL DEFAULT 0
    );
""")

# Enroll customers with enough points
with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    INSERT INTO {db_schema}.loyalty_members (email, tier, enrolled_at, total_earned)
    SELECT
        email,
        CASE
            WHEN loyalty_points >= 3000 THEN 'Platinum'
            WHEN loyalty_points >= 1500 THEN 'Gold'
            WHEN loyalty_points >= 500  THEN 'Silver'
            ELSE 'Bronze'
        END,
        NOW(),
        loyalty_points
    FROM {db_schema}.customers
    WHERE loyalty_points > 0
    ON CONFLICT (id) DO NOTHING;
""")

with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    SELECT lm.id, u.name, lm.tier, lm.total_earned AS points
    FROM {db_schema}.loyalty_members lm
    JOIN {db_schema}.customers u ON u.email = lm.email
    ORDER BY lm.total_earned DESC
    LIMIT 10;
""")
    cols, rows = [d[0] for d in cur.description], cur.fetchall()
print("✅ Created 'loyalty_members' table and enrolled customers:")
print_table(cols, rows)
conn_loyalty.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task A-4: Seed Product Reviews on the Branch
# MAGIC
# MAGIC Developer A also seeds customer reviews collected from beta testers. These reviews
# MAGIC will be promoted to production along with the loyalty features in Lab 3.3 — giving
# MAGIC the storefront star ratings and customer feedback.

# COMMAND ----------

# connect to dev-loyalty-reviews branch
conn_loyalty, _, _ = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

import random
random.seed(42)

with conn_loyalty.cursor() as cur:
    # Create reviews table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.reviews (
            id SERIAL PRIMARY KEY,
            product_id INT NOT NULL REFERENCES {db_schema}.products(id),
            customer_id INT NOT NULL REFERENCES {db_schema}.customers(id),
            rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
            comment TEXT,
            review_date TIMESTAMP DEFAULT NOW()
        );
    """)

    positive_comments = [
        "Great product, highly recommend!",
        "Exceeded my expectations.",
        "Fast shipping and excellent quality.",
        "Would buy again in a heartbeat.",
        "Best purchase I've made this year.",
        "Solid build quality, very happy.",
    ]
    neutral_comments = [
        "Decent product for the price.",
        "Does what it's supposed to do.",
        "Average quality, nothing special.",
        "Okay but could be improved.",
    ]
    negative_comments = [
        "Not as described, somewhat disappointed.",
        "Quality could be better.",
        "Arrived late but product is okay.",
    ]

    reviews = []
    reviewed_pairs = set()
    for _ in range(80):
        product_id = random.randint(1, 50)
        customer_id = random.randint(1, 100)
        if (product_id, customer_id) in reviewed_pairs:
            continue
        reviewed_pairs.add((product_id, customer_id))
        rating = random.choices([1, 2, 3, 4, 5], weights=[5, 8, 15, 35, 37])[0]
        if rating >= 4:
            comment = random.choice(positive_comments)
        elif rating == 3:
            comment = random.choice(neutral_comments)
        else:
            comment = random.choice(negative_comments)
        day_offset = random.randint(0, 270)
        review_date = f"2024-01-{1 + (day_offset % 28):02d}"
        reviews.append((product_id, customer_id, rating, comment, review_date))

    cur.executemany(
        f"INSERT INTO {db_schema}.reviews (product_id, customer_id, rating, comment, review_date) "
        f"VALUES (%s, %s, %s, %s, %s)",
        reviews
    )

print(f"✅ Created reviews table and seeded {len(reviews)} product reviews on dev-loyalty-reviews branch")
conn_loyalty.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task A-5: Verify Production Branch is UNCHANGED ✅
# MAGIC
# MAGIC This is the critical test. Connect to the **`production`** branch and confirm that:
# MAGIC 1. `users` still has **no** `loyalty_points` column
# MAGIC
# MAGIC This proves that branches provide true **schema isolation**.

# COMMAND ----------

# connect to production branch
conn_prod, conn_host, conn_endpoint = connect_to_branch('production')

# COMMAND ----------

print("🔍 Checking production branch schema...\n")

# Check columns on users table in production
with conn_prod.cursor() as cur:
    cur.execute(f"""
    SELECT column_name, data_type, column_default, table_schema, table_name
    FROM information_schema.columns
    WHERE table_schema = '{db_schema}' AND table_name = 'customers'
    ORDER BY ordinal_position;
""")
    prod_columns = [row[0] for row in cur.fetchall()]

print(f"📋 Production branch columns: {prod_columns}")
print(f"   Has loyalty_points? {'loyalty_points' in prod_columns}")
print("\n" + "=" * 60)
print("🎯 RESULT: 'loyalty_points' and 'loyalty_members' exist ONLY")
print("   in 'dev-loyalty-reviews'. Production schema is untouched!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform checks to validate `loyalty_members` table does **not exist** in Production

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🌍 Developer B — Global Team
# MAGIC
# MAGIC **Goal:** Refactor the `orders` table to support true multi-currency by replacing the `currency` varchar column with a foreign key to a new `exchange_rates` table.
# MAGIC
# MAGIC This is a **breaking schema change** — it would have caused a conflict with Developer A's work if they shared a database. With Lakebase branching, it's completely isolated.
# MAGIC
# MAGIC ### Task B-1: Create Branch `modify-orders`

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

BRANCH_NAMEV2 = "modify-orders"

# Clean up from previous runs
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{BRANCH_NAMEV2}").wait()
    print(f"🧹 Cleaned up existing branch '{BRANCH_NAMEV2}'")
except Exception:
    pass

# Create your feature branch
print(f"\n🔄 Creating branch '{BRANCH_NAMEV2}' from production...")
w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        ttl=Duration(seconds=172800)  # 48-hour TTL
    )),
    branch_id=BRANCH_NAMEV2
).wait()
print(f"✅ Branch '{BRANCH_NAMEV2}' created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task B-2: Create the `exchange_rates` Table
# MAGIC
# MAGIC Developer B first creates the reference table that the new foreign key will point to.

# COMMAND ----------

# connect to orders branch
conn_orders, conn_host, conn_endpoint = connect_to_branch('modify-orders')

# COMMAND ----------

print("🔧 Developer B: Building multi-currency support in 'modify-orders' branch...\n")

# create exchange_rates table
with conn_orders.cursor() as cur:
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.exchange_rates (
        id              SERIAL PRIMARY KEY,
        currency_code   CHAR(3)         UNIQUE NOT NULL,
        currency_name   VARCHAR(100)    NOT NULL,
        rate_to_usd     NUMERIC(12, 6)  NOT NULL,
        last_updated    TIMESTAMP       NOT NULL DEFAULT NOW()
    );
""")

# insert data into the exchange_rates table    
with conn_orders.cursor() as cur:
    cur.execute(f"""
    INSERT INTO {db_schema}.exchange_rates (currency_code, currency_name, rate_to_usd) VALUES
        ('USD', 'US Dollar',          1.000000),
        ('EUR', 'Euro',               1.085000),
        ('GBP', 'British Pound',      1.265000),
        ('JPY', 'Japanese Yen',       0.006700),
        ('AED', 'UAE Dirham',         0.272300),
        ('INR', 'Indian Rupee',       0.012000),
        ('BRL', 'Brazilian Real',     0.200000),
        ('CNY', 'Chinese Yuan',       0.138000)
    ON CONFLICT (currency_code) DO NOTHING;
""")

with conn_orders.cursor() as cur:
    cur.execute(f"""
    SELECT currency_code, currency_name, rate_to_usd
    FROM {db_schema}.exchange_rates ORDER BY currency_code;
""")
    cols, rows = [d[0] for d in cur.description], cur.fetchall()
print("✅ Created 'exchange_rates' table with live rates:")
print_table(cols, rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task B-3: Migrate `orders.currency` to a Foreign Key
# MAGIC
# MAGIC Developer B adds a new FK column `currency_id`, migrates data from the old `currency` varchar, and can then drop the old column. This migration is entirely contained within the `modify-orders` branch.

# COMMAND ----------

print("🔧 Migrating orders.currency varchar → FK to exchange_rates...\n")

# Step 1: Add new FK column
with conn_orders.cursor() as cur:
    cur.execute(f"""
    ALTER TABLE {db_schema}.orders
    ADD COLUMN IF NOT EXISTS currency_id INT REFERENCES {db_schema}.exchange_rates(id);
""")

# Step 2: Populate FK from existing currency codes
with conn_orders.cursor() as cur:
    cur.execute(f"""
    UPDATE {db_schema}.orders o
    SET currency_id = er.id
    FROM {db_schema}.exchange_rates er
    WHERE er.currency_code = o.currency;
""")

# Step 3: Make the FK NOT NULL (all rows have been migrated)
with conn_orders.cursor() as cur:
    cur.execute(f"""
        ALTER TABLE {db_schema}.orders ALTER COLUMN currency_id SET NOT NULL;
""")

# Step 4: Drop the old varchar column
with conn_orders.cursor() as cur:
    cur.execute(f"""
        ALTER TABLE {db_schema}.orders DROP COLUMN currency;
""") 
    
print("✅ Migration complete. Lets verifying the result...\n")

# COMMAND ----------

print("Verifying the result...\n")

with conn_orders.cursor() as cur:
    cur.execute(f"""
    SELECT o.id, u.name AS customer, p.name AS product,
           o.quantity, o.total,
           er.currency_code, er.rate_to_usd,
           ROUND(o.total * er.rate_to_usd, 2) AS total_usd
    FROM {db_schema}.orders o
    JOIN {db_schema}.customers         u  ON u.id  = o.customer_id
    JOIN {db_schema}.products      p  ON p.id  = o.product_id
    JOIN {db_schema}.exchange_rates er ON er.id = o.currency_id
    ORDER BY o.id
    LIMIT 10;
""")
    cols, rows = [d[0] for d in cur.description], cur.fetchall()
print("📊 Orders with normalised currency FK (modify-orders branch):")
print_table(cols, rows)

# Confirm production still has varchar currency column
conn_prod, conn_host, conn_endpoint = connect_to_branch('production')
with conn_prod.cursor() as cur:
    cur.execute(f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{db_schema}' AND table_name = 'orders'
    ORDER BY ordinal_position;
""")
    cols2, rows2 = [d[0] for d in cur.description], cur.fetchall()
print("\n📋 'currency' column in orders table in PRODUCTION (still has varchar currency):")
print_table(cols2, rows2)

conn_orders.close()
conn_prod.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⚡ Developer C — Performance Team
# MAGIC
# MAGIC **Goal:** Create indexes on the `products` table to handle the high-traffic surge expected during the Spring Sale. Under load, full table scans on `products` would be catastrophic.
# MAGIC
# MAGIC ### Task C-1: Create Branch `add-index`

# COMMAND ----------

BRANCH_NAMEV3 = "add-index"

# Fixed configuration
db_schema = "ecommerce"
min_cu = 0.5
max_cu = 4.0
suspend_timeout_seconds = 1800

# Clean up from previous runs
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{BRANCH_NAMEV3}").wait()
    print(f"🧹 Cleaned up existing branch '{BRANCH_NAMEV3}'")
except Exception:
    pass

# Create your feature branch
print(f"\n🔄 Creating branch '{BRANCH_NAMEV3}' from production...")
w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        ttl=Duration(seconds=172800)  # 48-hour TTL
    )),
    branch_id=BRANCH_NAMEV3
).wait()
print(f"✅ Branch '{BRANCH_NAMEV3}' created!")

# COMMAND ----------

conn_index, conn_host, conn_endpoint = connect_to_branch('add-index')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task C-2: Create Performance Indexes on `products`
# MAGIC
# MAGIC Developer C creates three indexes on the `add-index` branch:
# MAGIC - **`idx_products_category`** — speeds up category browsing (most common query pattern)
# MAGIC - **`idx_products_price`** — speeds up price-range filter queries  
# MAGIC - **`idx_products_stock_qty`** — speeds up "in stock" filter queries
# MAGIC
# MAGIC These can be validated on the branch before promoting to production.

# COMMAND ----------

print("🔧 Developer C: Creating performance indexes on 'add-index' branch...\n")

index_statements = [
    ("idx_products_price",
     f"CREATE INDEX IF NOT EXISTS idx_products_price ON {db_schema}.products (price);",
     "Price-range filter queries")
]

for name, sql, purpose in index_statements:
    with conn_index.cursor() as cur:
        cur.execute(sql)
    print(f"   ✅ Created: {name} ({purpose})")

# Verify indexes exist
with conn_index.cursor() as cur:
    cur.execute(f"""
    SELECT indexname, indexdef
    FROM pg_indexes
    WHERE schemaname = '{db_schema}' AND tablename = 'products'
    ORDER BY indexname;
""")
    cols, rows = [d[0] for d in cur.description], cur.fetchall()
print("\n📋 Indexes on 'products' in add-index branch:")
print_table(cols, rows)

# Confirm production has NO extra indexes yet
conn_prod, conn_host, conn_endpoint = connect_to_branch('production')
with conn_prod.cursor() as cur:
    cur.execute(f"""
    SELECT indexname, indexdef
    FROM pg_indexes
    WHERE schemaname = 'public' AND tablename = 'products'
    ORDER BY indexname;
""")
    cols2, rows2 = [d[0] for d in cur.description], cur.fetchall()
print("\n📋 Indexes on 'products' in PRODUCTION branch (no custom indexes yet):")
print_table(cols2, rows2)

conn_index.close()
conn_prod.close()

print("\n" + "=" * 60)
print("🎯 SUMMARY: Three developers worked in PARALLEL with zero conflicts.")
print("   Each branch has isolated changes ready for review & merge.")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC Each developer created an isolated **branch** to accomplish their tasks in an isolated environment. They work independently, test their changes, and then merge back when ready. The production branch is never touched during development.
# MAGIC
# MAGIC | Developer | Team | Task |
# MAGIC |-----------|------|------|
# MAGIC | Developer A | Loyalty Team | Add `loyalty_points` column + new `loyalty_members` table |
# MAGIC | Developer B | Global Team | Add `exchange_rates` table + convert `currency` to a FK |
# MAGIC | Developer C | Performance Team | Add indexes to `products` for Spring Sale traffic surge |
