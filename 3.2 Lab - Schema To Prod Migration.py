# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 2: Schema Changes — Feature Branch to Production
# MAGIC
# MAGIC In the last demo, we created a feature branch `dev-loyalty-reviews`, added  the `loyalty_points` column to the `customers` table, created a new `loyalty_members` table to support DataCart's Spring Sale loyalty program, and added a `reviews` table to support product reviews in the UI. Now we will show how to promote these changes to the production branch.
# MAGIC
# MAGIC ## How to run validated schema changes to `production`
# MAGIC - Re-run the same migrations in prod (integrate with Github actions or other versioning tool)
# MAGIC
# MAGIC ## How It Works
# MAGIC ```
# MAGIC production ─────────────────── replay migration ────── production (with loyalty_points, loyalty_memebers, and reviews)
# MAGIC        \                           ↑
# MAGIC         └── feature/dev-loyalty-reviews   │
# MAGIC              1. ALTER TABLE        │
# MAGIC              2. Backfill data      │
# MAGIC              3. Validate ──────────┘
# MAGIC              4. 🗑️ delete branch
# MAGIC ```
# MAGIC
# MAGIC **[Technical Blog](https://community.databricks.com/t5/technical-blog/lakebase-branching-meets-docker-the-migration-safety-net-i-wish/ba-p/149945) to learn more about the great benefits of Lakebase Branching from an ex-backend engineer**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Run Setup

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
# MAGIC ## Step 1: Connect to dev-loyalty-reviews branch

# COMMAND ----------

db_schema = "ecommerce"
BRANCH_NAME = "dev-loyalty-reviews"

# COMMAND ----------

# connect to dev-loyalty-reviews branch
conn_loyalty, _, _ = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

# Show updated customers table with loyalty_points column
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

# COMMAND ----------

# Show loyalty_memebers table in dev-loyalty-reviews branch
with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    SELECT lm.id, u.name, lm.tier, lm.total_earned AS points
    FROM {db_schema}.loyalty_members lm
    JOIN {db_schema}.customers u ON u.email = lm.email
    ORDER BY lm.total_earned DESC
    LIMIT 10;
""")
    cols, rows = [d[0] for d in cur.description], cur.fetchall()
print("✅ 'loyalty_members' table and enrolled customers (dev-loyalty-reviews branch):")
print_table(cols, rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Production is Untouched
# MAGIC
# MAGIC The schema change only exists on the branch. Production still has the original schema.

# COMMAND ----------

# connect to production branch
conn, conn_host, conn_endpoint = connect_to_branch('production')

# COMMAND ----------

print("🔍 Checking production branch schema...\n")

# Check columns on customers table in production
with conn.cursor() as cur:
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
# MAGIC ## Step 3: Compare Schemas in the UI
# MAGIC
# MAGIC Lakebase provides a built-in **Schema Diff** tool that lets you visually compare the schema
# MAGIC of a branch against its parent. This is a great way to review what changed before promoting.
# MAGIC
# MAGIC 👉 **Try it now:**
# MAGIC 1. Open the Lakebase UI (link printed below)
# MAGIC 2. Navigate to the `feature-loyalty-tier` branch
# MAGIC 3. Click the **Schema diff** button to see the differences vs production
# MAGIC
# MAGIC > 📖 **Docs**: [Compare branch schemas](https://docs.databricks.com/aws/en/oltp/projects/manage-branches#compare-branch-schemas)

# COMMAND ----------

# MAGIC %md
# MAGIC Picture of Schema differences

# COMMAND ----------

# Print direct link to the branch in the Lakebase UI
branch_obj = w.postgres.get_branch(name=f"projects/{project_name}/branches/{BRANCH_NAME}")
branch_uid = branch_obj.uid
workspace_host = w.config.host.rstrip("/")
lakebase_url = f"{workspace_host}/lakebase/projects/{branch_uid }"
print(f"🔗 Open the branch in the Lakebase UI and click 'Schema diff':")
print(f"   {lakebase_url}/branches/{branch_uid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Promote Migration to Production (Replay on Production)
# MAGIC
# MAGIC Now we replay the **exact same DDL** on `production`:
# MAGIC
# MAGIC 1. We validated the migration on the branch ✅
# MAGIC 2. Now we replay the same idempotent DDL on `production`
# MAGIC 3. Since the SQL uses `IF NOT EXISTS`, it's safe to run multiple times

# COMMAND ----------

# The migration script — idempotent and replayable
MIGRATION_SQL = f"""
-- Add loyalty_points column to users
ALTER TABLE {db_schema}.customers
    ADD COLUMN IF NOT EXISTS loyalty_points INT NOT NULL DEFAULT 0;

-- Backfill some loyalty points based on order history
UPDATE {db_schema}.customers u
    SET loyalty_points = (
        SELECT COALESCE(SUM(FLOOR(o.total)::INT), 0)
        FROM {db_schema}.orders o WHERE o.customer_id = u.id
    );

-- Create loyalty_members table for customers with enough points
CREATE TABLE IF NOT EXISTS {db_schema}.loyalty_members (
        id              SERIAL PRIMARY KEY,
        email           VARCHAR(255) NOT NULL REFERENCES {db_schema}.customers(email),
        tier            VARCHAR(20) NOT NULL DEFAULT 'Bronze'
            CHECK (tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')),
        enrolled_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
        total_earned    INT         NOT NULL DEFAULT 0
    );

-- Enroll customers with enough points
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

-- Create reviews table (product ratings from beta testers)
CREATE TABLE IF NOT EXISTS {db_schema}.reviews (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES {db_schema}.products(id),
    customer_id INT NOT NULL REFERENCES {db_schema}.customers(id),
    rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comment TEXT,
    review_date TIMESTAMP DEFAULT NOW()
);
"""

print("✅ Migration Script Created!")

# COMMAND ----------

# Replay the exact same migration on production
with conn.cursor() as cur:
    cur.execute(MIGRATION_SQL)

print("✅ Migration replayed on production!")

# Verify on production
with conn.cursor() as cur:
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
print("🎯 RESULT: 'loyalty_points' and 'loyalty_members' exist now exist in Production")
print("=" * 60)

print(f"\n🎉 Schema change successfully promoted to production!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed Product Reviews on Production
# MAGIC
# MAGIC The reviews table was created by the migration. Now seed it with the beta tester
# MAGIC reviews data that Developer A prepared on the branch.

# COMMAND ----------

import random
random.seed(42)

with conn.cursor() as cur:
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

print(f"✅ Seeded {len(reviews)} product reviews on production!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storefront Checkpoint: Loyalty Program & Reviews Go Live!
# MAGIC
# MAGIC Open the **DataCart Storefront** and observe the new features appearing in real time:
# MAGIC
# MAGIC 1. **Navbar** — Alice Smith's loyalty tier badge appears (Bronze/Silver/Gold/Platinum) with points count
# MAGIC 2. **Home page** — A purple "Loyalty Program Active!" banner appears below the hero
# MAGIC 3. **Products** — Star ratings now appear on every product card
# MAGIC 4. **Product detail** — Full customer reviews section with star ratings and comments
# MAGIC 5. **Product cards** — "Earn X pts" labels appear below prices
# MAGIC 6. **Cart** — Shows how many loyalty points you'll earn with your order
# MAGIC
# MAGIC > The storefront auto-detects schema changes every 30 seconds. No redeployment needed!
# MAGIC > If you don't see changes immediately, wait 30 seconds and refresh.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Cleanup — Delete the Feature Branch
# MAGIC
# MAGIC The feature branch has served its purpose. You can safely delete it, or let TTL handle it.
# MAGIC
# MAGIC > ⚠️ **This cell is skipped by default.** Remove `%skip` below to delete the branch now.

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC feature_conn.close()
# MAGIC
# MAGIC delete_branch_safe(BRANCH_NAME)
# MAGIC
# MAGIC # List remaining branches
# MAGIC branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
# MAGIC print(f"\n📋 Remaining branches:")
# MAGIC for b in branches:
# MAGIC     branch_id = b.name.split("/branches/")[-1]
# MAGIC     print(f"   • {branch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |---|---|
# MAGIC | **Branch** | Created `dev-loyalty-reviews` from production (instant) |
# MAGIC | **Develop** | Added `loyalty_points` column in customers + `loyalty_members` table |
# MAGIC | **Validate** | Verified schema, data integrity, tier distribution |
# MAGIC | **Isolate** | Confirmed production was untouched during development |
# MAGIC | **Promote** | Replayed the same idempotent DDL on production |
# MAGIC | **Cleanup** | Deleted the feature branch |
# MAGIC
# MAGIC ### The Migration Replay Pattern
# MAGIC ```
# MAGIC 1. Write idempotent DDL (ALTER TABLE ... IF NOT EXISTS, etc.)
# MAGIC 2. Test on branch → validate → fix if needed → re-test
# MAGIC 3. Once validated, replay the DDL on production
# MAGIC 4. Delete the branch
# MAGIC ```
