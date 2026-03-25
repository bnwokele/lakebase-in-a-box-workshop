# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 3: Resetting a Branch to Parent State
# MAGIC
# MAGIC **The Challenge:**  
# MAGIC You're developing a feature on a branch, but meanwhile another team pushes
# MAGIC schema changes to production. When you try to promote your migration, you discover
# MAGIC production has drifted. How do you handle it?
# MAGIC
# MAGIC **The Lakebase Solution: Reset Branch to Production State**  
# MAGIC This will replace the data and schema in our branch with the latest data and schema from its parent. This allows us to align our branch with prod so we can develop on a fresh updated copy of production data.
# MAGIC
# MAGIC **We can also create a new branch from prod but this demo demonstrates resetting to prod**
# MAGIC
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - How to detect that production has changed since your branch was created
# MAGIC - The **"re-set and re-test"** pattern for handling drift
# MAGIC - How to write migrations that are resilient to concurrent changes
# MAGIC
# MAGIC ## How It Works
# MAGIC ```
# MAGIC production ── another team adds email_verified ──── replay both migrations ── production (final)
# MAGIC        \                                                 ↑
# MAGIC         └── feature/order-priority                       │
# MAGIC              1. Add priority column                      │
# MAGIC              2. Discover drift!                          │
# MAGIC              3. Reset branch to match production         │
# MAGIC              4. Re-test migration ───────────────────────┘
# MAGIC              5. 🗑️ cleanup
# MAGIC ```
# MAGIC
# MAGIC > 📖 **Docs**: [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)

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

print("🔧 connect_to_branch() and delete_branch_safe() helpers defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Create Your Feature Branch
# MAGIC
# MAGIC You start developing the `order-priority` feature — adding a `priority` column to orders.

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

BRANCH_NAME = "dev-order-priority"

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
# MAGIC ## Step 2: Develop Your Migration on the Branch
# MAGIC
# MAGIC You add a `priority` column to the `orders` table.

# COMMAND ----------

feature_conn, _, _ = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

# Your migration: add priority to orders
YOUR_MIGRATION = f"""
ALTER TABLE {db_schema}.orders
ADD COLUMN IF NOT EXISTS priority VARCHAR(10) DEFAULT 'normal';

UPDATE {db_schema}.orders
SET priority = CASE
    WHEN total > 500 THEN 'high'
    WHEN total > 200 THEN 'medium'
    ELSE 'normal'
END;
"""

with feature_conn.cursor() as cur:
    cur.execute(YOUR_MIGRATION)

print("✅ Your migration applied on feature branch!")

with feature_conn.cursor() as cur:
    cur.execute(f"""
        SELECT priority, COUNT(*) as cnt, ROUND(AVG(total), 2) as avg_total
        FROM {db_schema}.orders
        GROUP BY priority ORDER BY priority
    """)
    print(f"\n📊 Order priorities (on branch):")
    for row in cur.fetchall():
        print(f"   {row[0]:10s} {row[1]:4d} orders  (avg ${row[2]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Meanwhile... Another Team Changes Production
# MAGIC
# MAGIC While you were working on your branch, another team added an `email_verified` column
# MAGIC to the `customers` table on **production**. You don't know about this yet.

# COMMAND ----------

# connect to production branch
conn, conn_host, conn_endpoint = connect_to_branch('production')

# COMMAND ----------

# Simulate another team's change on production
OTHER_TEAM_MIGRATION = f"""
ALTER TABLE {db_schema}.customers
ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT FALSE;

UPDATE {db_schema}.customers
SET email_verified = TRUE
WHERE id % 3 = 0;  -- roughly 1/3 verified
"""

with conn.cursor() as cur:
    cur.execute(OTHER_TEAM_MIGRATION)
    cur.execute(f"""
        SELECT email_verified, COUNT(*)
        FROM {db_schema}.customers
        GROUP BY email_verified
    """)
    print("📢 Another team pushed to production!")
    print(f"   Added 'email_verified' column to customers:")
    for row in cur.fetchall():
        status = "verified" if row[0] else "not verified"
        print(f"   • {status}: {row[1]} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Discover the Drift
# MAGIC
# MAGIC Before promoting your migration to production, you compare the schemas.
# MAGIC You discover that production has a column your branch doesn't!

# COMMAND ----------

# Get columns from production
with conn.cursor() as cur:
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    prod_columns = [row[0] for row in cur.fetchall()]

# Get columns from your branch
with feature_conn.cursor() as cur:
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    branch_columns = [row[0] for row in cur.fetchall()]

# Compare
prod_only = set(prod_columns) - set(branch_columns)
branch_only = set(branch_columns) - set(prod_columns)

print("🔍 Schema comparison (customers table):")
print(f"   Production columns: {prod_columns}")
print(f"   Branch columns:     {branch_columns}")
print(f"")
if prod_only:
    print(f"   ⚠️  Columns on production but NOT on branch: {prod_only}")
if branch_only:
    print(f"   ⚠️  Columns on branch but NOT on production: {branch_only}")
print(f"\n🚨 Production has drifted! It has changes your branch doesn't know about.")

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="Includes/images/branching/lab3.4_schema_diff_prod.png"
# MAGIC      alt="schema_diff_prod"
# MAGIC      width="1100">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Reset from the Production Branch
# MAGIC
# MAGIC The safest approach: Reset **the branch** to the current state of production (which includes
# MAGIC the other team's changes), and re-apply your migration on it.
# MAGIC
# MAGIC This is analogous to a `git rebase` — you replay your changes on top of the latest production.

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Navigate to the Lakebase UI and manually reset the branch. To reset a branch to its parent:**
# MAGIC
# MAGIC - Navigate to your project's Branches page in the Lakebase App.
# MAGIC - Click the Kebab menu icon next to the branch you want to reset and select Reset from parent.
# MAGIC - Confirm the reset operation.
# MAGIC <br>
# MAGIC
# MAGIC ![reset_branch.png](Includes/images/branching/lab3.4_reset_to_parent.png)
# MAGIC
# MAGIC After that, compare schemas and confirm both branches are now in sync
# MAGIC
# MAGIC ![reset_to_parent_schema_diff.png](Includes/images/branching/lab3.4_reset_to_parent_schema_diff.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MAKES SURE YOU HAVE RESET THE BRANCH BEFORE MOVING FORWARD WITH THE NEXT STEPS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Re-apply Your Migration on the Reset Branch
# MAGIC
# MAGIC Since your migration is **idempotent** (`IF NOT EXISTS`), it's safe to replay.
# MAGIC The new branch already has `email_verified` from production, and now gets `priority` from you.

# COMMAND ----------

feature_conn, _, _ = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

# Re-apply your migration
with feature_conn.cursor() as cur:
    cur.execute(YOUR_MIGRATION)

# Verify: the new branch should have BOTH changes
with feature_conn.cursor() as cur:
    # Check customers columns (should have email_verified from production)
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    v2_customer_cols = [row[0] for row in cur.fetchall()]

    # Check orders columns (should have priority from your migration)
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'orders'
        ORDER BY ordinal_position
    """)
    v2_order_cols = [row[0] for row in cur.fetchall()]

print(f"✅ Migration re-applied on '{BRANCH_NAME}'!")
print(f"   customers columns: {v2_customer_cols}")
print(f"   → Has email_verified (from other team): {'email_verified' in v2_customer_cols}")
print(f"   orders columns: {v2_order_cols}")
print(f"   → Has priority (your change): {'priority' in v2_order_cols}")
print(f"\n🎉 Both changes coexist — no conflicts!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Promote to Production
# MAGIC
# MAGIC Now that we've validated our migration works alongside the other team's changes,
# MAGIC we can safely replay just our migration on production.
# MAGIC
# MAGIC > Production already has `email_verified`. We just need to add `priority`.

# COMMAND ----------

# Replay YOUR migration on production (it's idempotent, safe to run)
with conn.cursor() as cur:
    cur.execute(YOUR_MIGRATION)

# Verify production has both changes
with conn.cursor() as cur:
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'orders'
        ORDER BY ordinal_position
    """)
    prod_order_cols = [row[0] for row in cur.fetchall()]

    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'customers'
        ORDER BY ordinal_position
    """)
    prod_customer_cols = [row[0] for row in cur.fetchall()]

print(f"✅ Migration promoted to production!")
print(f"   customers: {prod_customer_cols}")
print(f"   orders: {prod_order_cols}")
print(f"\n🎉 Production has both teams' changes!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storefront Checkpoint: Priority & Verification
# MAGIC
# MAGIC Open the **DataCart Storefront** and observe the latest features:
# MAGIC
# MAGIC 1. **Orders page** — Each order now shows a priority badge (high / medium / normal)
# MAGIC 2. **Navbar** — A green "Verified" badge appears next to the loyalty tier (Alice's email is verified)
# MAGIC
# MAGIC > These features appeared automatically because the backend detected the new columns in the schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Cleanup
# MAGIC
# MAGIC > ⚠️ **This cell is skipped by default.** Remove `%skip` below to delete the branches now.

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC feature_conn.close()
# MAGIC feature_conn_v2.close()
# MAGIC
# MAGIC for bn in [BRANCH_NAME, BRANCH_NAME_V2]:
# MAGIC     try:
# MAGIC         delete_branch_safe(bn)
# MAGIC     except Exception as e:
# MAGIC         print(f"   ('{bn}' already cleaned up)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |---|---|
# MAGIC | **Your branch** | Added `priority` column to orders |
# MAGIC | **Meanwhile** | Another team added `email_verified` to customers on production |
# MAGIC | **Detect drift** | Schema comparison revealed the gap |
# MAGIC | **Re-set Branch** | Reset branch from updated production |
# MAGIC | **Re-test** | Replayed your migration — validated it works alongside new changes |
# MAGIC | **Promote** | Replayed your migration on production (idempotent, safe) |
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC 1. **Always compare schemas** before promoting migrations
# MAGIC 2. **Write idempotent DDL** (`IF NOT EXISTS`, `IF EXISTS`) so migrations can be replayed
# MAGIC 3. **Re-set branch from current production** when drift is detected (like `git rebase`) 
# MAGIC 4. Additionally, we can create a new branch. **Branches are cheap** — creating a v2 branch costs nothing (copy-on-write)
