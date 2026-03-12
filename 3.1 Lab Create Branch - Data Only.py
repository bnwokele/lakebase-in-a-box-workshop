# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 1: Production Data in Dev — No Schema Changes
# MAGIC
# MAGIC Let start easy! We will create a copy of the production data for development/testing Then we will modify the data without any risk to the production database.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - How to create a **branch** from `production` (instant, zero-cost copy-on-write)
# MAGIC - How branches provide **full data isolation** — changes on a branch don't affect `production`
# MAGIC - How to query production data safely on a branch
# MAGIC - How **branch TTL** (time-to-live) auto-cleans up after you're done
# MAGIC
# MAGIC ## How It Works
# MAGIC ```
# MAGIC production ─────────────────────────────────────────── (untouched)
# MAGIC        \
# MAGIC         └── dev-readonly ── query ── insert ── 🗑️ (auto-expires)
# MAGIC              (instant copy)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Run Setup
# MAGIC
# MAGIC This runs notebook `00_Setup_Project` to ensure the project exists, data is seeded,
# MAGIC and all shared variables (`w`, `project_name`, `conn`, `connect_to_branch()`, etc.) are available.

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
# MAGIC ## Step 1: Create a Dev Branch
# MAGIC
# MAGIC We'll create a branch called `dev-readonly` from `production`. This is an **instant** operation
# MAGIC thanks to Lakebase's copy-on-write architecture — no data is physically duplicated.
# MAGIC
# MAGIC We also set a **TTL of 24 hours**, so the branch auto-deletes if we forget to clean up.

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

BRANCH_NAME = "dev-readonly"

# Fixed configuration
db_schema = "ecommerce"
min_cu = 0.5
max_cu = 4.0
suspend_timeout_seconds = 1800

# Delete the branch if it already exists (from a previous run)
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{BRANCH_NAME}").wait()
    print(f"🧹 Cleaned up existing branch '{BRANCH_NAME}'")
except Exception:
    pass  # Branch doesn't exist, that's fine

# Create the branch
print(f"🔄 Creating branch '{BRANCH_NAME}' from production...")

branch_result = w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        ttl=Duration(seconds=86400)  # 24-hour TTL
    )),
    branch_id=BRANCH_NAME
).wait()

print(f"✅ Branch '{BRANCH_NAME}' created!")
print(f"   Source: production")
print(f"   TTL: 24 hours (auto-deletes after expiry)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Connect to the Dev Branch
# MAGIC
# MAGIC Each branch gets its own compute endpoint. We use the `connect_to_branch()` helper
# MAGIC (defined in notebook 00) to connect.

# COMMAND ----------

dev_conn, dev_host, dev_endpoint = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Query Production Data on the Branch
# MAGIC
# MAGIC The branch has a **complete copy** of the production data. Let's verify by running the same
# MAGIC queries we'd run on `production`.
# MAGIC
# MAGIC > 💡 **Key insight**: No data was copied during branch creation. Lakebase uses
# MAGIC > copy-on-write — the branch shares storage with `production` until data diverges.

# COMMAND ----------

with dev_conn.cursor() as cur:
    # Row counts — should match production exactly
    tables = ["customers", "products", "orders"]
    print(f"📊 Data on branch '{BRANCH_NAME}' (schema: {db_schema}):")
    for table in tables:
        cur.execute(f"SELECT count(*) FROM {db_schema}.{table}")
        count = cur.fetchone()[0]
        print(f"   • {table:20s} {count:>6} rows")

    # Run an analytics query
    print(f"\n📈 Top 5 customers by total spend:")
    cur.execute(f"""
        SELECT c.name, COUNT(o.id) as order_count, ROUND(SUM(o.total), 2) as total_spent
        FROM {db_schema}.customers c
        JOIN {db_schema}.orders o ON c.id = o.customer_id
        GROUP BY c.name
        ORDER BY total_spent DESC
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:20s}  {row[1]:3d} orders  ${row[2]:>10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Demonstrate Data Isolation
# MAGIC
# MAGIC Now let's prove that branches are **fully isolated**. We'll:
# MAGIC 1. Insert a new customer on the **branch**
# MAGIC 2. Verify it exists on the **branch**
# MAGIC 3. Verify it does **NOT** exist on **production**
# MAGIC
# MAGIC This is the core value of branching — developers can freely experiment without any risk
# MAGIC to production data.

# COMMAND ----------

# connect to production branch
conn, conn_host, conn_endpoint = connect_to_branch('production')

# COMMAND ----------

# Insert a test customer on the dev branch
with dev_conn.cursor() as cur:
    cur.execute(f"""
        INSERT INTO {db_schema}.customers (name, email)
        VALUES ('Branch Test User', 'branch.test@example.com')
    """)
    cur.execute(f"SELECT count(*) FROM {db_schema}.customers")
    branch_count = cur.fetchone()[0]

# Check production — the test customer should NOT be there
with conn.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM {db_schema}.customers")
    prod_count = cur.fetchone()[0]

print(f"📊 Customer counts after insert:")
print(f"   Branch '{BRANCH_NAME}': {branch_count} customers (includes test user)")
print(f"   Production:             {prod_count} customers (unchanged!)")
print(f"")
print(f"✅ Data isolation confirmed — branch changes don't affect production!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Cleanup — Delete the Branch
# MAGIC
# MAGIC We set a 24-hour TTL, so the branch will auto-delete. You can also delete it explicitly.
# MAGIC
# MAGIC > 💡 **In practice**: TTL is useful for dev/test branches that developers might forget about.
# MAGIC > Lakebase ensures they don't linger and consume resources.
# MAGIC
# MAGIC > ⚠️ **This cell is skipped by default.** Remove `%skip` below to delete the branch now.

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC # Close the branch connection first
# MAGIC dev_conn.close()
# MAGIC
# MAGIC # Delete the branch (with retry in case endpoint is still reconciling)
# MAGIC delete_branch_safe(BRANCH_NAME)
# MAGIC
# MAGIC # Verify — list remaining branches
# MAGIC branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
# MAGIC print(f"\n📋 Remaining branches:")
# MAGIC for b in branches:
# MAGIC     branch_id = b.name.split("/branches/")[-1]
# MAGIC     print(f"   • {branch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC | Concept | What Happened |
# MAGIC |---|---|
# MAGIC | **Branch creation** | Instant — no data copied (copy-on-write) |
# MAGIC | **Data access** | Full production dataset available immediately |
# MAGIC | **Isolation** | Insert on branch did NOT appear on production |
# MAGIC | **Cleanup** | Explicit delete, or automatic via TTL |
# MAGIC
# MAGIC ### When to Use This Pattern
# MAGIC - **Development**: Query prod data without risk
# MAGIC - **Testing**: Run integration tests against realistic data
# MAGIC - **Analytics**: Ad-hoc queries on a snapshot without affecting OLTP performance
# MAGIC - **Debugging**: Reproduce a production issue in an isolated environment
