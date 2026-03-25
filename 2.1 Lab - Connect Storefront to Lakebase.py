# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Connect the DataCart Storefront to Lakebase
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Where We Are
# MAGIC
# MAGIC At this point in the workshop, two things have been completed:
# MAGIC
# MAGIC 1. **Lab 1.1** created a Lakebase Autoscaling project and seeded the `ecommerce` schema
# MAGIC    with 5 tables: `customers`, `products`, `inventory`, `orders`, and `order_items`.
# MAGIC
# MAGIC 2. **The DataCart Storefront app** has been created and deployed as a Databricks App
# MAGIC    (either via the UI, DABs, or CLI — see the Workshop Setup guide). The app is running
# MAGIC    but currently shows **"Loading..."** because it can't connect to the database yet.
# MAGIC
# MAGIC ## What This Lab Does
# MAGIC
# MAGIC Every Databricks App runs as a **service principal (SP)** — an automated identity
# MAGIC separate from your user account. The SP was created automatically when you created
# MAGIC the app. However, the SP needs two things before it can query Lakebase:
# MAGIC
# MAGIC 1. **A Postgres role** — Created automatically when you add the Lakebase project as an
# MAGIC    app resource
# MAGIC 2. **Schema-level grants** — The SP's Postgres role exists but has no permissions on
# MAGIC    the `ecommerce` schema. We need to explicitly grant access.
# MAGIC
# MAGIC This lab walks you through:
# MAGIC - Finding the SP identity for your app
# MAGIC - Understanding what permissions are needed and why
# MAGIC - Granting the SP access to read and write the `ecommerce` schema
# MAGIC - Verifying the grants work
# MAGIC - Granting access on dev branches (for later labs)
# MAGIC
# MAGIC ## How It Works
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────┐        ┌──────────────────────┐
# MAGIC │  Databricks App      │        │  Lakebase Project     │
# MAGIC │  "datacart-storefront"│       │  production branch    │
# MAGIC │                      │        │                       │
# MAGIC │  Runs as SP:         │  OAuth │  Postgres role:       │
# MAGIC │  8241cbc7-...        │───────▶│  "8241cbc7-..."       │
# MAGIC │                      │  token │                       │
# MAGIC │  Needs:              │        │  Needs:               │
# MAGIC │  • generate_db_cred  │        │  • USAGE on schema    │
# MAGIC │  • CAN_MANAGE on     │        │  • ALL on tables      │
# MAGIC │    project           │        │  • ALL on sequences   │
# MAGIC └──────────────────────┘        └──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC > 📖 **Docs**: [Grant user access tutorial](https://docs.databricks.com/aws/en/oltp/projects/grant-user-access-tutorial)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Install Dependencies

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Identify the Service Principal
# MAGIC
# MAGIC Every Databricks App has a service principal (SP) that acts as its identity.
# MAGIC The SP is created automatically when you create the app. Let's find it. **Make sure to change the APP_NAME variable to the name of the app you have deployed.**

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Derive project name from current user
current_user = w.current_user.me()
db_user = current_user.user_name
username_prefix = db_user.split("@")[0].replace(".", "-")
project_name = f"lakebase-branching-workshop-{username_prefix}"

# Look up the app to find its service principal
APP_NAME = "<ADD_YOUR_APP_NAME>"

app_info = w.apps.get(APP_NAME)
SP_CLIENT_ID = app_info.service_principal_client_id
SP_NAME = app_info.service_principal_name
APP_URL = app_info.url

print(f"📋 App Details:")
print(f"   App Name:      {APP_NAME}")
print(f"   App URL:       {APP_URL}")
print(f"   SP Client ID:  {SP_CLIENT_ID}")
print(f"   SP Name:       {SP_NAME}")
print(f"")
print(f"📋 Lakebase Project:")
print(f"   Project:       {project_name}")
print(f"   Schema:        ecommerce")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Understand the Permission Model
# MAGIC
# MAGIC The SP needs two layers of access:
# MAGIC
# MAGIC | Layer | What | How |
# MAGIC |-------|------|-----|
# MAGIC | **Lakebase project** | `CAN_MANAGE` permission on the project | Added via the app resource |
# MAGIC | **Postgres role** | Auto-created when the app resource is added | Lakebase OAuth system links the SP identity to a Postgres role |
# MAGIC | **Schema grants** | `USAGE`, `ALL` on tables/sequences in `ecommerce` | Granted by the project owner (you) — **this is what we do now** |
# MAGIC
# MAGIC > **Why can't the SP access the schema automatically?**
# MAGIC > In PostgreSQL, having a role doesn't grant access to any schemas or tables.
# MAGIC > The project owner must explicitly grant permissions — this is standard Postgres
# MAGIC > security and a good practice for production databases.
# MAGIC
# MAGIC > **Why not just use your user account?**
# MAGIC > Databricks Apps run as service principals for security isolation. The app
# MAGIC > should never use a human user's credentials — SPs have their own OAuth tokens
# MAGIC > that rotate automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Connect to the Production Branch
# MAGIC
# MAGIC We connect as **you** (the project owner) to grant roles to the SP.
# MAGIC Only the project owner or a user with sufficient privileges can grant access.

# COMMAND ----------

import psycopg2

DB_SCHEMA = "ecommerce"

# Get production endpoint
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
prod_branch = next(b for b in branches if b.status and b.status.default)
prod_branch_name = prod_branch.name

endpoints = list(w.postgres.list_endpoints(parent=prod_branch_name))
prod_endpoint = endpoints[0]
prod_host = prod_endpoint.status.hosts.host
prod_endpoint_name = prod_endpoint.name

# Generate your OAuth credential (project owner)
cred = w.postgres.generate_database_credential(endpoint=prod_endpoint_name)

conn = psycopg2.connect(
    host=prod_host,
    port=5432,
    dbname="databricks_postgres",
    user=db_user,
    password=cred.token,
    sslmode="require"
)
conn.autocommit = True

print(f"✅ Connected to production branch as {db_user}")
print(f"   Host: {prod_host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify the SP Role Exists
# MAGIC
# MAGIC When you added the Lakebase project as an app resource (Workshop Setup Step 4),
# MAGIC the Lakebase OAuth system should have auto-created a Postgres role for the SP.
# MAGIC Let's confirm.

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute(f"""
        SELECT r.rolname, r.rolcanlogin
        FROM pg_roles r
        WHERE r.rolname = '{SP_CLIENT_ID}'
    """)
    role_info = cur.fetchone()

if role_info:
    print(f"✅ SP role exists in Postgres:")
    print(f"   Role name: {role_info[0]}")
    print(f"   Can login: {role_info[1]}")
else:
    print(f"❌ SP role '{SP_CLIENT_ID}' NOT found!")
    print(f"")
    print(f"   This means the app resource was not added correctly.")
    print(f"   Go to Compute > Apps > {APP_NAME} > Settings:")
    print(f"   1. Click 'Add Resource'")
    print(f"   2. Select 'Database'")
    print(f"   3. Choose your Lakebase project")
    print(f"   4. Grant 'Can connect' permission")
    print(f"   5. Save and redeploy the app")
    print(f"   6. Re-run this cell")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Grant Schema Permissions
# MAGIC
# MAGIC Now we grant the SP access to the `ecommerce` schema. This includes:
# MAGIC
# MAGIC | Grant | Purpose |
# MAGIC |-------|---------|
# MAGIC | `USAGE ON SCHEMA` | Allows the SP to see the schema and its objects |
# MAGIC | `ALL ON ALL TABLES` | Read products/inventory, write orders, etc. |
# MAGIC | `ALL ON ALL SEQUENCES` | Needed for `SERIAL` columns (auto-increment IDs) |
# MAGIC | `ALTER DEFAULT PRIVILEGES` | Ensures future tables (reviews, loyalty_members, promotions) are automatically accessible |
# MAGIC
# MAGIC > The `ALTER DEFAULT PRIVILEGES` grants are important — as the workshop progresses
# MAGIC > and new tables are created (reviews in Lab 3.2, promotions in Lab 5.1), the SP
# MAGIC > will automatically have access without needing to re-run this notebook.

# COMMAND ----------

with conn.cursor() as cur:
    sp_role = f'"{SP_CLIENT_ID}"'

    # Grant schema usage
    cur.execute(f"GRANT USAGE ON SCHEMA {DB_SCHEMA} TO {sp_role};")
    print(f"✅ Granted USAGE on schema {DB_SCHEMA}")

    # Grant ALL on all existing tables
    cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {DB_SCHEMA} TO {sp_role};")
    print(f"✅ Granted ALL on all tables in {DB_SCHEMA}")

    # Grant ALL on all sequences (needed for SERIAL columns / INSERT)
    cur.execute(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {DB_SCHEMA} TO {sp_role};")
    print(f"✅ Granted ALL on all sequences in {DB_SCHEMA}")

    # Set default privileges for future tables
    cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {DB_SCHEMA} GRANT ALL ON TABLES TO {sp_role};")
    print(f"✅ Set default privileges for future tables")

    cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {DB_SCHEMA} GRANT ALL ON SEQUENCES TO {sp_role};")
    print(f"✅ Set default privileges for future sequences")

print(f"\n🎉 Service principal {SP_CLIENT_ID} now has full access to {DB_SCHEMA}.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify the Grants
# MAGIC
# MAGIC Let's confirm the SP has the expected permissions by querying `information_schema`.

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute(f"""
        SELECT grantee, table_name, privilege_type
        FROM information_schema.table_privileges
        WHERE table_schema = '{DB_SCHEMA}'
          AND grantee = '{SP_CLIENT_ID}'
        ORDER BY table_name, privilege_type
    """)
    grants = cur.fetchall()

if grants:
    print(f"✅ Grants verified for SP on {DB_SCHEMA} tables:\n")
    current_table = ""
    for g in grants:
        if g[1] != current_table:
            current_table = g[1]
            print(f"  {DB_SCHEMA}.{current_table}:")
        print(f"    • {g[2]}")
else:
    print(f"⚠️ No direct table grants found.")
    print(f"   The default privileges should still work for future tables.")
    print(f"   Try accessing the storefront to confirm connectivity.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Test the Storefront
# MAGIC
# MAGIC The app should now be able to connect. Let's verify by calling the debug endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC Check the app! The service should be connected now! show screenshot

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Grant Roles on Dev Branches (Optional)
# MAGIC
# MAGIC If you've already created branches in Labs 3.1–3.4, the SP needs roles on those too.
# MAGIC This is because each branch has its own independent set of Postgres roles and grants.
# MAGIC
# MAGIC > You can re-run this cell any time after creating new branches.

# COMMAND ----------

import time

all_branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))

granted_count = 0
for branch in all_branches:
    branch_id = branch.name.split("/branches/")[-1]
    if branch_id == "production":
        continue  # Already done above

    try:
        branch_endpoints = list(w.postgres.list_endpoints(parent=branch.name))
        if not branch_endpoints:
            print(f"   Skipping {branch_id} (no endpoint)")
            continue

        branch_host = branch_endpoints[0].status.hosts.host
        branch_endpoint_name = branch_endpoints[0].name
        branch_cred = w.postgres.generate_database_credential(endpoint=branch_endpoint_name)

        branch_conn = psycopg2.connect(
            host=branch_host, port=5432,
            dbname="databricks_postgres",
            user=db_user, password=branch_cred.token,
            sslmode="require"
        )
        branch_conn.autocommit = True

        with branch_conn.cursor() as cur:
            sp_role = f'"{SP_CLIENT_ID}"'
            cur.execute(f"GRANT USAGE ON SCHEMA {DB_SCHEMA} TO {sp_role};")
            cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {DB_SCHEMA} TO {sp_role};")
            cur.execute(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {DB_SCHEMA} TO {sp_role};")
            cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {DB_SCHEMA} GRANT ALL ON TABLES TO {sp_role};")
            cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {DB_SCHEMA} GRANT ALL ON SEQUENCES TO {sp_role};")

        branch_conn.close()
        print(f"   ✅ Granted SP roles on branch: {branch_id}")
        granted_count += 1
    except Exception as e:
        print(f"   ❌ Failed on {branch_id}: {e}")

if granted_count == 0:
    print("ℹ️ No dev branches found (only production). This is expected before Labs 3.1–3.4.")
else:
    print(f"\n✅ Granted roles on {granted_count} branch(es)")

# COMMAND ----------

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |------|---------------|
# MAGIC | **Identify SP** | Found the app's service principal client ID via the SDK |
# MAGIC | **Verify role** | Confirmed the Postgres role was auto-created by the app resource |
# MAGIC | **Grant permissions** | Gave the SP `USAGE`, `ALL` on tables/sequences in `ecommerce` |
# MAGIC | **Default privileges** | Ensured future tables (reviews, promotions, etc.) are auto-accessible |
# MAGIC | **Test connectivity** | Verified the storefront can query Lakebase |
# MAGIC
# MAGIC ### What's Next
# MAGIC
# MAGIC The DataCart Storefront is now connected and ready. As you run through the remaining
# MAGIC labs, the storefront will **evolve automatically**:
# MAGIC
# MAGIC - **Lab 3.3** — Star ratings, loyalty badges, and "Earn pts" labels appear
# MAGIC - **Lab 3.4** — Priority badges on orders, verified badge in navbar
# MAGIC - **Lab 4.1** — Orders page breaks during the disaster, then recovers after PITR
# MAGIC - **Lab 5.1** — Sale badges and discount prices appear via reverse ETL
# MAGIC
# MAGIC > The `ALTER DEFAULT PRIVILEGES` grants ensure the SP can access new tables
# MAGIC > created in later labs without needing to re-run this notebook.
