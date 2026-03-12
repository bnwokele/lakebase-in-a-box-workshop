# Databricks notebook source
# MAGIC %md
# MAGIC # 🧹 Cleanup: Delete Branches & Project
# MAGIC
# MAGIC This notebook cleans up all resources created by the scenario notebooks.
# MAGIC
# MAGIC ## What It Does
# MAGIC 1. Lists and deletes **all non-default branches**
# MAGIC 2. Optionally deletes the **entire Lakebase project**
# MAGIC
# MAGIC > ⚠️ **Deleting the project is irreversible.** Only do this when you're completely done.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Derive project name from the current user's identity
current_user = w.current_user.me()
db_user = current_user.user_name
username_prefix = db_user.split("@")[0].replace(".", "-")
project_name = f"lakebase-branching-{username_prefix}"

# Set to True to delete the entire project (not just branches)
DELETE_PROJECT = False

print(f"🧹 Cleanup target: {project_name}")
print(f"   Delete project: {DELETE_PROJECT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: List All Branches

# COMMAND ----------

try:
    branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
    
    print(f"📋 Branches in '{project_name}' ({len(branches)} total):")
    for b in branches:
        branch_id = b.name.split("/branches/")[-1]
        is_default = "⭐ default (protected)" if b.status and b.status.default else "  (can delete)"
        print(f"   • {branch_id:30s} {is_default}")
except Exception as e:
    print(f"⚠️ Could not list branches: {e}")
    print(f"   The project may not exist.")
    branches = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Delete Non-Default Branches

# COMMAND ----------

deleted = 0
skipped = 0

for b in branches:
    branch_id = b.name.split("/branches/")[-1]
    
    # Skip the default (production) branch — it can't be deleted directly
    if b.status and b.status.default:
        print(f"   ⏭️ Skipping '{branch_id}' (default branch)")
        skipped += 1
        continue
    
    try:
        w.postgres.delete_branch(name=b.name).wait()
        print(f"   🗑️ Deleted '{branch_id}'")
        deleted += 1
    except Exception as e:
        print(f"   ⚠️ Could not delete '{branch_id}': {e}")

print(f"\n📊 Results: {deleted} deleted, {skipped} skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Delete the Project (Optional)
# MAGIC
# MAGIC > ⚠️ Set `DELETE_PROJECT = True` above to enable this step.
# MAGIC > This deletes the entire Lakebase project, including the production branch and all data.

# COMMAND ----------

if DELETE_PROJECT:
    try:
        print(f"🔄 Deleting project '{project_name}'...")
        w.postgres.delete_project(name=f"projects/{project_name}").wait()
        print(f"✅ Project '{project_name}' deleted.")
    except Exception as e:
        print(f"❌ Could not delete project: {e}")
else:
    print(f"ℹ️  Project '{project_name}' was NOT deleted.")
    print(f"   Set DELETE_PROJECT = True and re-run to delete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Cleanup Complete
# MAGIC
# MAGIC | Action | Status |
# MAGIC |---|---|
# MAGIC | Non-default branches | Deleted |
# MAGIC | Project | Kept (unless `DELETE_PROJECT = True`) |
