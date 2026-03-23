# Lakebase-in-a-Box Workshop

This hands-on workshop introduces Databricks Lakebase — a fully managed, serverless PostgreSQL database built on open architecture that decouples compute from storage and demonstrates how to leverage its unique capabilities to build and maintain production-grade applications with unprecedented agility.

You will step into the role of a developer at DataCart, a rapidly growing e-commerce platform. The stakes are high: the "Spring Sale" launch is weeks away, and you and your team needs to roll out international currency support, a new loyalty program, and performance optimizations—all while ensuring the production site stays bulletproof.

## What's Included

### Core Modules

| # | Notebook | Type | Description |
|---|----------|------|-------------|
| 0 | `0 Workshop Introduction` | Lecture | Workshop overview, Lakebase architecture, and the DataCart scenario |
| 1 | `1 Lecture - Creating and Exploring a Lakebase Autoscaling Project` | Lecture | Create a project, explore settings, connect to your database, create tables, and query PostgreSQL system metadata |
| 1.1 | `1.1 Lab Setup Project` | Lab | Automated project creation, OAuth connection, and e-commerce schema seeding (customers, products, orders) |
| 2 | `2 Lecture - Roles and Permissions` | Lecture | Workspace vs. database permission layers, OAuth roles, native Postgres roles, and GRANT/REVOKE workflows |
| 2.1 | `2.1 Lab Connect Storefront to Lakebase` | Lab | Handle permissions of app service principal to connect it to Lakebase |
| 3 | `3 Lecture - Database Branching` | Lecture | Branching concepts, copy-on-write storage, branch strategies, Schema Diff, and branch lifecycle management |
| 3.1 | `3.1 Lab - Parallel Development` | Lab | Three developers work in parallel on isolated branches (loyalty features, multi-currency support, performance indexes) |
| 3.2 | `3.2 Lab - Schema To Prod Migration` | Lab | Promote validated schema changes from a feature branch to production by replaying DDL |
| 3.3 | `3.3 Lab - Branch Reset` | Lab | Detect production drift, reset a branch to match parent state, and re-test migrations |
| 4 | `4 Lecture - Point in Time Restore & Snapshots` | Lecture | PITR restore windows, snapshot scheduling, and when to use each |
| 4.1 | `4.1 Lab - Point in Time Recovery (Disaster Management)` | Lab | Simulate an accidental `DROP TABLE` and recover using Point-in-Time Recovery |
| 5 | `5 Lecture - Reverse ETL` | Lecture | Introduces Reverse ETL and how Lakebase makes it easy to do |
| 5.1 | `5.1 Lab - Reverse ETL with Synced Table` | Lab | Creates promotions Delta table in Unity Catalog, syncs to Lakebase via reverse ETL |
| 6 | `6 Lecture - Monitoring` | Lecture | How to monitor your Lakebase instance / How to interpret graphs provided in the Lakebase monitoring page |
| 7 | `7 Lecture - Connects Apps to Lakebase` | Lecture | How to connect external apps to lakebase |


### DataCart Storefront App

A customer-facing e-commerce web application (React + FastAPI) that **evolves in real time** as each lab modifies the database. Located in `datacart-storefront/`.

| Feature | Appears After |
|---------|--------------|
| Products, stock badges, cart, orders | Lab 1.1 |
| Star ratings, reviews | Lab 3.3 |
| Loyalty tier badge, points, "Earn X pts" | Lab 3.3 |
| Priority badges, verified badge | Lab 3.4 |
| Graceful degradation during disaster | Lab 4.1 |
| Sale badges, discount prices, promo deals | Lab 5.1 |

## Quick Start

### Prerequisites

- Databricks workspace with Lakebase support

### Setup Steps

1. **Run Lab 1.1** — Creates the Lakebase project and seeds the database

2. **Update `datacart-storefront/app.yaml`** — Set your project name:
   ```yaml
   env:
     - name: ENDPOINT_NAME
       value: "projects/<project-name>/branches/production/endpoints/primary"
     - name: LAKEBASE_PROJECT
       value: "<project-name>"
   ```
   Your project name is `lakebase-branching-workshop-<username>` where `<username>` is your
   email with `.` replaced by `-` and `@domain.com` removed.
   (e.g., `john.doe@databricks.com` → `lakebase-branching-workshop-john-doe`)

3. **Create the app & add database resource** (do this before deploying):
   - Compute > Apps > Create App > name it `datacart-storefront`
   - Settings > Add Resource > Database > select your Lakebase project > Can connect > Save

4. **Deploy the app**:
   - UI: Compute > Apps > datacart-storefront > Deploy > set source path to `/Workspace/Users/<your-email>/datacart-storefront`
   - Or CLI: `databricks apps deploy datacart-storefront --source-code-path /Workspace/Users/<your-email>/datacart-storefront -p <profile>`
   - Or DABs: `databricks bundle deploy && databricks bundle run datacart_storefront`

5. **Run Lab 1.2** — Grants the app's service principal access to the ecommerce schema

6. **Open the storefront** and run through the labs!

## How the Storefront Works

The storefront **auto-detects schema changes** every 30 seconds by querying `information_schema`.
As each lab adds tables and columns to the production database, new features appear on the
storefront without any code changes or redeployment.

Key components:
- **`schema_detector.py`** — Queries `information_schema` with 30s cache, exposes feature flags
- **`/api/features`** — Returns boolean flags (reviews_active, loyalty_active, promotions_active, etc.)
- **Frontend** — Polls `/api/features` every 30s, conditionally renders UI elements

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Storefront shows "Loading..." | Check `<app-url>/api/dbtest`. If `PGHOST` is `NOT SET`, redeploy the app. |
| "Password authentication failed" | Re-add the Lakebase database resource in app settings, then redeploy. |
| Schema grants missing | Run Lab 1.2 or `GRANT ALL ON ALL TABLES IN SCHEMA ecommerce TO "<SP_CLIENT_ID>";` |
| Spring Sale Deals not showing | Re-run `GRANT ALL ON ALL TABLES` after the synced table is created (Lab 5.1 Step 7). |
| Orders page shows "Unavailable" | Expected during Lab 4.1 PITR disaster. Resolves after recovery. |

## Extra Setup Documentation (if needed)

- **WORKSHOP_SETUP.md** — Full step-by-step setup guide
- **APP_DEEP_DIVE.md** — Technical architecture, API reference, design system

## Databricks Documentation

- [Lakebase Overview](https://docs.databricks.com/aws/en/oltp/)
- [Manage Branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)
- [Point-in-Time Recovery](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore)
- [Connect to Your Database](https://docs.databricks.com/aws/en/oltp/projects/connect)
- [Postgres Roles](https://docs.databricks.com/aws/en/oltp/projects/postgres-roles)
- [API Reference](https://docs.databricks.com/api/workspace/postgres)
