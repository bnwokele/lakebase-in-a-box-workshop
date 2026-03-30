# Lakebase-in-a-Box Workshop

This hands-on workshop introduces Databricks Lakebase — a fully managed, serverless PostgreSQL database built on open architecture that decouples compute from storage and demonstrates how to leverage its unique capabilities to build and maintain production-grade applications with unprecedented agility.

You will step into the role of a developer at DataCart, a rapidly growing e-commerce platform. The stakes are high: the "Spring Sale" launch is weeks away, and you and your team needs to roll out international currency support, a new loyalty program, and performance optimizations—all while ensuring the production site stays bulletproof.

## Core Modules

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


## DataCart Storefront App 

A customer-facing e-commerce web application (React + FastAPI) that **evolves in real time** as each lab modifies the database. Located in `datacart-storefront/`.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│              DataCart Storefront App                  │
│  ┌─────────────┐        ┌────────────────────────┐  │
│  │ React UI    │  HTTP  │  FastAPI Backend        │  │
│  │ (Vite SPA)  │───────▶│  /api/shop/*           │  │
│  │             │        │  /api/cart/*            │  │
│  │ - Home      │        │  /api/orders/*          │  │
│  │ - Shop      │        └───────────┬────────────┘  │
│  │ - Product   │                    │ psycopg3       │
│  │ - Cart      │                    │ OAuth tokens   │
│  │ - Orders    │                    ▼                │
│  └─────────────┘        ┌────────────────────────┐  │
│                         │  Lakebase (PostgreSQL)  │  │
│                         │  ecommerce schema       │  │
│                         │  production branch      │  │
│                         └────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

| Feature | Appears After |
|---------|--------------|
| Products, stock badges, cart, orders | Lab 1.1 |
| Star ratings, reviews | Lab 3.3 |
| Loyalty tier badge, points, "Earn X pts" | Lab 3.3 |
| Priority badges, verified badge | Lab 3.4 |
| Graceful degradation during disaster | Lab 4.1 |
| Sale badges, discount prices, promo deals | Lab 5.1 |

### Prerequisites

- Databricks workspace with Lakebase & Databricks Apps support

## Setup Steps

### Step 1: Create the Lakebase Project & Seed Data

Run notebook **`1.1 Lab Setup Project.py`** in the Databricks workspace. This:
1. Creates the Lakebase Autoscaling project (`lakebase-branching-workshop-<username>`)
2. Seeds 5 tables: customers, products, inventory, orders, order_items
3. Outputs the endpoint hostname and project name

Note the **project name** from the output — you'll need it for the app configuration.

**How to find your project name:** The project name follows the pattern
`lakebase-branching-workshop-<username>`, where `<username>` is your Databricks email
with `.` replaced by `-` and `@domain.com` removed. For example:
- Email: `john.doe@databricks.com` → Project: `lakebase-branching-workshop-john-doe`
- Email: `alice.smith@company.com` → Project: `lakebase-branching-workshop-alice-smith`

You can also find it in the **Lakebase UI** (left sidebar > Lakebase) then look for the project that was just created by running the first lab

### Step 2: Update app.yaml

Edit `app.yaml` with the correct endpoint and project name from Step 1:

```yaml
command:
  - "python"
  - "-m"
  - "uvicorn"
  - "app:app"
  - "--host"
  - "0.0.0.0"
  - "--port"
  - "8000"

env:
  - name: ENDPOINT_NAME
    value: "projects/<project-name>/branches/production/endpoints/primary"
  - name: LAKEBASE_PROJECT
    value: "<project-name>"
  - name: DB_SCHEMA
    value: "ecommerce"

resources:
  - name: postgres
    type: postgres
```

> **Note**: Do NOT hardcode `PGHOST`, `PGUSER`, or `PGDATABASE`. These are auto-injected
> by the Databricks Apps runtime when you add the Lakebase database as an app resource (Step 3).

### Step 3: Create the App & Add Lakebase Resource

Create the app and add the Lakebase database as a resource **before** deploying with source
code. This ensures the connection environment variables (`PGHOST`, `PGUSER`, `PGDATABASE`,
`PGPORT`) are injected on the first deploy.

1. Navigate to **Compute > Apps** in your Databricks workspace
2. Click **Create App**
3. Fill in:
   - **Name**: `datacart-storefront`
   - **Description**: `DataCart E-Commerce Storefront - Lakebase Branching Workshop`
4. Click **Next: Configure**
5. Click **Add Resource**
6. Select **Database** as the resource type
7. Choose the Lakebase project (`lakebase-branching-workshop-<username>`)
8. Grant **Can connect** permission
9. Save

> **Why this matters**: Databricks Apps run as a service principal (SP). The SP needs a
> Postgres role in Lakebase to authenticate. Adding the database as a resource handles
> role creation and credential injection automatically. Manual `CREATE ROLE` does not
> work because the role must be registered through the Lakebase OAuth system.
>
> **Important**: By adding the resource before the first deploy in Step 4, the `PGHOST`,
> `PGUSER`, and `PGDATABASE` env vars are injected immediately — no second redeploy needed.
> If you deploy first and add the resource after, you'll need to redeploy again for the
> env vars to take effect.

### Step 4: Deploy the App

Now deploy with the source code. Choose one of the options below.

#### Deploy via the Databricks UI

1. Go to **Compute > Apps > datacart-storefront**
2. Click the **Deploy** button
3. Set the **Source code path** to: `/Workspace/Users/<your-email>/datacart-storefront`
4. Click **Deploy**

### Step 5: Go through the rest of the Workshop!

Initially you will see that the service principal doesn't have access to the tables in lakebase so you will see an error. Run notebook **`2.1 Lab - Connect Storefront to Lakebase`** to connect the two. Once this has been done, you will see the store populate in the UI. Now you can go through the rest of the workshop!

## Workshop Flow — Storefront Evolution

The storefront **auto-detects schema changes** every 30 seconds. No redeployment is needed — just
run the lab and refresh the browser.

### After Lab 1.1 — Setup

**Database:** 5 tables (customers, products, inventory, orders, order_items). No reviews yet.

**Storefront shows:**
- Products with prices and stock badges (In Stock / Low Stock / Out of Stock)
- Shopping cart with checkout
- Order history with status badges
- **No star ratings** — reviews table doesn't exist yet
- **No loyalty features** — loyalty tables don't exist yet

### After Lab 3.1 — Parallel Development

**Database:** No changes to production. Three feature branches are created:
- `dev-loyalty-reviews` — loyalty_points column, loyalty_members table, and **reviews table**
- `modify-orders` — exchange_rates table, currency FK migration
- `add-index` — price index on products

**Storefront shows:** No change — all work is on isolated branches.

### After Lab 3.2 — Schema to Prod Migration

**Database changes on production:**
- `customers` table gets `loyalty_points` column (backfilled from order history)
- `loyalty_members` table created (customers enrolled by tier: Bronze/Silver/Gold/Platinum)
- `reviews` table created and seeded with ~80 product reviews

**Storefront shows (new features appear!):**
- **Navbar** — Alice Smith's loyalty tier badge (e.g., "Gold") and points count
- **Homepage** — Purple "Loyalty Program Active!" banner below the hero
- **Homepage** — "Top Rated" section appears (now that reviews exist)
- **Product cards** — Star ratings and review counts appear
- **Product cards** — "Earn X pts" labels below prices
- **Product detail** — Full customer reviews section with stars and comments
- **Cart** — "You'll earn X loyalty points" summary with tier badge
- **Checkout** — Awards loyalty points after placing an order

### After Lab 3.3 — Branch Reset

**Database changes on production:**
- `customers` table gets `email_verified` BOOLEAN column (~1/3 verified)
- `orders` table gets `priority` VARCHAR column (high/medium/normal based on total)

**Storefront shows (more features appear!):**
- **Navbar** — Green "Verified" badge appears next to the loyalty tier
- **Orders page** — Each order now shows a priority badge (high = red, medium = amber, normal = gray)

### During Lab 4.1 — PITR (The Disaster)

**Database change:** `DROP TABLE orders CASCADE` — drops both `orders` and `order_items`.
Tables that **survive**: customers, products, inventory, reviews, loyalty_members.

**Storefront shows (graceful degradation):**

| Page | What Happens |
|------|-------------|
| **Homepage** | Top Rated still works. Best Sellers shows "temporarily unavailable" |
| **Shop** | Products still browsable with stock badges, ratings, and "Earn X pts" |
| **Product Detail** | Reviews still visible |
| **Cart** | Items still there, but checkout shows "temporarily unavailable" error |
| **Orders** | Full-page "Orders Service Unavailable" with "Continue Shopping" button |

> Key demo point: the storefront **degrades gracefully** — products are still browsable
> even though orders are gone. This is what real customers would experience.

### After Lab 4.1 — PITR Recovery

**Database change:** Orders table recreated from PITR branch, data restored.

**Storefront shows (recovery):**
- Orders page is back with full order history
- Best Sellers works again
- Checkout is functional again
- **Priority badges are gone** — PITR restored to a point before Lab 3.4

### After Lab 4.1 — Post-Recovery Migrations

**Database change:** Lab 3.4 migrations re-applied (email_verified + priority columns).

**Storefront shows (full restore):**
- Priority badges are back on the Orders page
- Verified badge is back in the navbar
- All features from the entire workshop are restored

> Key demo point: PITR recovers data to a point in time. Post-recovery, you re-apply
> any migrations that happened after the recovery point — just like replaying commits
> after a git reset.

### After Lab 5.1 — Reverse ETL with Synced Tables

**Database change:** A `promotions` Delta table is created in Unity Catalog
(`serverless_stable_339b90_catalog.ecommerce.promotions`) and synced to Lakebase
via a synced table pipeline. First synced to a `dev-promotions` branch for validation,
then promoted to the `production` branch. The synced table appears as
`promotions_synced_prod` (or `promotions`) in the `ecommerce` Postgres schema.

**Important — SP permissions for synced tables:** After the sync completes, you must
re-grant the app SP access to the new table. Synced tables are created by the Lakebase
sync pipeline (a different internal role), so `ALTER DEFAULT PRIVILEGES` from Lab 1.2
does **not** cover them. Lab 5.1 Step 7 handles this with:
```sql
GRANT ALL ON ALL TABLES IN SCHEMA ecommerce TO "<SP_CLIENT_ID>";
```

**Storefront shows (promotions go live!):**
- **Homepage** — New "Spring Sale Deals" section with promoted products
- **Product cards** — Red sale badges (e.g., "SPRING SALE -20%", "FLASH SALE -45%") on promoted products
- **Product cards** — Original prices crossed out with sale prices in red
- **Product detail** — Promotion alert showing badge, discount %, and sale price
- **Cart** — Promoted items show the discounted sale price

> Key demo point: The marketing team updated a Delta table in Unity Catalog. The synced
> table pipeline pushed the data to Lakebase. The storefront detected the new table and
> rendered promotions. **Zero application code changes required.**

> The storefront auto-detects both `promotions_synced_prod` and `promotions` table names.

### After Lab 5.1 — Update Promotions

**Database change:** New flash sale promotions added to the Delta table, re-synced.

**Storefront shows:**
- 3 new products with "FLASH SALE" badges
- Existing promotions updated (e.g., Product 1 discount increased from 20% to 35%)
- Changes appear within 30 seconds of sync completion

## Troubleshooting

### "Store Unavailable" error on homepage
- The Lakebase endpoint may be suspended (scale-to-zero). Wait 10-20 seconds and refresh.
- Check that the ecommerce schema exists (run `1.1 Lab Setup Project.py` first).

### "Loading..." forever
- Hit `<app-url>/api/dbtest` to check connectivity.
- If `PGHOST` shows `NOT SET`: the app was **not redeployed after adding the database resource**.
  Go to Compute > Apps > datacart-storefront > click **Deploy** > confirm and deploy.
  The env vars are only injected at deploy time, not when the resource is added.
- If `db_connected: false` with "password authentication failed": the database resource
  was not added (Step 3), or the SP role was not auto-created. Remove and re-add the resource,
  then **redeploy**.
- If `db_connected: true` with `schema_error`: the SP needs schema grants (Step 5 / Lab 1.2).

### 500 errors on product pages
- Check app logs at `<app-url>/logz`
- Verify the SP has PostgreSQL roles on the ecommerce schema (Step 5)

### Spring Sale Deals section not appearing (after Lab 5.1)
- Check `/api/features` — if `promotions_active` is `false`, the SP can't see the synced table.
- Re-run `GRANT ALL ON ALL TABLES IN SCHEMA ecommerce TO "<SP_CLIENT_ID>";` as the project owner
  (Lab 5.1 Step 7). Synced tables are created by the sync pipeline, not your user, so
  `ALTER DEFAULT PRIVILEGES` doesn't apply to them.
- The storefront checks for both `promotions_synced_prod` and `promotions` table names.

### Cart/checkout not working
- Cart is stored in-memory on the app server. It resets on deploy.
- Checkout requires sufficient inventory stock.

## Logs

Access application logs at: `<app-url>/logz`

## Databricks Documentation

- [Lakebase Overview](https://docs.databricks.com/aws/en/oltp/)
- [Manage Branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)
- [Point-in-Time Recovery](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore)
- [Connect to Your Database](https://docs.databricks.com/aws/en/oltp/projects/connect)
- [Postgres Roles](https://docs.databricks.com/aws/en/oltp/projects/postgres-roles)
- [API Reference](https://docs.databricks.com/api/workspace/postgres)
