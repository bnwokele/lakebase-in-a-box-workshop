# DataCart Storefront - App Deep Dive

## What Is This App?

The DataCart Storefront is a **customer-facing e-commerce web application** that simulates a real online store. It's built as a Databricks App using React (frontend) and FastAPI (backend), connected to a Lakebase PostgreSQL database.

Unlike a typical admin dashboard, this app is designed from the **customer's perspective** — browsing products, checking stock, adding items to a cart, and placing orders. The storefront **auto-detects schema changes** as each workshop lab modifies the database, dynamically revealing new features without redeployment.

## Why a Storefront?

The workshop tells the story of DataCart, an e-commerce company. The key scenarios are:

1. **Parallel development** — Three developers modify the database schema on isolated branches
2. **Schema promotion** — Loyalty features and reviews are migrated to production
3. **Code Red disaster** — Someone drops the orders table in production
4. **Point-in-Time Recovery** — Lakebase PITR restores the dropped table

A customer-facing storefront makes these scenarios **visceral**:
- When orders are dropped, the storefront shows **"Orders Service Unavailable"** — just like real customers would see
- When PITR restores the data, the storefront **comes back to life**
- When the loyalty migration runs, tier badges and points **appear in real time**

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Frontend | React 18 + TypeScript | Single-page storefront UI (teal/emerald theme) |
| Icons | Lucide React | Category-specific icons (Monitor, Shirt, BookOpen, Lamp, Dumbbell) |
| Build | Vite | Fast builds, dev server with proxy |
| Backend | FastAPI (Python 3.12) | REST API with dynamic feature detection |
| Database | Lakebase (PostgreSQL 17) | Transactional data store |
| Auth | OAuth tokens via Databricks SDK + App Resource | Secure, auto-rotating DB credentials |
| Connection Pool | psycopg3 + psycopg_pool | Efficient connection management |
| Deployment | Databricks Apps + DABs | Serverless deployment with bundle support |

## Project Structure

```
datacart-storefront/
├── app.py                    # FastAPI entry point + /api/features + /api/dbtest
├── app.yaml                  # Databricks App config (resources, env vars)
├── databricks.yml            # DABs bundle config (dev + workshop targets)
├── resources/
│   └── datacart_storefront.app.yml  # DABs app resource definition
├── main.py                   # Import wrapper
├── pyproject.toml            # Python project config
├── requirements.txt          # Deployment dependencies
├── setup_sp_roles_notebook.py # Notebook to grant SP Postgres permissions
├── server/
│   ├── config.py             # Dual-mode auth (local vs deployed)
│   ├── db.py                 # Lakebase connection pool + OAuth
│   ├── schema_detector.py    # Dynamic feature detection (30s cached)
│   └── routes/
│       ├── shop.py           # Product catalog, search, featured
│       ├── cart.py           # Shopping cart (add, update, clear)
│       ├── orders.py         # Order history + checkout + PITR guards
│       └── account.py        # Customer profile + loyalty tier
└── frontend/
    ├── package.json          # Node dependencies
    ├── vite.config.ts        # Vite config with API proxy
    ├── tsconfig.json         # TypeScript config
    ├── index.html            # HTML entry point
    ├── src/
    │   ├── main.tsx          # React entry point
    │   ├── App.tsx           # All pages, components, feature polling
    │   └── index.css         # Teal/emerald theme + loyalty/priority styles
    └── dist/                 # Pre-built output (served in production)
```

## Dynamic Feature Detection

The storefront auto-detects database schema changes every 30 seconds using `server/schema_detector.py`. This module queries `information_schema` to discover what columns and tables exist, caches the result, and exposes feature flags via `/api/features`.

### How It Works

```
Lab runs DDL on production
        ↓ (schema changes)
schema_detector queries information_schema (30s cache)
        ↓
/api/features returns updated flags
        ↓ (frontend polls every 30s)
React UI conditionally renders new features
```

### Feature Flags

| Flag | True When | Storefront Effect |
|------|-----------|-------------------|
| `reviews_active` | `reviews` table exists | Star ratings, review comments appear |
| `loyalty_active` | `loyalty_members` table + `loyalty_points` column | Tier badge, points, "Earn X pts" labels, loyalty banner |
| `order_priority_active` | `priority` column on `orders` | Priority badges on order cards |
| `email_verified_active` | `email_verified` column on `customers` | Verified badge in navbar |
| `orders_available` | `orders` table exists | Orders page works vs disaster state |
| `order_items_available` | `order_items` table exists | Best sellers, checkout, order details |
| `promotions_active` | `promotions` table exists (synced from UC) | Sale badges, discount prices, "Spring Sale Deals" section |

## API Endpoints

### Shop (`/api/shop/`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/shop/products` | Browse catalog. Conditionally includes reviews + loyalty points. Params: `category`, `search`, `limit`, `offset` |
| GET | `/api/shop/products/{id}` | Product detail with reviews (if table exists) and stock info |
| GET | `/api/shop/featured` | Top-rated, best-selling, and Spring Sale deals (requires promotions synced table). Returns graceful degradation flags |

### Cart (`/api/cart/`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/cart` | Cart with stock checks. Includes loyalty points, sale prices from promotions when active |
| POST | `/api/cart/add` | Add item to cart. Body: `{product_id, quantity}` |
| POST | `/api/cart/update` | Update quantity (0 = remove). Body: `{product_id, quantity}` |
| POST | `/api/cart/clear` | Clear entire cart |

### Orders (`/api/orders/`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/orders` | Order history. Returns 503 if `orders` table missing (PITR disaster). Includes `priority` when column exists |
| GET | `/api/orders/{id}` | Order detail with line items. Guards both `orders` and `order_items` tables |
| POST | `/api/orders/checkout` | Place order. Awards loyalty points if active. Returns 503 if orders/order_items missing |

### Account (`/api/account/`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/account` | Demo customer profile. Includes `loyalty_points`, `loyalty_tier`, `email_verified` when columns/tables exist |

### Features & Debug

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/features` | Boolean feature flags based on current schema. Frontend polls every 30s |
| GET | `/api/health` | App health check |
| GET | `/api/dbtest` | Database connectivity diagnostic (bypasses pool) |

## Database Schema

The app starts with **5 tables** (Lab 1.1). Additional tables and columns are added as labs progress.

### Initial Tables (After Lab 1.1)

```
ecommerce.customers    ── 100 rows  (name, email, created_at)
ecommerce.products     ── 50 rows   (name, price, category)
ecommerce.inventory    ── 50 rows   (product_id, quantity, warehouse, reorder_level)
ecommerce.orders       ── 22 rows   (customer_id, product_id, quantity, total, currency, status)
ecommerce.order_items  ── ~55 rows  (order_id, product_id, quantity, unit_price, line_total)
```

### Tables Added During Labs

| Table/Column | Added In | Purpose |
|---|---|---|
| `reviews` table | Lab 3.3 | Star ratings and customer comments |
| `loyalty_points` column (customers) | Lab 3.3 | Points balance per customer |
| `loyalty_members` table | Lab 3.3 | Tier enrollment (Bronze/Silver/Gold/Platinum) |
| `exchange_rates` table | Lab 3.2 (branch only) | Currency conversion rates |
| `email_verified` column (customers) | Lab 3.4 | Email verification status |
| `priority` column (orders) | Lab 3.4 | Order priority (high/medium/normal) |
| `promotions` table (synced from UC) | Lab 5.1 | Sale badges, discount prices, Spring Sale deals |

### How Tables Map to the Storefront

| Table | Storefront Feature |
|-------|--------------------|
| `products` | Product catalog with category icons, search, price filters |
| `inventory` | Stock badges (In Stock / Low Stock / Out of Stock), checkout validation |
| `reviews` | Star ratings on product cards, review comments on detail page (appears after Lab 3.3) |
| `customers` | Demo customer identity (Alice Smith), loyalty points, email verified status |
| `orders` | Order history page, checkout. Priority badges after Lab 3.4 |
| `order_items` | Order detail line items, "Best Sellers" homepage section |
| `loyalty_members` | Tier badge in navbar, loyalty banner on homepage (appears after Lab 3.3) |
| `promotions` | Sale badges on product cards, strikethrough prices, "Spring Sale Deals" section (appears after Lab 5.1, synced from Unity Catalog) |

## Frontend Pages

### Home Page
- **Spring Sale hero banner** (teal gradient) with product/category count
- **Loyalty Program banner** (amber gradient) — appears after Lab 3.3
- **Spring Sale Deals** section — promoted products with sale badges and discount prices (appears after Lab 5.1)
- **Top Rated** products — appears after Lab 3.3 when reviews table exists
- **Best Sellers** — sorted by units sold. Shows "temporarily unavailable" during PITR disaster

### Shop Page
- Product grid with **category-specific icons and colored gradients**:
  - Electronics (Monitor / blue), Clothing (Shirt / purple), Books (BookOpen / amber), Home (Lamp / green), Sports (Dumbbell / red)
- Category pill filters and search
- Star ratings and review counts — appear after Lab 3.3
- "Earn X pts" labels — appear after Lab 3.3
- **Sale badges** (e.g., "SPRING SALE -20%") on promoted product cards — appear after Lab 5.1
- **Strikethrough prices** with sale prices for promoted products — appear after Lab 5.1
- "Add to Cart" button (disabled when out of stock)

### Product Detail Page
- Large category icon with colored gradient background
- Star ratings + review count — appear after Lab 3.3
- "Earn X loyalty points with this purchase" — appears after Lab 3.3
- **Promotion alert** with badge, discount %, and sale price — appears after Lab 5.1
- Customer reviews section with stars and comments
- Stock badge with warehouse location

### Cart Page
- Line items with quantity controls (+/-)
- Stock validation (highlights out-of-stock items in red)
- "You'll earn X loyalty points" summary — appears after Lab 3.3
- **Sale prices** for promoted items with original price shown — appears after Lab 5.1
- Checkout error messaging during PITR disaster

### Orders Page
- Order history with status badges (pending/confirmed/shipped/delivered/cancelled)
- **Priority badges** (high=red, medium=amber, normal=gray) — appear after Lab 3.4
- Full-page "Orders Service Unavailable" with "Continue Shopping" button during PITR disaster

### Navbar
- DataCart brand (teal)
- Navigation links (Home, Shop, Orders)
- **Loyalty tier badge** + points count (amber) — appears after Lab 3.3
- **Verified badge** (green) — appears after Lab 3.4
- Cart icon with item count badge

## Design System

### Color Palette

| Element | Color | Hex |
|---------|-------|-----|
| Primary (brand, buttons, links) | Teal | `#0d9488` |
| Primary hover | Dark teal | `#0f766e` |
| Primary light (active states) | Mint | `#f0fdfa` |
| Background | Warm off-white | `#f7faf9` |
| Borders | Soft sage | `#e0ebe7` |
| Text | Dark teal-black | `#1a2e2a` |
| Muted text | Sage | `#5f7c75` |
| Loyalty/rewards | Warm amber | `#b45309` |
| Loyalty banner | Amber gradient | `#d97706` → `#b45309` |
| Promo badges | Red gradient | `#e53e3e` → `#c53030` |
| Sale price | Red | `#c53030` |
| Strikethrough price | Muted | `var(--text-muted)` |
| Error/danger | Red | `#e53e3e` |

### Category Visual System

| Category | Icon | Gradient Background |
|----------|------|-------------------|
| Electronics | Monitor | Blue (`#ebf8ff` → `#bee3f8`) |
| Clothing | Shirt | Purple (`#faf5ff` → `#e9d8fd`) |
| Books | BookOpen | Amber (`#fffff0` → `#fefcbf`) |
| Home | Lamp | Green (`#f0fff4` → `#c6f6d5`) |
| Sports | Dumbbell | Red (`#fff5f5` → `#fed7d7`) |

## Authentication & Connection Pattern

### App Resource Auth (Recommended)

The app uses the **Databricks App Resource** pattern for Lakebase connectivity. When you
add the Lakebase project as a database resource in the app settings:

1. The runtime auto-injects `PGHOST`, `PGUSER`, `PGDATABASE`, and `PGPORT` environment variables
2. The service principal's Postgres role is auto-created through the Lakebase OAuth system
3. The app's `OAuthConnection` class generates fresh tokens via `generate_database_credential`

This is configured in `app.yaml`:
```yaml
resources:
  - name: postgres
    type: postgres
```

> **Important**: Do NOT manually create the SP's Postgres role with `CREATE ROLE`.
> Manually-created roles are not linked to the Lakebase OAuth authentication system and
> will fail with "password authentication failed". Always use the app resource mechanism
> to create the role automatically.

After adding the resource, you still need to grant the SP permissions on the `ecommerce`
schema (see `setup_sp_roles_notebook.py`).

### Dual-Mode Auth (`server/config.py`)

The app detects whether it's running inside Databricks Apps or locally:

```python
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

def get_workspace_client():
    if IS_DATABRICKS_APP:
        return WorkspaceClient()  # Auto-injected SP credentials
    else:
        return WorkspaceClient(profile="fe-vm-ben")  # CLI profile
```

### OAuth Connection Pool (`server/db.py`)

The `OAuthConnection` class generates a fresh OAuth token every time the pool creates a new connection:

```python
class OAuthConnection(psycopg.Connection):
    @classmethod
    def connect(cls, conninfo='', **kwargs):
        credential = w.postgres.generate_database_credential(endpoint=endpoint_name)
        kwargs['password'] = credential.token
        return super().connect(conninfo, **kwargs)
```

Key configuration:
- `max_lifetime=2700` (45 min) — recycles connections before the 1-hour OAuth token expires
- `connect_timeout=15` — prevents hanging connections if the endpoint is suspended
- `open=False` — pool opens lazily on first request, not at import time

## PITR Disaster Handling

When Lab 4.1 runs `DROP TABLE orders CASCADE`, both `orders` and `order_items` are dropped (CASCADE FK). The storefront handles this gracefully:

| Page | Behavior During Disaster |
|------|-------------------------|
| **Homepage** | Top Rated works (reviews survive). Best Sellers shows "temporarily unavailable" |
| **Shop** | Products still browsable with stock badges, ratings, and "Earn X pts" |
| **Product Detail** | Reviews still visible |
| **Cart** | Items visible but checkout shows "temporarily unavailable" error |
| **Orders** | Full-page "Orders Service Unavailable" with "Continue Shopping" button |

After PITR recovery, all features come back within 30 seconds (cache TTL). Priority badges may disappear since PITR restores the pre-Lab 3.4 schema. The post-recovery migration step in Lab 4.1 re-applies these columns.

### Tables That Survive the Disaster

| Table | Status | Why |
|-------|--------|-----|
| customers | Survives | No FK to orders |
| products | Survives | No FK to orders |
| inventory | Survives | No FK to orders |
| reviews | Survives | FK to products + customers only |
| loyalty_members | Survives | FK to customers only |
| orders | **DROPPED** | Directly dropped |
| order_items | **DROPPED** | FK to orders with ON DELETE CASCADE |
| promotions | Survives | Synced table, no FK to orders |

## Reverse ETL — Promotions via Synced Tables

Lab 5.1 demonstrates **Lakebase Synced Tables** (reverse ETL). A `promotions` Delta table
in Unity Catalog (`serverless_stable_339b90_catalog.ecommerce.promotions`) is synced to
the Lakebase `ecommerce` schema via a managed pipeline. The synced table appears in Postgres
as `promotions_synced_prod` (the name may vary depending on how it's created in the UI).

### Data Flow

```
Unity Catalog Delta Table          Lakebase Postgres                      Storefront
─────────────────────────          ─────────────────                      ──────────
ecommerce.promotions    ──sync──►  ecommerce.promotions_synced_prod ──►  Sale badges
(marketing team edits)             (read-only copy)                       Discount prices
                                                                          Promo deals section
```

### How It Works

1. Marketing team creates/updates the `promotions` Delta table in Unity Catalog
2. A Lakebase synced table pipeline copies the data to the `ecommerce` schema in Postgres
3. **Re-grant SP permissions** — synced tables are created by the sync pipeline (a different
   role), so `ALTER DEFAULT PRIVILEGES` from Lab 1.2 doesn't cover them. Run
   `GRANT ALL ON ALL TABLES IN SCHEMA ecommerce TO "<SP_CLIENT_ID>";` after the sync.
4. The storefront's `schema_detector` detects the table within 30 seconds via
   `get_promotions_table()`, which checks for `promotions_synced_prod` first, then `promotions`
5. The `shop.py` routes query the detected table to overlay sale badges and discount prices
6. The frontend renders `PromoBadge` components and `PriceDisplay` with strikethrough pricing

### Table Name Detection

The synced table name in Postgres may differ from the Delta table name. The storefront
handles this with `get_promotions_table()` in `schema_detector.py`:

```python
def get_promotions_table() -> str | None:
    if table_exists("promotions_synced_prod"):
        return "promotions_synced_prod"
    if table_exists("promotions"):
        return "promotions"
    return None
```

### Promotions Table Schema

```sql
id INT PRIMARY KEY          -- Promotion ID
product_id INT              -- FK to products
badge_text STRING           -- e.g., "SPRING SALE", "FLASH SALE", "CLEARANCE"
discount_pct DECIMAL(5,2)   -- Percentage off (e.g., 20.00)
sale_price DECIMAL(10,2)    -- Pre-computed discounted price
promo_type STRING           -- "percentage", "fixed", "bundle"
is_active BOOLEAN           -- Only active promos are shown
start_date TIMESTAMP        -- Promotion start
end_date TIMESTAMP          -- Promotion end
```

### Key Constraints

1. The Unity Catalog schema name **must match** the Lakebase Postgres schema name. Since the
   storefront reads from `ecommerce.*`, the Delta table must be in a UC schema named `ecommerce`
   (e.g., `serverless_stable_339b90_catalog.ecommerce.promotions`).

2. After each new synced table is created, **re-grant table permissions** to the app SP.
   Synced tables are created by the Lakebase sync pipeline (an internal role), not by your
   user account, so `ALTER DEFAULT PRIVILEGES` does not apply to them.

## Demo Customer

The app simulates a logged-in customer:
- **Name**: Alice Smith
- **Customer ID**: 1
- **Email**: alice.smith.0@example.com

All cart operations and order history are scoped to this customer. The cart is stored in-memory (resets on redeploy), while orders persist in the database. After Lab 3.3, Alice has a loyalty tier and points balance that updates when she places orders.

## Local Development

### Start the backend
```bash
cd datacart-storefront
export DATABRICKS_PROFILE=fe-vm-ben
uv run uvicorn app:app --reload --port 8000
```

### Start the frontend (with API proxy)
```bash
cd frontend
npm run dev  # Runs on port 5173, proxies /api to localhost:8000
```

### Build for deployment
```bash
cd frontend
npm run build  # Outputs to frontend/dist/
```

## Deployment

### Via DABs (Recommended)
```bash
cd datacart-storefront
databricks bundle validate
databricks bundle deploy
databricks bundle run datacart_storefront
```

### Via CLI
```bash
databricks sync . /Users/<email>/datacart-storefront \
  --exclude node_modules --exclude .venv --exclude __pycache__ \
  --exclude .git --exclude "frontend/src" --exclude "frontend/public" \
  --full -p fe-vm-ben

databricks apps deploy datacart-storefront \
  --source-code-path /Workspace/Users/<email>/datacart-storefront \
  -p fe-vm-ben
```

## Logs

Access application logs at: `<app-url>/logz`
