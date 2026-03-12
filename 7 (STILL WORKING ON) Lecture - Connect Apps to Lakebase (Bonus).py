# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Connect External and Databricks Apps to Lakebase
# MAGIC
# MAGIC This notebook explores the different ways of integrating applications with Lakebase. We will discuss the follwoing topics listed below:
# MAGIC
# MAGIC 1. **Lakebase Data API** — A RESTful interface for direct database interaction
# MAGIC 2. **Connecting External Apps** — Using the Databricks SDK or REST API
# MAGIC 3. **Connecting Databricks Apps** — Native integration with automatic credential rotation

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Lakebase Data API

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC ![lakebase_data_api.png](Includes/images/integration/lakebase_data_api.png)
# MAGIC
# MAGIC The **Lakebase Data API** is a [PostgREST](https://postgrest.org)-compatible RESTful interface that lets you interact directly with your Lakebase Postgres database using standard HTTP methods (GET, POST, PATCH, DELETE). It **automatically generates endpoints** based on your database schema, enabling full CRUD operations **without writing custom backend code**.
# MAGIC
# MAGIC | Capability | HTTP Method | Description |
# MAGIC |---|---|---|
# MAGIC | Query data | `GET` | Flexible filtering, sorting, pagination |
# MAGIC | Insert records | `POST` | Add new rows |
# MAGIC | Update records | `PATCH` / `PUT` | Modify existing rows |
# MAGIC | Delete records | `DELETE` | Remove rows |
# MAGIC | Execute functions | `POST` (RPC) | Call stored functions |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - A **Lakebase Postgres Autoscaling** database project
# MAGIC - Workspace in a supported region: `us-east-1`, `us-east-2`, `us-west-2`, `ca-central-1`, `sa-east-1`, `eu-central-1`, `eu-west-1`, `eu-west-2`, `ap-south-1`, `ap-southeast-1`, `ap-southeast-2`
# MAGIC - **Databricks OAuth bearer token** for authentication
# MAGIC - The Data API must be **enabled** from your project's Data API page

# COMMAND ----------

# MAGIC %md
# MAGIC ## Major Use Cases
# MAGIC
# MAGIC The Data API is ideal for scenarios where you need **direct, lightweight database access over HTTP**:
# MAGIC
# MAGIC | Use Case | Why Data API? |
# MAGIC |---|---|
# MAGIC | **Web Applications** | Frontend apps can query Lakebase directly via REST — no backend needed |
# MAGIC | **Microservices** | Each service gets its own REST endpoints auto-generated from the schema |
# MAGIC | **Serverless Architectures** | Stateless HTTP calls fit naturally into Lambda/Cloud Functions |
# MAGIC | **Mobile Applications** | Standard HTTP makes it easy to integrate with iOS/Android clients |
# MAGIC | **Third-party Integrations** | Any system that can make HTTP requests can access your data |

# COMMAND ----------

# MAGIC %md
# MAGIC ## How It Supports Row-Level Security (RLS)
# MAGIC
# MAGIC Row-Level Security provides **fine-grained access control** by restricting which rows a user can see or modify. Because the Data API authenticates via Databricks OAuth, each request maps to a specific Postgres role — and RLS policies are enforced per-role.
# MAGIC
# MAGIC ### Enabling RLS on a Table
# MAGIC ```sql
# MAGIC ALTER TABLE clients ENABLE ROW LEVEL SECURITY;
# MAGIC ```
# MAGIC
# MAGIC ### Creating Policies
# MAGIC ```sql
# MAGIC -- User-specific access: Alice sees clients 1-2, Bob sees clients 2-3
# MAGIC CREATE POLICY alice_clients ON clients
# MAGIC   TO "alice@databricks.com"
# MAGIC   USING (id IN (1, 2));
# MAGIC
# MAGIC CREATE POLICY bob_clients ON clients
# MAGIC   TO "bob@databricks.com"
# MAGIC   USING (id IN (2, 3));
# MAGIC ```
# MAGIC
# MAGIC ### Common RLS Patterns
# MAGIC
# MAGIC | Pattern | Policy Example |
# MAGIC |---|---|
# MAGIC | **User ownership** | `USING (assigned_to = current_user)` — users only see their own rows |
# MAGIC | **Tenant isolation** | `USING (tenant_id = (SELECT tenant_id FROM user_tenants WHERE user_email = current_user))` |
# MAGIC | **Role-based access** | `USING (status = 'pending' OR pg_has_role(current_user, 'managers', 'member'))` |
# MAGIC
# MAGIC > **Key takeaway:** RLS + Data API means you can expose a single REST endpoint, and each user automatically sees only the data they're authorized to access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Connect External Apps to Lakebase

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option A: Using the Databricks SDK
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC For applications written in **Python, Java, or Go**, the Databricks SDK provides the simplest path to connect to Lakebase. You use standard Postgres drivers (`psycopg`, `pgx`, `JDBC`) and the SDK handles **OAuth token rotation automatically** — no manual credential management required.
# MAGIC
# MAGIC The SDK's `generate_database_credential()` method:
# MAGIC 1. Obtains a workspace OAuth token
# MAGIC 2. Exchanges it for a database credential
# MAGIC 3. Returns the credential as a Postgres password
# MAGIC
# MAGIC Both tokens expire after **60 minutes**, but connection pools handle automatic refresh seamlessly.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prerequisites
# MAGIC
# MAGIC - **Databricks SDK**: Python `v0.89.0+`, Java `v0.73.0+`, or Go `v0.109.0+`
# MAGIC - A **Lakebase Autoscaling** project with an available endpoint
# MAGIC - A **Service Principal** with an OAuth secret configured
# MAGIC - Workspace access enabled for the service principal

# COMMAND ----------

# MAGIC %md
# MAGIC ### How It Works
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐      ┌─────────────────────┐     ┌──────────────────┐
# MAGIC │  Your App    │      │  Databricks SDK     │     │  Lakebase        │
# MAGIC │              │      │                     │     │  (Postgres)      │
# MAGIC │  1. Call     │─────>│  2. Get workspace   │     │                  │
# MAGIC │  generate_   │      │     OAuth token     │     │                  │
# MAGIC │  database_   │      │  3. Exchange for    │     │                  │
# MAGIC │  credential()│<─────│     DB credential   │     │                  │
# MAGIC │              │      │                     │     │                  │
# MAGIC │  4. Connect  │──────┼─────────────────────┼────>│  5.Authenticated │
# MAGIC │  with token  │      │                     │     │     connection   │
# MAGIC │  as password │<─────┼─────────────────────┼─────│     established  │
# MAGIC └──────────────┘      └─────────────────────┘     └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC > **Token lifecycle:** Service Principal secret (up to 730 days) → Workspace OAuth token (60 min) → Database credential (60 min). Connection pools refresh tokens automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps
# MAGIC
# MAGIC **Step 1: Create a Service Principal with OAuth Secret**
# MAGIC - Create a service principal with an OAuth secret (up to 730-day lifetime)
# MAGIC - Enable "Workspace access" under Settings → Identity and access → Service principals
# MAGIC - Record the **client ID** (UUID format)
# MAGIC
# MAGIC **Step 2: Create the Postgres Role**
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```sql
# MAGIC CREATE EXTENSION IF NOT EXISTS databricks_auth;
# MAGIC SELECT databricks_create_role('{client-id}', 'SERVICE_PRINCIPAL');
# MAGIC GRANT CONNECT ON DATABASE databricks_postgres TO "{client-id}";
# MAGIC GRANT CREATE, USAGE ON SCHEMA public TO "{client-id}";
# MAGIC GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{client-id}";
# MAGIC GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO "{client-id}";
# MAGIC ```
# MAGIC
# MAGIC **Step 3: Gather Connection Details** from the Lakebase Console:
# MAGIC - `host` — endpoint domain
# MAGIC - `database` — typically `databricks_postgres`
# MAGIC - `endpoint name` — format: `projects/<project-id>/branches/<branch-id>/endpoints/<endpoint-id>`
# MAGIC
# MAGIC **Step 4: Set Environment Variables**
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```bash
# MAGIC export DATABRICKS_HOST="https://your-workspace.databricks.com"
# MAGIC export DATABRICKS_CLIENT_ID="<service-principal-client-id>"
# MAGIC export DATABRICKS_CLIENT_SECRET="<your-oauth-secret>"
# MAGIC export ENDPOINT_NAME="projects/<id>/branches/<id>/endpoints/<id>"
# MAGIC export PGHOST="<endpoint-id>.database.<region>.cloud.databricks.com"
# MAGIC export PGDATABASE="databricks_postgres"
# MAGIC export PGUSER="<service-principal-client-id>"
# MAGIC export PGPORT="5432"
# MAGIC export PGSSLMODE="require"
# MAGIC ```
# MAGIC
# MAGIC **Step 5: Connect with Python**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Troubleshooting Best Practices
# MAGIC
# MAGIC | Error | Resolution |
# MAGIC |---|---|
# MAGIC | `API is disabled for users without workspace-access entitlement` | Enable **Workspace access** for the service principal in Settings |
# MAGIC | `Role does not exist` | Create the OAuth role via SQL editor using `databricks_create_role()` — not the UI |
# MAGIC | `Connection refused` | Verify the `ENDPOINT_NAME` format and that the endpoint ID is correct |
# MAGIC | `Invalid user` | Use the service principal **client ID (UUID)**, not the display name |
# MAGIC | Slow first connection | Normal — compute may be starting from zero after idle. Implement retry logic. |
# MAGIC | Token expiration issues | Set connection pool `max_lifetime` to **45 minutes** (before the 60-min token expiry) |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary (SDK Approach)
# MAGIC
# MAGIC - The **Databricks SDK** abstracts away the two-step OAuth token exchange
# MAGIC - Use a **custom connection class** (e.g., `OAuthConnection`) to inject fresh tokens per connection
# MAGIC - **Connection pools** handle automatic token refresh — set max lifetime to 45 minutes
# MAGIC - Supported languages: **Python** (psycopg3), **Java** (JDBC/HikariCP), **Go** (pgx)
# MAGIC - For Node.js, Ruby, PHP, or other languages → use the REST API approach below

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Option B: Using the REST API (Manual Token Exchange)
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC For applications in languages **without Databricks SDK support** (Node.js, Ruby, PHP, Rust, Elixir), you can connect to Lakebase by making direct REST API calls to obtain database credentials. This requires a **two-step token exchange** that you manage manually.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prerequisites
# MAGIC
# MAGIC - A **Service Principal** with OAuth secret (up to 730-day lifetime)
# MAGIC - **Workspace access** enabled for the service principal
# MAGIC - A **Postgres role** created in the Lakebase SQL Editor
# MAGIC - Connection details: endpoint name, host, database

# COMMAND ----------

# MAGIC %md
# MAGIC ### How It Works
# MAGIC
# MAGIC The manual approach requires **two sequential HTTP calls** to obtain a database password:
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐         ┌─────────────────────┐         ┌──────────────────┐
# MAGIC │  Your App    │         │  Databricks APIs     │        │  Lakebase        │
# MAGIC │              │         │                      │        │  (Postgres)      │
# MAGIC │  Step 1:     │────────>│  POST /oidc/v1/token │        │                  │
# MAGIC │  Exchange    │         │  (Basic Auth with    │        │                  │
# MAGIC │  SP secret   │<────────│   client_id:secret)  │        │                  │
# MAGIC │  for OAuth   │         │  Returns: workspace  │        │                  │
# MAGIC │  token       │         │  OAuth token (60min) │        │                  │
# MAGIC │              │         │                      │        │                  │
# MAGIC │  Step 2:     │────────>│  POST /api/2.0/      │        │                  │
# MAGIC │  Exchange    │         │  postgres/credentials │       │                  │
# MAGIC │  OAuth token │<────────│  (Bearer token)      │        │                  │
# MAGIC │  for DB cred │         │  Returns: DB token   │        │                  │
# MAGIC │              │         │  (60min)                      │                  │
# MAGIC │              │         │                      │        │                  │
# MAGIC │  Step 3:     │─────────┼──────────────────────┼───────>│  Connect with    │
# MAGIC │  Connect     │         │                      │        │  DB token as     │
# MAGIC │  to Postgres │<────────┼──────────────────────┼────────│  password        │
# MAGIC └──────────────┘         └─────────────────────┘         └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC > **Token lifetimes:** Service Principal secret (up to 730 days) → Workspace OAuth token (60 min) → Database credential (60 min). Cache both tokens and refresh before expiry.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps
# MAGIC
# MAGIC **Step 1: Get a Workspace OAuth Token**
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```bash
# MAGIC curl -X POST "${DATABRICKS_HOST}/oidc/v1/token" \
# MAGIC   -H "Authorization: Basic $(echo -n "${DATABRICKS_CLIENT_ID}:${DATABRICKS_CLIENT_SECRET}" | base64)" \
# MAGIC   -H "Content-Type: application/x-www-form-urlencoded" \
# MAGIC   -d "grant_type=client_credentials&scope=all-apis"
# MAGIC ```
# MAGIC
# MAGIC **Step 2: Exchange for a Database Credential**
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```bash
# MAGIC curl -X POST "${DATABRICKS_HOST}/api/2.0/postgres/credentials" \
# MAGIC   -H "Authorization: Bearer ${WORKSPACE_TOKEN}" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d "{\"endpoint\": \"${ENDPOINT_NAME}\"}"
# MAGIC ```
# MAGIC
# MAGIC **Step 3: Connect to Postgres** using the returned token as the password
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```bash
# MAGIC PGPASSWORD="${DB_TOKEN}" psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary (REST API Approach)
# MAGIC
# MAGIC - Use when the **Databricks SDK is not available** for your language
# MAGIC - Requires **two sequential API calls**: SP secret → workspace token → DB credential
# MAGIC - **Cache both tokens** and refresh before the 60-minute expiry (recommend 5 min early)
# MAGIC - The `password` parameter in most Postgres drivers accepts an async function for automatic refresh
# MAGIC - Implement **retry logic** for first connections (compute may be starting from zero)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Connect a Databricks App to Lakebase

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC **Databricks Apps** provide a native way to build and deploy web applications directly within your workspace. Connecting a Databricks App to Lakebase is streamlined because the app **runs as its own service principal** — the same OAuth token rotation pattern applies, but configuration is simpler since the app is already inside the Databricks environment.
# MAGIC
# MAGIC The example below uses **Flask**, but the pattern works with any Python web framework.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Databricks workspace with **Lakebase Postgres Autoscaling** enabled
# MAGIC - Permission to **create Databricks Apps**
# MAGIC - Python 3.9+
# MAGIC - Databricks CLI installed
# MAGIC - Basic Python and SQL knowledge

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Does It Work?
# MAGIC
# MAGIC When deployed, a Databricks App runs as its **own service principal**. The app uses the Databricks SDK's `WorkspaceClient()` (no explicit credentials needed — they're injected by the platform) to generate fresh database tokens via `generate_database_credential()`.
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────────────┐
# MAGIC │                     Databricks Workspace                          │
# MAGIC │                                                                    │
# MAGIC │  ┌──────────────┐     ┌─────────────────┐     ┌───────────────┐  │
# MAGIC │  │ Databricks   │     │ WorkspaceClient  │     │  Lakebase     │  │
# MAGIC │  │ App (Flask)  │────>│ (auto-injected   │────>│  Postgres     │  │
# MAGIC │  │              │     │  credentials)    │     │  Database     │  │
# MAGIC │  │ Runs as its  │     │                  │     │               │  │
# MAGIC │  │ own Service  │<────│ generate_database│<────│  Authenticated│  │
# MAGIC │  │ Principal    │     │ _credential()    │     │  connection   │  │
# MAGIC │  └──────────────┘     └─────────────────┘     └───────────────┘  │
# MAGIC │                                                                    │
# MAGIC │  Key: No explicit credentials needed — the platform handles it    │
# MAGIC └────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **During local testing**, the app runs under your user account (via `databricks auth login`). **When deployed**, it runs as its service principal. The token rotation code is identical in both cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps to Get It Working
# MAGIC
# MAGIC ### Step 1: Create the Databricks App
# MAGIC - Create a new Databricks App using the **Flask Hello World** template
# MAGIC - Note the app's `DATABRICKS_CLIENT_ID` (UUID) from the **Environment tab** — this is the Postgres username
# MAGIC
# MAGIC ### Step 2: Create the Lakebase Project
# MAGIC - Create a **Lakebase Autoscaling** project
# MAGIC - Wait ~1 minute for compute to activate
# MAGIC
# MAGIC ### Step 3: Configure Authentication (in Lakebase SQL Editor)
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```sql
# MAGIC -- Enable OAuth auth extension
# MAGIC CREATE EXTENSION IF NOT EXISTS databricks_auth;
# MAGIC
# MAGIC -- Create a role for the app's service principal
# MAGIC SELECT databricks_create_role('<DATABRICKS_CLIENT_ID>', 'service_principal');
# MAGIC
# MAGIC -- Grant permissions
# MAGIC GRANT CONNECT ON DATABASE databricks_postgres TO "<DATABRICKS_CLIENT_ID>";
# MAGIC GRANT CREATE, USAGE ON SCHEMA public TO "<DATABRICKS_CLIENT_ID>";
# MAGIC ```
# MAGIC
# MAGIC ### Step 4: Create Sample Data
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE notes (
# MAGIC     id SERIAL PRIMARY KEY,
# MAGIC     content TEXT NOT NULL,
# MAGIC     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE notes TO "<DATABRICKS_CLIENT_ID>";
# MAGIC
# MAGIC INSERT INTO notes (content) VALUES
# MAGIC    ('Welcome to Lakebase Autoscaling!'),
# MAGIC    ('This app connects to Postgres'),
# MAGIC    ('Data fetched from your database');
# MAGIC ```
# MAGIC
# MAGIC ### Step 5: Configure `app.yaml`
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```yaml
# MAGIC command: ['flask', '--app', 'app.py', 'run', '--host', '0.0.0.0', '--port', '8000']
# MAGIC env:
# MAGIC   - name: PGHOST
# MAGIC     value: '<your-endpoint-hostname>'
# MAGIC   - name: PGDATABASE
# MAGIC     value: 'databricks_postgres'
# MAGIC   - name: PGUSER
# MAGIC     value: '<DATABRICKS_CLIENT_ID>'
# MAGIC   - name: PGPORT
# MAGIC     value: '5432'
# MAGIC   - name: PGSSLMODE
# MAGIC     value: 'require'
# MAGIC   - name: ENDPOINT_NAME
# MAGIC     value: 'projects/<project-id>/branches/<branch-id>/endpoints/<endpoint-id>'
# MAGIC ```
# MAGIC
# MAGIC ### Step 6: Write the App (`app.py`)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Test Locally
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```bash
# MAGIC databricks auth login
# MAGIC export PGHOST="<endpoint-hostname>"
# MAGIC export PGDATABASE="databricks_postgres"
# MAGIC export PGUSER="your.email@company.com"   # Use your email for local testing
# MAGIC export PGPORT="5432"
# MAGIC export PGSSLMODE="require"
# MAGIC export ENDPOINT_NAME="<endpoint-name>"
# MAGIC pip3 install --upgrade -r requirements.txt
# MAGIC python3 app.py
# MAGIC ```
# MAGIC
# MAGIC ### Step 8: Deploy
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```bash
# MAGIC databricks sync . /Workspace/Users/<your-email>/my-lakebase-app
# MAGIC databricks apps deploy <app-name> --source-code-path /Workspace/Users/<your-email>/my-lakebase-app
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC | Topic | Key Point |
# MAGIC |---|---|
# MAGIC | **Data API** | PostgREST-compatible REST interface — auto-generated CRUD endpoints, supports RLS for fine-grained access |
# MAGIC | **External Apps (SDK)** | Use `generate_database_credential()` with connection pools for automatic token rotation (Python, Java, Go) |
# MAGIC | **External Apps (REST)** | Two-step token exchange for languages without SDK support — cache tokens, refresh before 60-min expiry |
# MAGIC | **Databricks Apps** | Simplest path — platform injects credentials automatically, same `OAuthConnection` pattern, deploy with CLI |
# MAGIC | **Authentication** | Always via Databricks OAuth → mapped to Postgres roles. Service principal secrets last up to 730 days. |
# MAGIC | **Security** | Enable RLS, grant minimal permissions, use separate roles per app, never use DB owner for API access |
