# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakebase Workshop
# MAGIC
# MAGIC For decades, **databases** have been the backbone of software, yet while we've completely reinvented how applications are built, the underlying databases have changed very little since the 1980s — suffering from fragile and costly operations, clunky development experiences, and extreme vendor lock-in.
# MAGIC
# MAGIC **Lakebase** represents a new approach: an open database architecture that brings together the reliability of transactional systems with the scalability and cost efficiency of the data lake. At its core is a rethinking of how databases are built — decoupling compute from storage so that data lives in affordable cloud object storage using open formats, while the database engine operates independently on top. This design removes much of the overhead, rigidity, and lock-in that traditional databases have carried for decades.
# MAGIC
# MAGIC **Databricks Lakebase** delivers this architecture as a fully managed, serverless Postgres database. Compute resources scale up automatically to meet demand and scale back to zero when not in use, so you only pay for what you consume. This makes it well suited for variable workloads, developer sandboxes, and AI agents that need to spin up isolated environments on the fly.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```
# MAGIC                         ┌─────────────────────────────────────────────────────────────┐
# MAGIC                         │                        COMPUTE LAYER                        │
# MAGIC                         │  ┌─────────────┐   ┌─────────────┐   ┌─────────────────┐    │
# MAGIC                         │  │ Endpoint A  │   │ Endpoint B  │   │   Endpoint C    │    │
# MAGIC                         │  │ (read-write)│   │ (read-only) │   │  (read-write)   │    │
# MAGIC                         │  └──────┬──────┘   └──────┬──────┘   └────────┬────────┘    │
# MAGIC                         │         │                 │                   │             │
# MAGIC                         ├─────────┼─────────────────┼───────────────────┼─────────────┤
# MAGIC                         │         │         STORAGE LAYER               │             │
# MAGIC                         │  ┌──────▼─────────────────▼───────────────────▼────────┐    │
# MAGIC                         │  │            Shared Object Storage (S3 / ADLS)        │    │
# MAGIC                         │  │   Branch: production  │  Branch: dev-feature  │ ... │    │
# MAGIC                         │  └─────────────────────────────────────────────────────┘    │
# MAGIC                         └─────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### What this means in practice
# MAGIC
# MAGIC | Concept | Explanation |
# MAGIC |---------|-------------|
# MAGIC | **Compute (Endpoints)** | Each branch has one or more *endpoints* — PostgreSQL-compatible connection targets that process queries. Endpoints can scale up, scale down, or reach zero when idle, without touching your data. |
# MAGIC | **Storage (Branches)** | All branch data lives in object storage. Because storage is separate from compute, creating a branch is **zero-copy** — no data is physically duplicated. A branch only consumes extra storage as changes diverge from its source. |
# MAGIC | **Scale-to-Zero** | When no connections are active, the compute layer scales to zero, eliminating cost. The data remains safely in storage and the endpoint resumes instantly on the next connection. |
# MAGIC | **Independent Scaling** | You can attach multiple endpoints to a single branch (e.g. one read-write, one read-only for analytics). Each scales according to its own workload. |

# COMMAND ----------

# MAGIC %md
# MAGIC # DataCart: Modernizing E-Commerce Database Operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Challenge
# MAGIC
# MAGIC DataCart's engineering team needs to modernize their database schema to support international customers and a new loyalty program ahead of the Spring Sale. Lets take a look at how lakebase improves developer productivity to help the team meet their deadline.
# MAGIC
# MAGIC To support the launch **three developers** need to work in parallel without blocking each other or risking production stability:
# MAGIC
# MAGIC | Developer | Team | Task |
# MAGIC |---|---|---|
# MAGIC | Developer A | Loyalty Team | Add a new `loyalty_members` table and a `points_balance` column to the `users` table |
# MAGIC | Developer B | Global Team | Modify the `orders` table to change the `currency` column from a fixed string to a foreign key linked to a new `exchange_rates` table |
# MAGIC | Developer C | Performance Team | Create new indexes on the `products` table to handle the high-traffic surge expected during the sale |
# MAGIC
# MAGIC
# MAGIC #### The "Code Red" Disaster Scenario
# MAGIC
# MAGIC During the final Spring Sale deployment, a DevOps engineer accidentally executes `DROP TABLE inventory_main;` instead of dropping a temporary staging table. The production website immediately begins throwing 500 errors — customers cannot check stock levels or complete purchases, and every second of downtime means thousands of dollars in lost revenue.
# MAGIC
# MAGIC In a traditional database, the team would need to find the last nightly backup, provision a new instance, restore the data (which could take hours), and replay logs. With **Lakebase PITR**, the process to handle this is much smoother.
# MAGIC
# MAGIC ### This Workshop
# MAGIC
# MAGIC This workshop places you in the role of a database engineer at **DataCart**, a rapidly growing global e-commerce platform preparing for a major "Spring Sale" launch. You'll experience firsthand how Lakebase branching and PITR address real-world development and operational challenges.
# MAGIC
# MAGIC ### Key Learning Objectives
# MAGIC
# MAGIC | Topic | Description |
# MAGIC |---|---|
# MAGIC | **Branching** | Creating isolated environments for parallel schema evolution across multiple developer teams |
# MAGIC | **Point-in-Time Recovery** | Recovering from catastrophic human error without downtime using PITR |
# MAGIC | **Roles & Permissions** | Managing access control across branches to enforce governance |
