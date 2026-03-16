# Lakebase In-a-Box Workshop

This is a hands-on workshop for learning about **Databricks Lakebase** — a fully managed, serverless PostgreSQL database built on open architecture that decouples compute from storage.

You'll work through the workshop as a developer at **DataCart**, a rapidly growing e-commerce platform preparing for a major Spring Sale launch.

## What You'll Learn

| Topic | Description |
|-------|-------------|
| **Project Setup** | Provision and configure a Lakebase Autoscaling PostgreSQL project |
| **Roles & Permissions** | Manage workspace-level and database-level access control with OAuth and native Postgres roles |
| **Database Branching** | Create isolated, zero-copy environments for parallel development using copy-on-write storage |
| **Schema Diff** | Compare schemas across branches to validate changes before promotion |
| **Point-in-Time Recovery** | Recover from catastrophic errors (e.g. accidental `DROP TABLE`) to any second within a configurable window |
| **Snapshots** | Create named restore points before planned deployments and migrations |
| **Monitoring** | Interpret RAM, CPU, connections, deadlocks, cache hit rates, and query performance metrics |
| **Reverse ETL** | Sync curated Lakehouse data into Lakebase using Synced Tables (Snapshot, Triggered, Continuous modes) |

## Prerequisites

- A Databricks workspace with **Unity Catalog** enabled
- Permission to create **Lakebase Autoscaling Projects** and **catalogs**
- Access to a **Serverless SQL Warehouse** (2X-Small is sufficient)
- A Databricks cluster with **Python 3.10+** (for the lab notebooks)
- Workspace region must be one of: `us-east-1`, `us-east-2`, `eu-central-1`, `eu-west-1`, `eu-west-2`, `ap-south-1`, `ap-southeast-1`, `ap-southeast-2`

## Workshop Structure

The workshop is organized into sequential lectures and labs. Lectures provide conceptual background; labs give you hands-on practice.

### Core Modules

| # | Notebook | Type | Description |
|---|----------|------|-------------|
| 0 | `0 Workshop Introduction.py` | Lecture | Workshop overview, Lakebase architecture, and the DataCart scenario |
| 1 | `1 Lecture - Creating and Exploring a Lakebase Autoscaling Project.sql` | Lecture | Create a project, explore settings, connect to your database, create tables, and query PostgreSQL system metadata |
| 1.1 | `1.1 Lab Setup Project.py` | Lab | Automated project creation, OAuth connection, and e-commerce schema seeding (customers, products, orders) |
| 2 | `2 Lecture - Roles and Permissions.sql` | Lecture | Workspace vs. database permission layers, OAuth roles, native Postgres roles, and GRANT/REVOKE workflows |
| 3 | `3 Lecture - Database Branching.ipynb` | Lecture | Branching concepts, copy-on-write storage, branch strategies, Schema Diff, and branch lifecycle management |
| 3.1 | `3.1 Lab Create Branch - Data Only.py` | Lab | Create a dev branch from production, query and modify data in isolation, verify production is untouched |
| 3.2 | `3.2 Lab - Parallel Development.ipynb` | Lab | Three developers work in parallel on isolated branches (loyalty features, multi-currency support, performance indexes) |
| 3.3 | `3.3 Lab - Schema To Prod Migration.py` | Lab | Promote validated schema changes from a feature branch to production by replaying DDL |
| 3.4 | `3.4 Lab - Branch Reset.py` | Lab | Detect production drift, reset a branch to match parent state, and re-test migrations |
| 4 | `4 Lecture - Point in Time Restore & Snapshots.ipynb` | Lecture | PITR restore windows, snapshot scheduling, and when to use each |
| 4.1 | `4.1 Lab - PITR (Disaster Recovery).ipynb` | Lab | Simulate an accidental `DROP TABLE` and recover using Point-in-Time Recovery |

### Bonus Modules

| # | Notebook | Type | Description |
|---|----------|------|-------------|
| 5 | `5 Lecture - Monitoring (BONUS).ipynb` | Lecture | Metrics dashboards (RAM, CPU, connections, deadlocks, cache hit rate), active queries, query performance, and system operations |
| 6 | `6 Lecture Reverse ETL (BONUS).ipynb` | Lecture | Synced Tables, sync modes (Snapshot/Triggered/Continuous), data type mappings, and best practices |
| 7 | `7 (STILL WORKING ON) Lecture - Connect Apps to Lakebase (Bonus).py` | Lecture | Connecting external applications to Lakebase (work in progress) |

### Utilities

| Notebook | Description |
|----------|-------------|
| `99_Cleanup.py` | Deletes all non-default branches and optionally the entire project |
| `Includes/Classroom-Setup-Common.py` | Shared setup logic for catalog provisioning across lab environments |

## Getting Started

1. **Import the repo** into your Databricks workspace (Repos > Add Repo)
2. **Start with `0 Workshop Introduction.py`** to understand the scenario and architecture
3. **Run `1.1 Lab Setup Project.py`** to provision your Lakebase project and seed the e-commerce data
4. **Follow the numbered notebooks** in sequence — each builds on the previous one
5. **Run `99_Cleanup.py`** when you're done to delete your resources and avoid unnecessary costs

> **Important:** Each workspace supports a maximum of 1,000 Lakebase projects. Always clean up when you're finished.

## Documentation

- [Lakebase Overview](https://docs.databricks.com/aws/en/oltp/)
- [Manage Branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)
- [Point-in-Time Recovery](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore)
- [Connect to Your Database](https://docs.databricks.com/aws/en/oltp/projects/connect)
- [Postgres Roles](https://docs.databricks.com/aws/en/oltp/projects/postgres-roles)
- [API Reference](https://docs.databricks.com/api/workspace/postgres)
