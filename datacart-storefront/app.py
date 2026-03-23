import logging
import traceback
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    from server.db import pool
    try:
        logger.info("Opening connection pool...")
        pool.open(wait=False)
        logger.info("Connection pool opened (lazy mode).")
    except Exception as e:
        logger.error(f"Pool open failed: {e}\n{traceback.format_exc()}")
    yield
    try:
        pool.close()
    except Exception:
        pass


app = FastAPI(title="DataCart Storefront", lifespan=lifespan)

from server.routes import shop, cart, orders, account  # noqa: E402

app.include_router(shop.router, prefix="/api")
app.include_router(cart.router, prefix="/api")
app.include_router(orders.router, prefix="/api")
app.include_router(account.router, prefix="/api")


@app.get("/api/health")
def health():
    return {"status": "ok", "app": "DataCart Storefront"}


@app.get("/api/features")
def features():
    """Return feature flags based on current database schema."""
    from server.schema_detector import get_features
    return get_features()


@app.get("/api/dbtest")
def dbtest():
    """Debug endpoint to test DB connectivity."""
    from server.db import w, DB_SCHEMA
    import psycopg
    info = {
        "PGHOST": os.environ.get("PGHOST", "NOT SET"),
        "PGUSER": os.environ.get("PGUSER", "NOT SET"),
        "PGDATABASE": os.environ.get("PGDATABASE", "NOT SET"),
        "ENDPOINT_NAME": os.environ.get("ENDPOINT_NAME", "NOT SET"),
        "IS_APP": bool(os.environ.get("DATABRICKS_APP_NAME")),
    }
    try:
        endpoint_name = os.environ["ENDPOINT_NAME"]
        cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
        info["credential_generated"] = True
        info["credential_expires"] = str(cred.expire_time)
    except Exception as e:
        info["credential_generated"] = False
        info["credential_error"] = str(e)
        return info

    try:
        conn = psycopg.connect(
            dbname=os.environ.get("PGDATABASE", "databricks_postgres"),
            user=os.environ.get("PGUSER", ""),
            password=cred.token,
            host=os.environ.get("PGHOST", ""),
            port=int(os.environ.get("PGPORT", "5432")),
            sslmode=os.environ.get("PGSSLMODE", "require"),
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            info["db_connected"] = True
            try:
                cur.execute(f"SELECT count(*) FROM {DB_SCHEMA}.products")
                info["product_count"] = cur.fetchone()[0]
            except Exception as e:
                info["schema_error"] = str(e)
        conn.close()
    except Exception as e:
        info["db_connected"] = False
        info["db_error"] = str(e)
        info["db_error_type"] = type(e).__name__
    return info


# Serve React frontend
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.exists(frontend_dir):
    app.mount(
        "/assets",
        StaticFiles(directory=os.path.join(frontend_dir, "assets")),
        name="assets",
    )

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        return FileResponse(os.path.join(frontend_dir, "index.html"))
