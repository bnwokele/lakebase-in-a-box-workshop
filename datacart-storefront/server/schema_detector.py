import time
import logging
from server.db import pool, DB_SCHEMA

logger = logging.getLogger(__name__)

_cache: dict[str, set[str]] = {}
_cache_ts: float = 0
CACHE_TTL = 30


def _refresh_cache():
    global _cache, _cache_ts
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name, column_name "
                "FROM information_schema.columns "
                "WHERE table_schema = %s "
                "ORDER BY table_name, ordinal_position",
                (DB_SCHEMA,),
            )
            tables: dict[str, set[str]] = {}
            for table_name, column_name in cur.fetchall():
                tables.setdefault(table_name, set()).add(column_name)
            _cache = tables
            _cache_ts = time.time()


def get_schema() -> dict[str, set[str]]:
    if time.time() - _cache_ts > CACHE_TTL:
        try:
            _refresh_cache()
        except Exception as e:
            logger.warning(f"Schema detection failed: {e}")
            if _cache:
                return _cache
            raise
    return _cache


def invalidate_cache():
    global _cache_ts
    _cache_ts = 0


def table_exists(table: str) -> bool:
    return table in get_schema()


def column_exists(table: str, column: str) -> bool:
    schema = get_schema()
    return table in schema and column in schema[table]


def get_features() -> dict:
    return {
        "reviews_active": table_exists("reviews"),
        "loyalty_active": table_exists("loyalty_members") and column_exists("customers", "loyalty_points"),
        "exchange_rates_active": table_exists("exchange_rates"),
        "order_priority_active": column_exists("orders", "priority") if table_exists("orders") else False,
        "email_verified_active": column_exists("customers", "email_verified"),
        "orders_available": table_exists("orders"),
        "order_items_available": table_exists("order_items"),
        "promotions_active": table_exists("promotions_synced_prod") or table_exists("promotions"),
    }


def get_promotions_table() -> str | None:
    """Return the actual promotions table name, checking synced name first."""
    if table_exists("promotions_synced_prod"):
        return "promotions_synced_prod"
    if table_exists("promotions"):
        return "promotions"
    return None
