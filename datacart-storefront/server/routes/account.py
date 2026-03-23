from fastapi import APIRouter
from server.db import pool, DB_SCHEMA
from server.schema_detector import column_exists, table_exists
from server.routes.cart import DEMO_CUSTOMER_ID

router = APIRouter(prefix="/account")


@router.get("")
def get_account():
    """Return the demo customer's profile with optional loyalty/verified fields."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cols = ["c.id", "c.name", "c.email"]
            if column_exists("customers", "loyalty_points"):
                cols.append("c.loyalty_points")
            if column_exists("customers", "email_verified"):
                cols.append("c.email_verified")

            cur.execute(
                f"SELECT {', '.join(cols)} FROM {DB_SCHEMA}.customers c WHERE c.id = %s",
                (DEMO_CUSTOMER_ID,),
            )
            row = cur.fetchone()
            col_names = [d.name for d in cur.description]
            result = dict(zip(col_names, row))

            if table_exists("loyalty_members"):
                cur.execute(
                    f"""
                    SELECT lm.tier, lm.total_earned, lm.enrolled_at
                    FROM {DB_SCHEMA}.loyalty_members lm
                    JOIN {DB_SCHEMA}.customers c ON c.email = lm.email
                    WHERE c.id = %s
                    """,
                    (DEMO_CUSTOMER_ID,),
                )
                loyalty_row = cur.fetchone()
                if loyalty_row:
                    result["loyalty_tier"] = loyalty_row[0]
                    result["loyalty_total_earned"] = loyalty_row[1]
                    result["loyalty_enrolled_at"] = str(loyalty_row[2])

    return result
