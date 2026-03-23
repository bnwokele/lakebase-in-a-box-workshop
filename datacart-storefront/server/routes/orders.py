import logging
from fastapi import APIRouter, HTTPException
from server.db import pool, DB_SCHEMA
from server.schema_detector import table_exists, column_exists, invalidate_cache
from server.routes.cart import _carts, DEMO_CUSTOMER_ID

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/orders")


def _guard_orders():
    """Raise 503 if orders table is missing (PITR disaster scenario)."""
    if not table_exists("orders"):
        raise HTTPException(
            status_code=503,
            detail="Orders service temporarily unavailable. The orders table is missing — recovery may be in progress.",
        )


@router.get("")
def get_orders():
    """Get order history for the demo customer."""
    _guard_orders()

    has_priority = column_exists("orders", "priority")

    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                priority_select = ", o.priority" if has_priority else ""

                cur.execute(
                    f"""
                    SELECT o.id, p.name AS product, o.quantity, o.total,
                           o.currency, o.order_date, o.status
                           {priority_select}
                    FROM {DB_SCHEMA}.orders o
                    JOIN {DB_SCHEMA}.products p ON p.id = o.product_id
                    WHERE o.customer_id = %s
                    ORDER BY o.order_date DESC
                    """,
                    (DEMO_CUSTOMER_ID,),
                )
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]

        return {"orders": [dict(zip(cols, r)) for r in rows]}
    except Exception as e:
        if "does not exist" in str(e):
            invalidate_cache()
            raise HTTPException(status_code=503, detail="Orders service temporarily unavailable.")
        raise


@router.get("/{order_id}")
def get_order_detail(order_id: int):
    """Get order detail including line items."""
    _guard_orders()

    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                priority_select = ", o.priority" if column_exists("orders", "priority") else ""

                cur.execute(
                    f"""
                    SELECT o.id, c.name AS customer, o.order_date, o.status,
                           o.total, o.currency
                           {priority_select}
                    FROM {DB_SCHEMA}.orders o
                    JOIN {DB_SCHEMA}.customers c ON c.id = o.customer_id
                    WHERE o.id = %s AND o.customer_id = %s
                    """,
                    (order_id, DEMO_CUSTOMER_ID),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Order not found")
                cols = [d.name for d in cur.description]
                order = dict(zip(cols, row))

                items = []
                if table_exists("order_items"):
                    cur.execute(
                        f"""
                        SELECT oi.id, p.name AS product, oi.quantity,
                               oi.unit_price, oi.line_total
                        FROM {DB_SCHEMA}.order_items oi
                        JOIN {DB_SCHEMA}.products p ON p.id = oi.product_id
                        WHERE oi.order_id = %s
                        ORDER BY oi.id
                        """,
                        (order_id,),
                    )
                    item_rows = cur.fetchall()
                    item_cols = [d.name for d in cur.description]
                    items = [dict(zip(item_cols, r)) for r in item_rows]

        return {"order": order, "items": items}
    except HTTPException:
        raise
    except Exception as e:
        if "does not exist" in str(e):
            invalidate_cache()
            raise HTTPException(status_code=503, detail="Orders service temporarily unavailable.")
        raise


@router.post("/checkout")
def checkout():
    """Place an order from the current cart contents."""
    _guard_orders()

    if not table_exists("order_items"):
        raise HTTPException(
            status_code=503,
            detail="Checkout is temporarily unavailable. The order items table is missing.",
        )

    cart = _carts.get(DEMO_CUSTOMER_ID, {})
    if not cart:
        raise HTTPException(status_code=400, detail="Cart is empty")

    loyalty_active = column_exists("customers", "loyalty_points")
    product_ids = list(cart.keys())

    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(product_ids))
                cur.execute(
                    f"""
                    SELECT p.id, p.name, p.price, COALESCE(i.quantity, 0) AS stock
                    FROM {DB_SCHEMA}.products p
                    LEFT JOIN {DB_SCHEMA}.inventory i ON i.product_id = p.id
                    WHERE p.id IN ({placeholders})
                    """,
                    product_ids,
                )
                products = {r[0]: {"name": r[1], "price": r[2], "stock": r[3]} for r in cur.fetchall()}

                for pid, qty in cart.items():
                    p = products.get(pid)
                    if not p:
                        raise HTTPException(status_code=400, detail=f"Product {pid} not found")
                    if p["stock"] < qty:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Not enough stock for {p['name']} (requested {qty}, available {p['stock']})",
                        )

                order_ids = []
                total_points_earned = 0
                for pid, qty in cart.items():
                    p = products[pid]
                    total = float(p["price"]) * qty
                    cur.execute(
                        f"""
                        INSERT INTO {DB_SCHEMA}.orders
                            (customer_id, product_id, quantity, total, currency, status)
                        VALUES (%s, %s, %s, %s, 'USD', 'pending')
                        RETURNING id
                        """,
                        (DEMO_CUSTOMER_ID, pid, qty, round(total, 2)),
                    )
                    order_id = cur.fetchone()[0]
                    order_ids.append(order_id)

                    cur.execute(
                        f"""
                        INSERT INTO {DB_SCHEMA}.order_items
                            (order_id, product_id, quantity, unit_price, line_total)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (order_id, pid, qty, float(p["price"]), round(total, 2)),
                    )

                    cur.execute(
                        f"""
                        UPDATE {DB_SCHEMA}.inventory
                        SET quantity = quantity - %s
                        WHERE product_id = %s AND quantity >= %s
                        """,
                        (qty, pid, qty),
                    )

                    total_points_earned += int(p["price"]) * qty

                # Award loyalty points if active
                if loyalty_active and total_points_earned > 0:
                    cur.execute(
                        f"""
                        UPDATE {DB_SCHEMA}.customers
                        SET loyalty_points = loyalty_points + %s
                        WHERE id = %s
                        """,
                        (total_points_earned, DEMO_CUSTOMER_ID),
                    )

            conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        if "does not exist" in str(e):
            invalidate_cache()
            raise HTTPException(status_code=503, detail="Checkout is temporarily unavailable.")
        raise

    _carts[DEMO_CUSTOMER_ID] = {}

    result = {"message": "Order placed successfully!", "order_ids": order_ids}
    if loyalty_active:
        result["points_earned"] = total_points_earned
    return result
