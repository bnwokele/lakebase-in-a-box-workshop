import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from server.db import pool, DB_SCHEMA
from server.schema_detector import column_exists, table_exists, get_promotions_table

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/cart")

# In-memory cart store keyed by customer_id.
_carts: dict[int, dict[int, int]] = {}

DEMO_CUSTOMER_ID = 1  # Alice Smith


class CartItem(BaseModel):
    product_id: int
    quantity: int = 1


@router.get("")
def get_cart():
    """Get the current shopping cart with product details and stock checks."""
    loyalty_active = column_exists("customers", "loyalty_points")
    cart = _carts.get(DEMO_CUSTOMER_ID, {})
    if not cart:
        return {"items": [], "subtotal": 0, "item_count": 0}

    product_ids = list(cart.keys())
    with pool.connection() as conn:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(product_ids))
            cur.execute(
                f"""
                SELECT p.id, p.name, p.price, p.category,
                       COALESCE(i.quantity, 0) AS stock
                FROM {DB_SCHEMA}.products p
                LEFT JOIN {DB_SCHEMA}.inventory i ON i.product_id = p.id
                WHERE p.id IN ({placeholders})
                """,
                product_ids,
            )
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]

    products_map = {r[0]: dict(zip(cols, r)) for r in rows}

    # Fetch active promotions for sale prices
    promos: dict[int, dict] = {}
    promo_table = get_promotions_table()
    if promo_table:
        try:
            with pool.connection() as conn2:
                with conn2.cursor() as cur2:
                    cur2.execute(
                        f"SELECT product_id, sale_price, badge_text, discount_pct "
                        f"FROM {DB_SCHEMA}.{promo_table} WHERE is_active = true"
                    )
                    for r in cur2.fetchall():
                        promos[r[0]] = {"sale_price": float(r[1]) if r[1] else None, "badge_text": r[2], "discount_pct": float(r[3])}
        except Exception:
            pass

    items = []
    subtotal = 0
    total_points_earned = 0
    for pid, qty in cart.items():
        product = products_map.get(pid)
        if not product:
            continue
        promo = promos.get(pid)
        effective_price = promo["sale_price"] if promo and promo["sale_price"] else float(product["price"])
        line_total = effective_price * qty
        subtotal += line_total
        item = {
            **product,
            "cart_quantity": qty,
            "line_total": round(line_total, 2),
            "in_stock": product["stock"] >= qty,
        }
        if promo:
            item["sale_price"] = promo["sale_price"]
            item["badge_text"] = promo["badge_text"]
            item["discount_pct"] = promo["discount_pct"]
        if loyalty_active:
            pts = int(effective_price) * qty
            item["loyalty_points_earned"] = pts
            total_points_earned += pts
        items.append(item)

    result = {
        "items": items,
        "subtotal": round(subtotal, 2),
        "item_count": sum(cart.values()),
    }

    if loyalty_active:
        result["total_points_earned"] = total_points_earned
        # Include loyalty tier for discount hints
        if table_exists("loyalty_members"):
            try:
                with pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            SELECT lm.tier FROM {DB_SCHEMA}.loyalty_members lm
                            JOIN {DB_SCHEMA}.customers c ON c.email = lm.email
                            WHERE c.id = %s
                            """,
                            (DEMO_CUSTOMER_ID,),
                        )
                        row = cur.fetchone()
                        if row:
                            result["loyalty_tier"] = row[0]
            except Exception:
                pass

    return result


@router.post("/add")
def add_to_cart(item: CartItem):
    """Add a product to the cart."""
    if item.quantity < 1:
        raise HTTPException(status_code=400, detail="Quantity must be at least 1")

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT p.id, p.name, COALESCE(i.quantity, 0) AS stock
                FROM {DB_SCHEMA}.products p
                LEFT JOIN {DB_SCHEMA}.inventory i ON i.product_id = p.id
                WHERE p.id = %s
                """,
                (item.product_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Product not found")

    cart = _carts.setdefault(DEMO_CUSTOMER_ID, {})
    current_qty = cart.get(item.product_id, 0)
    cart[item.product_id] = current_qty + item.quantity

    return {"message": f"Added {item.quantity}x {row[1]} to cart", "cart_quantity": cart[item.product_id]}


@router.post("/update")
def update_cart_item(item: CartItem):
    """Update quantity of a cart item. Set quantity=0 to remove."""
    cart = _carts.get(DEMO_CUSTOMER_ID, {})
    if item.product_id not in cart:
        raise HTTPException(status_code=404, detail="Item not in cart")

    if item.quantity <= 0:
        del cart[item.product_id]
        return {"message": "Item removed from cart"}
    else:
        cart[item.product_id] = item.quantity
        return {"message": "Cart updated", "cart_quantity": item.quantity}


@router.post("/clear")
def clear_cart():
    """Clear the entire cart."""
    _carts[DEMO_CUSTOMER_ID] = {}
    return {"message": "Cart cleared"}
