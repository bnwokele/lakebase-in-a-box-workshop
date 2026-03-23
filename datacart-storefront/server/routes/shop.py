from fastapi import APIRouter, Query, HTTPException
from server.db import pool, DB_SCHEMA
from server.schema_detector import table_exists, column_exists, get_promotions_table

router = APIRouter(prefix="/shop")


def _get_active_promos(cur) -> dict[int, dict]:
    """Fetch active promotions keyed by product_id. Returns empty dict if table missing."""
    promo_table = get_promotions_table()
    if not promo_table:
        return {}
    try:
        cur.execute(
            f"""
            SELECT product_id, badge_text, discount_pct, sale_price, promo_type
            FROM {DB_SCHEMA}.{promo_table}
            WHERE is_active = true
            """
        )
        return {
            r[0]: {"badge_text": r[1], "discount_pct": float(r[2]), "sale_price": float(r[3]) if r[3] else None, "promo_type": r[4]}
            for r in cur.fetchall()
        }
    except Exception:
        return {}


def _apply_promos(products: list[dict], promos: dict[int, dict]):
    """Merge promo data into product dicts."""
    for p in products:
        promo = promos.get(p["id"])
        if promo:
            p["badge_text"] = promo["badge_text"]
            p["discount_pct"] = promo["discount_pct"]
            p["sale_price"] = promo["sale_price"]


@router.get("/products")
def list_products(
    category: str = Query("", description="Filter by category"),
    search: str = Query("", description="Search by product name"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Browse the product catalog with optional category/search filters."""
    reviews_active = table_exists("reviews")
    loyalty_active = column_exists("customers", "loyalty_points")

    with pool.connection() as conn:
        with conn.cursor() as cur:
            conditions = []
            params: list = []

            if category:
                conditions.append("p.category = %s")
                params.append(category)
            if search:
                conditions.append("p.name ILIKE %s")
                params.append(f"%{search}%")

            where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

            review_select = ""
            review_join = ""
            if reviews_active:
                review_select = ", COALESCE(ROUND(AVG(r.rating)::numeric, 1), 0) AS avg_rating, COUNT(r.id) AS review_count"
                review_join = f"LEFT JOIN {DB_SCHEMA}.reviews r ON r.product_id = p.id"

            loyalty_select = ""
            if loyalty_active:
                loyalty_select = ", FLOOR(p.price)::INT AS loyalty_points_earned"

            cur.execute(
                f"""
                SELECT p.id, p.name, p.price, p.category,
                       COALESCE(i.quantity, 0) AS stock
                       {review_select}
                       {loyalty_select}
                FROM {DB_SCHEMA}.products p
                LEFT JOIN {DB_SCHEMA}.inventory i ON i.product_id = p.id
                {review_join}
                {where}
                GROUP BY p.id, p.name, p.price, p.category, i.quantity
                ORDER BY p.id
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]

            cur.execute(
                f"SELECT DISTINCT category FROM {DB_SCHEMA}.products ORDER BY category"
            )
            categories = [r[0] for r in cur.fetchall()]

            cur.execute(f"SELECT count(*) FROM {DB_SCHEMA}.products")
            total = cur.fetchone()[0]

            promos = _get_active_promos(cur)

    products = [dict(zip(cols, r)) for r in rows]
    if not reviews_active:
        for p in products:
            p["avg_rating"] = 0
            p["review_count"] = 0

    _apply_promos(products, promos)

    return {
        "products": products,
        "categories": categories,
        "total": total,
    }


@router.get("/products/{product_id}")
def get_product(product_id: int):
    """Get full product detail with reviews and stock info."""
    reviews_active = table_exists("reviews")
    loyalty_active = column_exists("customers", "loyalty_points")

    with pool.connection() as conn:
        with conn.cursor() as cur:
            loyalty_select = ", FLOOR(p.price)::INT AS loyalty_points_earned" if loyalty_active else ""

            cur.execute(
                f"""
                SELECT p.id, p.name, p.price, p.category,
                       COALESCE(i.quantity, 0) AS stock,
                       i.warehouse, i.reorder_level
                       {loyalty_select}
                FROM {DB_SCHEMA}.products p
                LEFT JOIN {DB_SCHEMA}.inventory i ON i.product_id = p.id
                WHERE p.id = %s
                """,
                (product_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Product not found")

            cols = [d.name for d in cur.description]
            product = dict(zip(cols, row))

            reviews = []
            avg_rating = 0.0
            review_count = 0

            if reviews_active:
                cur.execute(
                    f"""
                    SELECT r.rating, r.comment, r.review_date, c.name AS reviewer
                    FROM {DB_SCHEMA}.reviews r
                    JOIN {DB_SCHEMA}.customers c ON c.id = r.customer_id
                    WHERE r.product_id = %s
                    ORDER BY r.review_date DESC LIMIT 20
                    """,
                    (product_id,),
                )
                reviews = [
                    {
                        "rating": r[0],
                        "comment": r[1],
                        "review_date": str(r[2]) if r[2] else None,
                        "reviewer": r[3],
                    }
                    for r in cur.fetchall()
                ]

                cur.execute(
                    f"""
                    SELECT COALESCE(ROUND(AVG(rating)::numeric, 1), 0), COUNT(*)
                    FROM {DB_SCHEMA}.reviews WHERE product_id = %s
                    """,
                    (product_id,),
                )
                agg = cur.fetchone()
                avg_rating = float(agg[0])
                review_count = agg[1]

            product["avg_rating"] = avg_rating
            product["review_count"] = review_count

            # Apply promotion if active
            promos = _get_active_promos(cur)
            promo = promos.get(product_id)
            if promo:
                product["badge_text"] = promo["badge_text"]
                product["discount_pct"] = promo["discount_pct"]
                product["sale_price"] = promo["sale_price"]

    return {"product": product, "reviews": reviews}


@router.get("/featured")
def featured_products():
    """Get top-rated and best-selling products for the homepage."""
    reviews_active = table_exists("reviews")
    order_items_active = table_exists("order_items")
    loyalty_active = column_exists("customers", "loyalty_points")
    promotions_active = table_exists("promotions")

    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Top rated products (needs reviews table)
            top_rated = []
            if reviews_active:
                try:
                    loyalty_select = ", FLOOR(p.price)::INT AS loyalty_points_earned" if loyalty_active else ""
                    cur.execute(
                        f"""
                        SELECT p.id, p.name, p.price, p.category,
                               COALESCE(i.quantity, 0) AS stock,
                               ROUND(AVG(r.rating)::numeric, 1) AS avg_rating,
                               COUNT(r.id) AS review_count
                               {loyalty_select}
                        FROM {DB_SCHEMA}.products p
                        LEFT JOIN {DB_SCHEMA}.inventory i ON i.product_id = p.id
                        JOIN {DB_SCHEMA}.reviews r ON r.product_id = p.id
                        GROUP BY p.id, p.name, p.price, p.category, i.quantity
                        HAVING COUNT(r.id) >= 2
                        ORDER BY avg_rating DESC, review_count DESC
                        LIMIT 8
                        """
                    )
                    rows = cur.fetchall()
                    cols = [d.name for d in cur.description]
                    top_rated = [dict(zip(cols, r)) for r in rows]
                except Exception:
                    top_rated = []

            # Best sellers by order volume (needs order_items table)
            best_sellers = []
            best_sellers_unavailable = False
            if order_items_active:
                try:
                    loyalty_select2 = ", FLOOR(p.price)::INT AS loyalty_points_earned" if loyalty_active else ""
                    review_select = ""
                    review_join = ""
                    if reviews_active:
                        review_select = ", COALESCE(ROUND(AVG(r.rating)::numeric, 1), 0) AS avg_rating"
                        review_join = f"LEFT JOIN {DB_SCHEMA}.reviews r ON r.product_id = p.id"

                    cur.execute(
                        f"""
                        SELECT p.id, p.name, p.price, p.category,
                               COALESCE(inv.quantity, 0) AS stock,
                               SUM(oi.quantity) AS units_sold
                               {review_select}
                               {loyalty_select2}
                        FROM {DB_SCHEMA}.order_items oi
                        JOIN {DB_SCHEMA}.products p ON p.id = oi.product_id
                        LEFT JOIN {DB_SCHEMA}.inventory inv ON inv.product_id = p.id
                        {review_join}
                        GROUP BY p.id, p.name, p.price, p.category, inv.quantity
                        ORDER BY units_sold DESC
                        LIMIT 8
                        """
                    )
                    rows = cur.fetchall()
                    cols = [d.name for d in cur.description]
                    best_sellers = [dict(zip(cols, r)) for r in rows]
                    if not reviews_active:
                        for bs in best_sellers:
                            bs["avg_rating"] = 0
                            bs["review_count"] = 0
                except Exception:
                    best_sellers_unavailable = True
            else:
                best_sellers_unavailable = True

            # Spring Sale deals (needs promotions synced table)
            promo_deals = []
            promo_table = get_promotions_table()
            if promo_table:
                try:
                    loyalty_select3 = ", FLOOR(p.price)::INT AS loyalty_points_earned" if loyalty_active else ""
                    cur.execute(
                        f"""
                        SELECT p.id, p.name, p.price, p.category,
                               COALESCE(i.quantity, 0) AS stock,
                               pr.badge_text, pr.discount_pct, pr.sale_price
                               {loyalty_select3}
                        FROM {DB_SCHEMA}.{promo_table} pr
                        JOIN {DB_SCHEMA}.products p ON p.id = pr.product_id
                        LEFT JOIN {DB_SCHEMA}.inventory i ON i.product_id = p.id
                        WHERE pr.is_active = true
                        ORDER BY pr.discount_pct DESC
                        LIMIT 8
                        """
                    )
                    rows = cur.fetchall()
                    cols = [d.name for d in cur.description]
                    promo_deals = [dict(zip(cols, r)) for r in rows]
                    # Add review data if available
                    for pd in promo_deals:
                        pd["avg_rating"] = 0
                        pd["review_count"] = 0
                except Exception:
                    promo_deals = []

            # Fetch promos to overlay on top_rated and best_sellers
            promos = _get_active_promos(cur)

            # Store-wide stats for the banner
            cur.execute(f"SELECT count(*) FROM {DB_SCHEMA}.products")
            total_products = cur.fetchone()[0]
            cur.execute(f"SELECT count(DISTINCT category) FROM {DB_SCHEMA}.products")
            total_categories = cur.fetchone()[0]

    # Ensure top_rated have review_count if not from query
    for p in top_rated:
        if "review_count" not in p:
            p["review_count"] = 0

    # Apply promo overlays to top_rated and best_sellers
    _apply_promos(top_rated, promos)
    _apply_promos(best_sellers, promos)

    return {
        "top_rated": top_rated,
        "best_sellers": best_sellers,
        "best_sellers_unavailable": best_sellers_unavailable,
        "reviews_unavailable": not reviews_active,
        "promo_deals": promo_deals,
        "total_products": total_products,
        "total_categories": total_categories,
    }
