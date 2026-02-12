"""
Ejemplo: Cómo crear transformaciones custom.

Muestra cómo usar DuckDB SQL dentro de pipelines
para crear lógica de negocio personalizada.
"""

import duckdb


def main():
    conn = duckdb.connect()

    # Crear datos de ejemplo en memoria
    conn.execute("""
        CREATE TABLE raw_orders AS SELECT * FROM (VALUES
            (1, 'ORD-001', 'Alice', 150.00, '2024-01-15', 'COMPLETED'),
            (2, 'ORD-002', 'Bob', 75.50, '2024-01-16', 'COMPLETED'),
            (3, 'ORD-003', 'Alice', 200.00, '2024-01-20', 'CANCELLED'),
            (4, 'ORD-004', 'Charlie', 50.00, '2024-02-01', 'COMPLETED'),
            (5, 'ORD-005', 'Alice', 300.00, '2024-02-15', 'COMPLETED'),
            (6, 'ORD-006', 'Bob', 125.00, '2024-03-01', 'PENDING'),
            (7, 'ORD-007', 'Diana', 500.00, '2024-03-10', 'COMPLETED')
        ) AS t(id, order_number, customer, total, order_date, status)
    """)

    print("=" * 60)
    print("Custom Transformation Examples")
    print("=" * 60)

    # 1. Transformación: Customer Segmentation
    print("\n--- Customer Segmentation ---")
    result = conn.execute("""
        WITH customer_metrics AS (
            SELECT
                customer,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN status = 'COMPLETED' THEN total ELSE 0 END) AS total_spent,
                AVG(CASE WHEN status = 'COMPLETED' THEN total ELSE NULL END) AS avg_order,
                MAX(order_date) AS last_order
            FROM raw_orders
            GROUP BY customer
        )
        SELECT
            customer,
            total_orders,
            total_spent,
            ROUND(avg_order, 2) AS avg_order,
            last_order,
            CASE
                WHEN total_spent >= 400 THEN 'VIP'
                WHEN total_spent >= 100 THEN 'Regular'
                ELSE 'New'
            END AS segment
        FROM customer_metrics
        ORDER BY total_spent DESC
    """).fetchdf()
    print(result.to_string(index=False))

    # 2. Transformación: Monthly Cohort Analysis
    print("\n--- Monthly Revenue ---")
    result = conn.execute("""
        SELECT
            DATE_TRUNC('month', CAST(order_date AS DATE)) AS month,
            COUNT(*) AS orders,
            COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed,
            ROUND(SUM(CASE WHEN status = 'COMPLETED' THEN total ELSE 0 END), 2) AS revenue,
            ROUND(
                COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) * 100.0 / COUNT(*), 1
            ) AS completion_rate_pct
        FROM raw_orders
        GROUP BY DATE_TRUNC('month', CAST(order_date AS DATE))
        ORDER BY month
    """).fetchdf()
    print(result.to_string(index=False))

    # 3. Transformación: usando enrichment helpers
    print("\n--- Hash Key Generation ---")
    from ducklake.transformations.enrichment import build_hash_key_sql
    sql = build_hash_key_sql(["order_number", "customer"], "order_hash", "raw_orders")
    result = conn.execute(sql).fetchdf()
    print(result[["order_number", "customer", "order_hash"]].to_string(index=False))

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
