CREATE OR REPLACE TABLE revenue_summary AS
SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    COUNT(o.id) AS total_orders,
    SUM(o.amount) AS total_revenue,
    MAX(o.ordered_at) AS last_order_at
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'completed'
GROUP BY c.id, c.name
ORDER BY total_revenue DESC;
