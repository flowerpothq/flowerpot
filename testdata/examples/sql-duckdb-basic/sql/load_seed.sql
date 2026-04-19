INSERT INTO customers (id, name, email) VALUES
    (1, 'Alice Johnson', 'alice@example.com'),
    (2, 'Bob Smith', 'bob@example.com'),
    (3, 'Carol Williams', 'carol@example.com')
ON CONFLICT (id) DO NOTHING;

INSERT INTO orders (id, customer_id, amount, status, ordered_at) VALUES
    (1, 1, 99.99,  'completed', '2026-01-15 10:30:00'),
    (2, 1, 149.50, 'completed', '2026-02-01 14:00:00'),
    (3, 2, 75.00,  'completed', '2026-01-20 09:15:00'),
    (4, 3, 200.00, 'pending',   '2026-03-10 16:45:00'),
    (5, 2, 50.00,  'completed', '2026-03-15 11:00:00')
ON CONFLICT (id) DO NOTHING;
