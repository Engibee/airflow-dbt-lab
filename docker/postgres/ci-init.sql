CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.raw_customers (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    updated_at TIMESTAMP
);

INSERT INTO raw.raw_customers (id, name, email, updated_at)
VALUES
    (1, 'Ana', 'ANA@EMAIL.COM', '2026-08-10 10:00:00'),
    (2, 'Joao Silva', 'joao@email.com', '2026-08-10 10:00:00'),
    (3, 'Carlos', NULL, '2026-08-10 10:00:00');