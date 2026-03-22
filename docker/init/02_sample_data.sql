-- =============================================================================
-- Sample data generation — target: 50 MB+ across all databases
-- Run time: ~60-90 seconds on first container start (one-time cost)
-- =============================================================================

-- =============================================================================
-- DATABASE: ecommerce_db  (~55 MB)
-- =============================================================================
\c ecommerce_db

-- ── Users (~30 MB) ────────────────────────────────────────────────────────────
CREATE TABLE users (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100)  NOT NULL,
    email       VARCHAR(150)  NOT NULL UNIQUE,
    phone       VARCHAR(20),
    address     TEXT,
    bio         TEXT,                         -- ~270 chars each
    status      VARCHAR(20)   DEFAULT 'active',
    created_at  TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX idx_users_email  ON users (email);
CREATE INDEX idx_users_status ON users (status);

INSERT INTO users (name, email, phone, address, bio, status, created_at)
SELECT
    'User ' || i,
    'user' || i || '@shop.example.com',
    '+1-555-' || LPAD((i % 9000 + 1000)::TEXT, 4, '0'),
    i || ' Commerce Ave, Suite ' || (i % 500) || ', City ' || (i % 100),
    repeat('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor. ', 3),
    CASE (i % 5)
        WHEN 0 THEN 'active'
        WHEN 1 THEN 'premium'
        WHEN 2 THEN 'inactive'
        WHEN 3 THEN 'active'
        ELSE 'active'
    END,
    NOW() - ((random() * 730)::INT * INTERVAL '1 day')
FROM generate_series(1, 100000) i;

-- ── Products (~10 MB) ─────────────────────────────────────────────────────────
CREATE TABLE products (
    id           SERIAL PRIMARY KEY,
    sku          VARCHAR(50)    NOT NULL UNIQUE,
    name         VARCHAR(200)   NOT NULL,
    category     VARCHAR(100),
    price        NUMERIC(10, 2) NOT NULL,
    cost         NUMERIC(10, 2),
    stock        INT            DEFAULT 0,
    description  TEXT,                        -- ~450 chars each
    created_at   TIMESTAMPTZ    DEFAULT NOW()
);

CREATE INDEX idx_products_category ON products (category);
CREATE INDEX idx_products_price    ON products (price);

INSERT INTO products (sku, name, category, price, cost, stock, description, created_at)
SELECT
    'SKU-' || LPAD(i::TEXT, 8, '0'),
    'Product ' || i || ' — ' || (ARRAY['Widget','Gadget','Gizmo','Device','Tool','Component'])[((i-1) % 6) + 1],
    (ARRAY['Electronics','Clothing','Books','Home','Sports','Toys','Food','Automotive'])[((i-1) % 8) + 1],
    (10 + (random() * 990))::NUMERIC(10, 2),
    (5  + (random() * 400))::NUMERIC(10, 2),
    (random() * 5000)::INT,
    repeat('Detailed product specification. High quality materials. Manufactured to industry standards. ', 5),
    NOW() - ((random() * 365)::INT * INTERVAL '1 day')
FROM generate_series(1, 20000) i;

-- ── Orders (~15 MB) ───────────────────────────────────────────────────────────
CREATE TABLE orders (
    id           SERIAL PRIMARY KEY,
    user_id      INT            NOT NULL REFERENCES users (id),
    status       VARCHAR(30)    DEFAULT 'pending',
    total        NUMERIC(12, 2) NOT NULL,
    notes        TEXT,
    created_at   TIMESTAMPTZ    DEFAULT NOW(),
    shipped_at   TIMESTAMPTZ
);

CREATE INDEX idx_orders_user_id    ON orders (user_id);
CREATE INDEX idx_orders_status     ON orders (status);
CREATE INDEX idx_orders_created_at ON orders (created_at);

CREATE TABLE order_items (
    id           SERIAL PRIMARY KEY,
    order_id     INT            NOT NULL REFERENCES orders (id),
    product_id   INT            NOT NULL REFERENCES products (id),
    quantity     INT            NOT NULL DEFAULT 1,
    unit_price   NUMERIC(10, 2) NOT NULL,
    total        NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);

CREATE INDEX idx_order_items_order_id   ON order_items (order_id);
CREATE INDEX idx_order_items_product_id ON order_items (product_id);

INSERT INTO orders (user_id, status, total, notes, created_at, shipped_at)
SELECT
    (random() * 99999 + 1)::INT,
    (ARRAY['pending','processing','shipped','delivered','cancelled','refunded'])[((i-1) % 6) + 1],
    (10 + random() * 4990)::NUMERIC(12, 2),
    CASE WHEN i % 4 = 0 THEN 'Customer requested gift wrapping. Please handle with care.' ELSE NULL END,
    NOW() - ((random() * 365)::INT * INTERVAL '1 day'),
    CASE WHEN i % 3 = 0 THEN NOW() - ((random() * 30)::INT * INTERVAL '1 day') ELSE NULL END
FROM generate_series(1, 150000) i;

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT
    (random() * 149999 + 1)::INT,
    (random() * 19999  + 1)::INT,
    (random() * 9 + 1)::INT,
    (10 + random() * 490)::NUMERIC(10, 2)
FROM generate_series(1, 300000) i;


-- =============================================================================
-- DATABASE: analytics_db  (~20 MB)
-- =============================================================================
\c analytics_db

-- ── Events (~15 MB) ───────────────────────────────────────────────────────────
CREATE TABLE events (
    id          BIGSERIAL PRIMARY KEY,
    session_id  UUID          NOT NULL DEFAULT gen_random_uuid(),
    user_id     INT,
    event_type  VARCHAR(60)   NOT NULL,
    page        VARCHAR(255),
    referrer    VARCHAR(500),
    user_agent  TEXT,
    ip_address  INET,
    properties  TEXT,                         -- JSON-like blob ~200 chars
    occurred_at TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX idx_events_user_id     ON events (user_id);
CREATE INDEX idx_events_event_type  ON events (event_type);
CREATE INDEX idx_events_occurred_at ON events (occurred_at);

INSERT INTO events (user_id, event_type, page, referrer, user_agent, ip_address, properties, occurred_at)
SELECT
    CASE WHEN i % 10 = 0 THEN NULL ELSE (random() * 99999 + 1)::INT END,
    (ARRAY['page_view','click','scroll','form_submit','purchase','signup','logout','search'])[((i-1) % 8) + 1],
    '/path/' || (i % 500) || '/page-' || (i % 20),
    CASE WHEN i % 3 = 0 THEN 'https://google.com/search?q=term' || (i % 100) ELSE NULL END,
    'Mozilla/5.0 (compatible; TestBrowser/' || (i % 5 + 1) || '.0)',
    ('10.' || (i % 256) || '.' || ((i / 256) % 256) || '.1')::INET,
    '{"duration":' || (i % 300) || ',"scroll_depth":' || (i % 100) || ',"clicks":' || (i % 50) || '}',
    NOW() - ((random() * 180)::INT * INTERVAL '1 day') - ((random() * 86400)::INT * INTERVAL '1 second')
FROM generate_series(1, 100000) i;

-- ── Page views summary (~5 MB) ────────────────────────────────────────────────
CREATE TABLE daily_page_views (
    id          SERIAL PRIMARY KEY,
    date        DATE          NOT NULL,
    page        VARCHAR(255)  NOT NULL,
    views       INT           NOT NULL DEFAULT 0,
    unique_users INT          NOT NULL DEFAULT 0,
    avg_duration_ms INT,
    UNIQUE (date, page)
);

INSERT INTO daily_page_views (date, page, views, unique_users, avg_duration_ms)
SELECT
    (CURRENT_DATE - (i % 365))::DATE                AS date,
    '/path/' || (i % 200) || '/page-' || (i % 10)  AS page,
    SUM((random() * 10000 + 100)::INT)              AS views,
    SUM((random() * 5000  + 50)::INT)               AS unique_users,
    AVG((random() * 30000 + 500)::INT)::INT         AS avg_duration_ms
FROM generate_series(1, 50000) i
GROUP BY date, page
ON CONFLICT (date, page) DO UPDATE
    SET views        = daily_page_views.views + EXCLUDED.views,
        unique_users = daily_page_views.unique_users + EXCLUDED.unique_users;


-- =============================================================================
-- DATABASE: inventory_db  (~10 MB)
-- =============================================================================
\c inventory_db

-- ── Warehouses ────────────────────────────────────────────────────────────────
CREATE TABLE warehouses (
    id        SERIAL PRIMARY KEY,
    code      VARCHAR(20)  NOT NULL UNIQUE,
    name      VARCHAR(150) NOT NULL,
    address   TEXT,
    capacity  INT          DEFAULT 10000,
    active    BOOLEAN      DEFAULT TRUE
);

INSERT INTO warehouses (code, name, address, capacity)
SELECT
    'WH-' || LPAD(i::TEXT, 4, '0'),
    'Warehouse ' || i || ' — Region ' || (i % 20),
    i || ' Industrial Blvd, Warehouse District, State ' || (i % 50),
    (1000 + (random() * 50000))::INT
FROM generate_series(1, 200) i;

-- ── Items (~5 MB) ─────────────────────────────────────────────────────────────
CREATE TABLE items (
    id           SERIAL PRIMARY KEY,
    sku          VARCHAR(50)  NOT NULL UNIQUE,
    name         VARCHAR(200) NOT NULL,
    warehouse_id INT          NOT NULL REFERENCES warehouses (id),
    quantity     INT          NOT NULL DEFAULT 0,
    unit         VARCHAR(30)  DEFAULT 'each',
    location     VARCHAR(100),
    notes        TEXT,
    last_counted TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_items_warehouse ON items (warehouse_id);
CREATE INDEX idx_items_sku       ON items (sku);

INSERT INTO items (sku, name, warehouse_id, quantity, unit, location, notes, last_counted)
SELECT
    'ITEM-' || LPAD(i::TEXT, 8, '0'),
    'Inventory Item ' || i || ' — ' || (ARRAY['Part','Assembly','Component','Module','Unit','Pack'])[((i-1) % 6) + 1],
    (random() * 199 + 1)::INT,
    (random() * 10000)::INT,
    (ARRAY['each','box','pallet','kg','litre','metre'])[((i-1) % 6) + 1],
    'Aisle-' || (i % 50) || '-Shelf-' || ((i / 50) % 20) || '-Bin-' || (i % 10),
    CASE WHEN i % 5 = 0 THEN 'Handle with care. Fragile item requires special packaging.' ELSE NULL END,
    NOW() - ((random() * 90)::INT * INTERVAL '1 day')
FROM generate_series(1, 50000) i;

-- ── Movements (~5 MB) ─────────────────────────────────────────────────────────
CREATE TABLE stock_movements (
    id            BIGSERIAL   PRIMARY KEY,
    item_id       INT         NOT NULL REFERENCES items (id),
    warehouse_id  INT         NOT NULL REFERENCES warehouses (id),
    movement_type VARCHAR(20) NOT NULL,  -- 'in','out','transfer','adjust'
    quantity      INT         NOT NULL,
    reference     VARCHAR(100),
    moved_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_movements_item_id  ON stock_movements (item_id);
CREATE INDEX idx_movements_moved_at ON stock_movements (moved_at);

INSERT INTO stock_movements (item_id, warehouse_id, movement_type, quantity, reference, moved_at)
SELECT
    (random() * 49999 + 1)::INT,
    (random() * 199   + 1)::INT,
    (ARRAY['in','out','transfer','adjust'])[((i-1) % 4) + 1],
    (random() * 500 + 1)::INT,
    'REF-' || LPAD(i::TEXT, 10, '0'),
    NOW() - ((random() * 365)::INT * INTERVAL '1 day')
FROM generate_series(1, 100000) i;

-- Back to default database
\c postgres
SELECT 'Sample data generation complete.' AS status;
