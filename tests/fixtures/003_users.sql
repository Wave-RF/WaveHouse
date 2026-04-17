CREATE TABLE IF NOT EXISTS default.users (
    user_id      String,
    name         String,
    email        String,
    plan         String DEFAULT 'free',
    created_at   DateTime64(3) DEFAULT now64(3),
    received_timestamp DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (user_id);
