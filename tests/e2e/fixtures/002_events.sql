CREATE TABLE IF NOT EXISTS default.events (
    event_id     String,
    type         String,
    user_id      String,
    payload      String DEFAULT '{}',
    source       String DEFAULT 'web',
    received_timestamp DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (type, received_timestamp);
