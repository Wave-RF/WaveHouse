CREATE TABLE IF NOT EXISTS default.clicks (
    event_id     String,
    page         String,
    user_id      String,
    session_id   String,
    referrer     String DEFAULT '',
    country      String DEFAULT 'US',
    duration_ms  UInt32 DEFAULT 0,
    received_timestamp DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (page, received_timestamp);
