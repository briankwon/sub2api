ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS concurrency_limit INTEGER NOT NULL DEFAULT 0
    CHECK (concurrency_limit >= 0);
COMMENT ON COLUMN api_keys.concurrency_limit IS
    'Simple-mode maximum concurrent requests; 0 adds no key limit';
