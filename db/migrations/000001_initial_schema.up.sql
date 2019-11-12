CREATE TABLE IF NOT EXISTS source_cache (
   id         VARCHAR(32) PRIMARY KEY,
   type       VARCHAR(32) NOT NULL,
   source     VARCHAR(32) NOT NULL,
   payload    TEXT NOT NULL,
   created_at timestamptz NOT NULL DEFAULT NOW(),
   updated_at timestamptz NOT NULL DEFAULT NOW()
);
