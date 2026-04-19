BEGIN;

-- Unique index for safe upserts
CREATE UNIQUE INDEX IF NOT EXISTS uq_game_images_game_source_type_url
    ON game_images (game_id, source, image_type, url);

-- Index for cdn_url IS NULL promotion query (without created_at)
CREATE INDEX IF NOT EXISTS idx_game_images_cdn_null
    ON game_images (id ASC)
    WHERE cdn_url IS NULL AND url IS NOT NULL;

-- game_media unique on (game_id, url)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'game_media'
          AND indexname  = 'uq_game_media_game_url'
    ) THEN
        CREATE UNIQUE INDEX uq_game_media_game_url
            ON game_media (game_id, url);
    END IF;
END $$;

-- game_ratings unique on (game_id, source)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'game_ratings'
          AND indexname  = 'uq_game_ratings_game_source'
    ) THEN
        CREATE UNIQUE INDEX uq_game_ratings_game_source
            ON game_ratings (game_id, source);
    END IF;
END $$;

-- game_metadata unique on (game_id, source)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'game_metadata'
          AND indexname  = 'uq_game_metadata_game_source'
    ) THEN
        CREATE UNIQUE INDEX uq_game_metadata_game_source
            ON game_metadata (game_id, source);
    END IF;
END $$;

-- game_system_requirements unique on (game_id, source, platform, tier)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'game_system_requirements'
          AND indexname  = 'uq_sysreq_game_source_platform_tier'
    ) THEN
        CREATE UNIQUE INDEX uq_sysreq_game_source_platform_tier
            ON game_system_requirements (game_id, source, platform, tier);
    END IF;
END $$;

-- platform_ids unique on (platform, namespace, external_id)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'platform_ids'
          AND indexname  = 'uq_platform_ids_platform_ns_ext'
    ) THEN
        CREATE UNIQUE INDEX uq_platform_ids_platform_ns_ext
            ON platform_ids (platform, namespace, external_id);
    END IF;
END $$;

COMMIT;