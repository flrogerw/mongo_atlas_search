-- vim: set ts=4 expandtab
SET client_min_messages TO WARNING;
SET ROLE=pod_admin;

-- DROP SCHEMA IF EXISTS podcast CASCADE CASCADE;
-- CREATE SCHEMA podcast;

SET search_path TO podcast_kafka;


/**
 *
 *  Extensions, Functions, and Stored Procedures
 *
 **/
--
-- Function to automate updated of `date_modified` columns so that the application is not responsible for managing that value.
--
CREATE OR REPLACE FUNCTION update_date_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.date_modified = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';


DROP TABLE IF EXISTS struct_type CASCADE;
CREATE TABLE struct_type (
      struct_type_id                                                            INTEGER                                     NOT NULL GENERATED BY DEFAULT AS IDENTITY
    , date_created                                                              TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , date_modified                                                             TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , display_order                                                             SMALLINT                                    NOT NULL
    , group_name                                                                TEXT                                        NOT NULL
    , att_pub_ident                                                             TEXT                                        NOT NULL
    , att_value                                                                 TEXT                                        NOT NULL
    , PRIMARY KEY (struct_type_id)
);
CREATE UNIQUE INDEX idx_stuct_type_att_pub_ident                                ON struct_type                              USING btree (group_name, att_pub_ident);


DROP TABLE IF EXISTS ingest CASCADE;
CREATE TABLE ingest (
      ingest_id                                                                 INTEGER                                     NOT NULL GENERATED BY DEFAULT AS IDENTITY
    , record_hash                                                                 TEXT                                        NOT NULL           -- hash of the entire file read as a string
    , podcast_uuid                                                              UUID                                        NOT NULL
    , PRIMARY KEY (ingest_id)
);
CREATE UNIQUE INDEX idx_ingest_podcast_uuid                                     ON ingest                                   USING btree (podcast_uuid);


DROP TABLE IF EXISTS meta_container CASCADE;
CREATE TABLE meta_container (
      meta_container_id                                                         INTEGER                                     NOT NULL GENERATED BY DEFAULT AS IDENTITY
    , container_name                                                            TEXT                                        NOT NULL           -- hash of the entire file read as a string
    , PRIMARY KEY (meta_container_id)
);


DROP TABLE IF EXISTS container_podcast_lnk CASCADE;
CREATE TABLE container_podcast_lnk (
      meta_container_id                                                         INTEGER                                     NOT NULL GENERATED BY DEFAULT AS IDENTITY
    , podcast_id                                                                TEXT                                        NOT NULL           -- hash of the entire file read as a string
    , PRIMARY KEY (meta_container_id, podcast_id)
);


DROP TABLE IF EXISTS podcast_purgatory CASCADE;
CREATE TABLE podcast_purgatory (
      podcast_purgatory_id                                                      INTEGER                                     NOT NULL
    , podcast_uuid                                                              UUID                                                   -- to OS
    , date_created                                                              TIMESTAMPTZ                                          DEFAULT CURRENT_TIMESTAMP
    , date_modified                                                             TIMESTAMPTZ                                          DEFAULT CURRENT_TIMESTAMP
    , description_selected                                                      INTEGER                                                         -- 110=Cleaned, 120=ChatGPT
    , readability                                                               INTEGER                                              DEFAULT 0
    , is_explicit                                                               BOOLEAN                                                         -- 0=False 1=True
    , index_status                                                              INTEGER                                              DEFAULT 310        CHECK(index_status IN (310, 320, 330)) -- 1=AUTO 2=MANUAL 3=EXCLUDED
    , episode_count                                                             INTEGER
    , is_deleted                                                                BOOLEAN                                              DEFAULT FALSE
    , advanced_popularity                                                       FLOAT                                                DEFAULT 0   -- used for calculating APS
    , listen_score_global                                                       FLOAT                                       NOT NULL DEFAULT 0   -- used for calculating global ranking
    , reason_for_failure                                                        TEXT                                        NOT NULL
    , file_name                                                                 TEXT
    , record_hash                                                               TEXT                                        NOT NULL           -- hash of the entire file read as a string
    , title_cleaned                                                             TEXT                                           -- to OS
    , publisher                                                                 TEXT                                           -- to OS
    , language                                                                  TEXT                                             -- to OS
    , description_cleaned                                                       TEXT
    , image_url                                                                 TEXT                                           -- to OS
    , rss_url                                                                   TEXT                                          -- to OS
) PARTITION BY HASH (podcast_purgatory_id);
CREATE INDEX idx_purgatory_purgatory_id                                         ON podcast_purgatory                                USING btree (podcast_purgatory_id);
CREATE TABLE podcast_purgatory_00 PARTITION OF podcast_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 0);
CREATE TABLE podcast_purgatory_01 PARTITION OF podcast_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 1);
CREATE TABLE podcast_purgatory_02 PARTITION OF podcast_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 2);
CREATE TABLE podcast_purgatory_03 PARTITION OF podcast_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 3);
CREATE TABLE podcast_purgatory_04 PARTITION OF podcast_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 4);
CREATE TABLE podcast_purgatory_05 PARTITION OF podcast_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 5);
CREATE TABLE podcast_purgatory_06 PARTITION OF podcast_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 6);
CREATE TABLE podcast_purgatory_07 PARTITION OF podcast_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 7);
CREATE TABLE podcast_purgatory_08 PARTITION OF podcast_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 8);
CREATE TABLE podcast_purgatory_09 PARTITION OF podcast_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 9);


DROP TABLE IF EXISTS station_purgatory CASCADE;
CREATE TABLE station_purgatory (
      station_purgatory_id                                                      INTEGER                                    NOT NULL GENERATED BY DEFAULT AS IDENTITY
    , station_uuid                                                              UUID                                                   -- to OS
    , date_created                                                              TIMESTAMPTZ                                 DEFAULT CURRENT_TIMESTAMP
    , date_modified                                                             TIMESTAMPTZ                                 DEFAULT CURRENT_TIMESTAMP
    , reason_for_failure                                                        TEXT                                        NOT NULL
    , title_cleaned                                                             TEXT                                           -- to OS
    , language                                                                  VARCHAR(4)                                             -- to OS
    , description_cleaned                                                       TEXT
    , is_searchable                                                             BOOLEAN                                     NOT NULL
    , PRIMARY KEY (station_purgatory_id)
);
CREATE INDEX idx_station_purgatory_station_purgatory_id                         ON station_purgatory                        USING btree (station_purgatory_id);


DROP TABLE IF EXISTS error_log CASCADE;
CREATE TABLE error_log (
      error_log_id                                                      		BIGINT                                      NOT NULL GENERATED BY DEFAULT AS IDENTITY
    , entity_identifier                                                         TEXT                               			NOT NULL
    , entity_type                                                               SMALLINT                                    NOT NULL
    , error                                                                     TEXT                                        NOT NULL
    , stack_trace                                                               TEXT                                        NOT NULL
    , date_created                                                              TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
	, PRIMARY KEY (error_log_id)
	, CONSTRAINT fk_error_log_entity_type                       				FOREIGN KEY (entity_type)                   REFERENCES struct_type (struct_type_id)
);


DROP TABLE IF EXISTS podcast_quality CASCADE;
CREATE TABLE podcast_quality (
      podcast_quality_id                                                        INTEGER                                     NOT NULL
    , podcast_uuid                                                              UUID                                        NOT NULL        -- to OS
    , date_created                                                              TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , date_modified                                                             TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , description_selected                                                      INTEGER                                     NOT NULL        -- 110=Cleaned, 120=ChatGPT
    , readability                                                               INTEGER                                     NOT NULL DEFAULT 0
    , is_explicit                                                               BOOLEAN                                     NOT NULL
    , index_status                                                              INTEGER                                     NOT NULL DEFAULT 310   CHECK(index_status IN (310, 320, 330)) -- 1=AUTO 2=MANUAL 3=EXCLUDED
    , episode_count                                                             INTEGER                                     NOT NULL
    , is_deleted                                                                BOOLEAN                                     NOT NULL DEFAULT FALSE
    , advanced_popularity                                                       FLOAT                                       NOT NULL DEFAULT 0   -- used for calculating APS
    , listen_score_global                                                       FLOAT                                       NOT NULL DEFAULT 0   -- used for calculating global ranking
    , record_hash                                                               TEXT                                        NOT NULL           -- hash of the entire file read as a string
    , title_cleaned                                                             TEXT                                		NOT NULL        -- to OS
    , title_lemma                                                               TEXT                                		NOT NULL        -- to OS
    , publisher                                                                 TEXT                                                		-- to OS
    , language                                                                  VARCHAR(4)                                  NOT NULL        -- to OS
    , description_cleaned                                                       TEXT                                        NOT NULL
    , description_chatgpt                                                       TEXT
    , description_lemma                                                         TEXT                                        NOT NULL        -- to OS
    , vector                                                                    BYTEA                                       NOT NULL        -- to OS
    , image_url                                                                 TEXT                                                		-- to OS
    , rss_url                                                                   TEXT                                		NOT NULL        -- to OS
--    , PRIMARY KEY (active_id)
--    , CONSTRAINT fk_podcast_quality_index_status                				FOREIGN KEY (index_status)                   REFERENCES struct_type (struct_type_id)
) PARTITION BY HASH (podcast_quality_id);
CREATE INDEX idx_podcast_quality_id                                             ON podcast_quality                          USING btree (podcast_quality_id);
CREATE TABLE podcast_quality_00 PARTITION OF podcast_quality FOR VALUES WITH (MODULUS 10, REMAINDER 0);
CREATE TABLE podcast_quality_01 PARTITION OF podcast_quality FOR VALUES WITH (MODULUS 10, REMAINDER 1);
CREATE TABLE podcast_quality_02 PARTITION OF podcast_quality FOR VALUES WITH (MODULUS 10, REMAINDER 2);
CREATE TABLE podcast_quality_03 PARTITION OF podcast_quality FOR VALUES WITH (MODULUS 10, REMAINDER 3);
CREATE TABLE podcast_quality_04 PARTITION OF podcast_quality FOR VALUES WITH (MODULUS 10, REMAINDER 4);
CREATE TABLE podcast_quality_05 PARTITION OF podcast_quality FOR VALUES WITH (MODULUS 10, REMAINDER 5);
CREATE TABLE podcast_quality_06 PARTITION OF podcast_quality FOR VALUES WITH (MODULUS 10, REMAINDER 6);
CREATE TABLE podcast_quality_07 PARTITION OF podcast_quality FOR VALUES WITH (MODULUS 10, REMAINDER 7);
CREATE TABLE podcast_quality_08 PARTITION OF podcast_quality FOR VALUES WITH (MODULUS 10, REMAINDER 8);
CREATE TABLE podcast_quality_09 PARTITION OF podcast_quality FOR VALUES WITH (MODULUS 10, REMAINDER 9);


DROP TABLE IF EXISTS podcast_quarantine CASCADE;
CREATE TABLE podcast_quarantine (
      podcast_quarantine_id                                                     INTEGER                                     NOT NULL GENERATED BY DEFAULT AS IDENTITY
    , date_processed                                                            TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , podcast_uuid                                                              UUID                                        NOT NULL
    , original_podcast_uuid                                                     UUID                                        NOT NULL
    , duplicate_file_name                                                       TEXT                                		NOT NULL
    , PRIMARY KEY (podcast_quarantine_id)
);
CREATE UNIQUE INDEX idx_podcast_quarantine_podcast_uuid       					ON podcast_quarantine    					USING btree (podcast_uuid);


DROP TABLE IF EXISTS station_quarantine CASCADE;
CREATE TABLE station_quarantine (
      station_quarantine_id                                                     INTEGER                                     NOT NULL GENERATED BY DEFAULT AS IDENTITY
    , date_processed                                                            TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , station_uuid                                                              UUID                                        NOT NULL
    , original_station_uuid                                                     UUID                                        NOT NULL
    , PRIMARY KEY (station_quarantine_id)
);
CREATE UNIQUE INDEX idx_station_quarantine_podcast_uuid       					ON station_quarantine    					USING btree (station_uuid);


DROP TABLE IF EXISTS station_quality CASCADE;
CREATE TABLE station_quality (
       station_quality_id                                                       INTEGER                                     NOT NULL GENERATED BY DEFAULT AS IDENTITY
    ,  station_uuid                                                             UUID                                        NOT NULL
    , date_created                                                              TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , date_modified                                                             TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , is_explicit                                                               BOOLEAN                                     NOT NULL
    , index_status                                                              INTEGER                                     NOT NULL DEFAULT 310   CHECK(index_status IN (310, 320, 330)) -- 1=AUTO 2=MANUAL 3=EXCLUDED
    , advanced_popularity                                                       FLOAT                                       NOT NULL DEFAULT 0   -- used for calculating APS
    , title_cleaned                                                             TEXT                                           				-- to OS
    , title_lemma                                                               TEXT                                		NOT NULL        -- to OS
    , language                                                                  VARCHAR(4)                                  NOT NULL        -- to OS
    , description_cleaned                                                       TEXT
    , description_lemma                                                         TEXT                                        NOT NULL        -- to OS
    , vector                                                                    BYTEA                                       NOT NULL        -- to OS
    , image_url                                                                 TEXT                                                		-- to OS
    , is_searchable                                                             BOOLEAN                                     NOT NULL
    , PRIMARY KEY (station_quality_id)
);
CREATE UNIQUE INDEX idx_station_quality_station_uuid       					ON station_quality    					USING btree (station_uuid);

DROP TABLE IF EXISTS episode_quality CASCADE;
CREATE TABLE episode_quality (
      episode_quality_id                                                        INTEGER                                     NOT NULL
    , episode_uuid                                                              UUID                                        NOT NULL        -- to OS
    , uuid_selector                                                             INTEGER                                     NOT NULL DEFAULT 410   CHECK(index_status IN (410, 420, 430)) -- 410 -> Native, 420 -> String, 430 -> URL
    , date_created                                                              TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , date_modified                                                             TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , description_selected                                                      INTEGER                                     NOT NULL        -- 110=Cleaned, 120=ChatGPT
    , readability                                                               INTEGER                                     NOT NULL DEFAULT 0
    , is_explicit                                                               BOOLEAN                                     NOT NULL
    , index_status                                                              INTEGER                                     NOT NULL DEFAULT 310   CHECK(index_status IN (310, 320, 330)) -- 1=AUTO 2=MANUAL 3=EXCLUDED
    , is_deleted                                                                BOOLEAN                                     NOT NULL DEFAULT FALSE
    , advanced_popularity                                                       FLOAT                                       NOT NULL DEFAULT 0   -- used for calculating APS
    , record_hash                                                               TEXT                                        NOT NULL           -- hash of the entire file read as a string
    , title_cleaned                                                             TEXT                                		NOT NULL        -- to OS
    , title_lemma                                                               TEXT                                		NOT NULL        -- to OS
    , publisher                                                                 TEXT                                                		-- to OS
    , language                                                                  VARCHAR(4)                                  NOT NULL        -- to OS
    , description_cleaned                                                       TEXT                                        NOT NULL
    , description_chatgpt                                                       TEXT
    , description_lemma                                                         TEXT                                        NOT NULL        -- to OS
    , vector                                                                    BYTEA                                       NOT NULL        -- to OS
    , image_url                                                                 TEXT                                                		-- to OS
    , duration                                                                  BIGINT
    , file_type                                                                 VARCHAR(32)
    , podcast_uuid                                                              UUID                                        NOT NULL
    , episode_url                                                               TEXT
    , publish_date                                                              BIGINT
--    , PRIMARY KEY (active_id)
--    , CONSTRAINT fk_episode_quality_index_status                				FOREIGN KEY (index_status)                   REFERENCES struct_type (struct_type_id)
) PARTITION BY HASH (podcast_quality_id);
CREATE INDEX idx_episode_quality_id                                             ON episode_quality                          USING btree (episode_quality_id);
CREATE TABLE episode_quality_00 PARTITION OF episode_quality FOR VALUES WITH (MODULUS 10, REMAINDER 0);
CREATE TABLE episode_quality_01 PARTITION OF episode_quality FOR VALUES WITH (MODULUS 10, REMAINDER 1);
CREATE TABLE episode_quality_02 PARTITION OF episode_quality FOR VALUES WITH (MODULUS 10, REMAINDER 2);
CREATE TABLE episode_quality_03 PARTITION OF episode_quality FOR VALUES WITH (MODULUS 10, REMAINDER 3);
CREATE TABLE episode_quality_04 PARTITION OF episode_quality FOR VALUES WITH (MODULUS 10, REMAINDER 4);
CREATE TABLE episode_quality_05 PARTITION OF episode_quality FOR VALUES WITH (MODULUS 10, REMAINDER 5);
CREATE TABLE episode_quality_06 PARTITION OF episode_quality FOR VALUES WITH (MODULUS 10, REMAINDER 6);
CREATE TABLE episode_quality_07 PARTITION OF episode_quality FOR VALUES WITH (MODULUS 10, REMAINDER 7);
CREATE TABLE episode_quality_08 PARTITION OF episode_quality FOR VALUES WITH (MODULUS 10, REMAINDER 8);
CREATE TABLE episode_quality_09 PARTITION OF episode_quality FOR VALUES WITH (MODULUS 10, REMAINDER 9);

DROP TABLE IF EXISTS episode_purgatory CASCADE;
CREATE TABLE episode_purgatory (
      episode_purgatory_id                                                      INTEGER                                     NOT NULL
    , episode_uuid                                                              UUID                                        NOT NULL        -- to OS
    , date_created                                                              TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , date_modified                                                             TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , description_selected                                                      INTEGER                                     NOT NULL        -- 110=Cleaned, 120=ChatGPT
    , readability                                                               INTEGER                                     NOT NULL DEFAULT 0
    , is_explicit                                                               BOOLEAN                                     NOT NULL
    , index_status                                                              INTEGER                                     NOT NULL DEFAULT 310   CHECK(index_status IN (310, 320, 330)) -- 1=AUTO 2=MANUAL 3=EXCLUDED
    , is_deleted                                                                BOOLEAN                                     NOT NULL DEFAULT FALSE
    , advanced_popularity                                                       FLOAT                                       NOT NULL DEFAULT 0   -- used for calculating APS
    , record_hash                                                               TEXT                                        NOT NULL           -- hash of the entire file read as a string
    , title_cleaned                                                             TEXT                                		NOT NULL        -- to OS
    , publisher                                                                 TEXT                                                		-- to OS
    , language                                                                  VARCHAR(4)                                  NOT NULL        -- to OS
    , description_cleaned                                                       TEXT                                        NOT NULL
    , image_url                                                                 TEXT                                                		-- to OS
    , duration                                                                  BIGINT
    , file_type                                                                 VARCHAR(32)
    , podcast_uuid                                                              UUID                                        NOT NULL
    , episode_url                                                               TEXT
    , publish_date                                                              BIGINT
--    , PRIMARY KEY (active_id)
--    , CONSTRAINT fk_episode_quality_index_status                				FOREIGN KEY (index_status)                   REFERENCES struct_type (struct_type_id)
) PARTITION BY HASH (podcast_purgatory_id);
CREATE INDEX idx_episode_purgatory_id                                             ON episode_purgatory                          USING btree (episode_purgatory_id);
CREATE TABLE episode_purgatory_00 PARTITION OF episode_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 0);
CREATE TABLE episode_purgatory_01 PARTITION OF episode_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 1);
CREATE TABLE episode_purgatory_02 PARTITION OF episode_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 2);
CREATE TABLE episode_purgatory_03 PARTITION OF episode_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 3);
CREATE TABLE episode_purgatory_04 PARTITION OF episode_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 4);
CREATE TABLE episode_purgatory_05 PARTITION OF episode_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 5);
CREATE TABLE episode_purgatory_06 PARTITION OF episode_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 6);
CREATE TABLE episode_purgatory_07 PARTITION OF episode_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 7);
CREATE TABLE episode_purgatory_08 PARTITION OF episode_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 8);
CREATE TABLE episode_purgatory_09 PARTITION OF episode_purgatory FOR VALUES WITH (MODULUS 10, REMAINDER 9);