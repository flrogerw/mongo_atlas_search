DROP TABLE IF EXISTS struct_type;
CREATE TABLE struct_type (
      struct_type_id                                                            INTEGER                                     NOT NULL PRIMARY KEY AUTOINCREMENT
    , date_created                                                              TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , date_modified                                                             TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , display_order                                                             SMALLINT                                    NOT NULL
    , group_name                                                                TEXT                                        NOT NULL
    , att_pub_ident                                                             TEXT                                        NOT NULL
    , att_value                                                                 TEXT                                        NOT NULL
);
CREATE UNIQUE INDEX idx_stuct_type_att_pub_ident                                ON struct_type                              (group_name, att_pub_ident);
INSERT INTO struct_type (struct_type_id, date_created, date_modified, display_order, group_name, att_pub_ident, att_value)
VALUES
	  (110, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 10, 'desc_select',	'110',	'Cleaned')
	, (120, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 20, 'desc_select',	'120',	'ChatGPT')
	, (210, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 10, 'explicit',		'0',	'False')
	, (220, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 20, 'explicit',		'1',	'True')
	, (230, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 30, 'explicit',		'2',	'Indeterminate')
	, (310, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 10, 'index_status',	'0',	'Auto')
	, (320, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 20, 'index_status',	'1',	'Manual')
	, (330, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 30, 'index_status',	'2',	'Excluded')
;

DROP TABLE IF EXISTS podcast_purgatory;
CREATE TABLE podcast_purgatory (
/*
      file_name             													VARCHAR(100)    							NOT NULL
    , title                 													VARCHAR(45)     							NOT NULL
    , description           													TEXT            							NOT NULL
	, date_processed        													TIMESTAMPTZ     							NOT NULL DEFAULT CURRENT_TIMESTAMP
*/
      podcast_purgatory_id     													INTEGER         									 PRIMARY KEY AUTOINCREMENT
    , date_created                                                              TIMESTAMPTZ                                 		 DEFAULT CURRENT_TIMESTAMP
    , date_modified                                                             TIMESTAMPTZ                                 		 DEFAULT CURRENT_TIMESTAMP
	, description_selected  													INTEGER         									    		-- 110=Cleaned, 120=ChatGPT
	, readability           													INTEGER         									 DEFAULT 0
    , is_explicit       														INTEGER         									        	-- 0=False 1=True
	, index_status        														INTEGER         									 DEFAULT 310		CHECK(index_status IN (310, 320, 330)) -- 1=AUTO 2=MANUAL 3=EXCLUDED
	, episode_count         													INTEGER
	, is_deleted            													INTEGER         									 DEFAULT 0   -- 0=False 1=True
	, advanced_popularity   													FLOAT         										 DEFAULT 1   -- used for calculating APS
	, reason_for_failure    													TEXT            							NOT NULL
    , file_name             													VARCHAR(45)
	, title_cleaned         													VARCHAR(100)    								       -- to OS
	, author                													VARCHAR(100)    								       -- to OS
	, language              													VARCHAR(4)      								       -- to OS
	, description_cleaned   													TEXT
	, image_url             													VARCHAR(256)    								       -- to OS
	, podcast_uuid          													VARCHAR(32)     								       -- to OS
	, podcast_url           													VARCHAR(128)    								       -- to OS
	, md5_description       													VARCHAR(32)
	, md5_title             													VARCHAR(32)
	, md5_podcast_url       													VARCHAR(32)
);

DROP TABLE IF EXISTS error_log;
CREATE TABLE error_log (
      file_name             													VARCHAR(45)     							NOT NULL
	, error                 													TEXT            							NOT NULL
	, stack_trace                 												TEXT            							NOT NULL
	, date_created        													    TIMESTAMPTZ     							NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS podcast_active;
CREATE TABLE podcast_active (
      podcast_active_id     													INTEGER         							NOT NULL PRIMARY KEY AUTOINCREMENT
    , date_created                                                              TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
    , date_modified                                                             TIMESTAMPTZ                                 NOT NULL DEFAULT CURRENT_TIMESTAMP
	, description_selected  													INTEGER         							NOT NULL		-- 110=Cleaned, 120=ChatGPT
	, readability           													INTEGER         							NOT NULL DEFAULT 0
    , is_explicit       														INTEGER         							NOT NULL        -- 0=False 1=True
	, index_status        														INTEGER         							NOT NULL DEFAULT 1   CHECK(index_status IN (310, 320, 330)) -- 1=AUTO 2=MANUAL 3=EXCLUDED
	, episode_count         													INTEGER         							NOT NULL
	, is_deleted            													INTEGER         							NOT NULL DEFAULT 0   -- 0=False 1=True
	, advanced_popularity   													FLOAT         								NOT NULL DEFAULT 1   -- used for calculating APS
    , file_name             													VARCHAR(45)     							NOT NULL
	, title_cleaned         													VARCHAR(100)    							NOT NULL        -- to OS
	, title_lemma           													VARCHAR(100)    							NOT NULL        -- to OS
	, author                													VARCHAR(100)    							                -- to OS
	, language              													VARCHAR(4)      							NOT NULL        -- to OS
	, description_cleaned   													TEXT            							NOT NULL
	, description_chatgpt   													TEXT
	, description_lemma     													TEXT            							NOT NULL        -- to OS
	, description_vector    													BLOB            							NOT NULL        -- to OS
	, image_url             													VARCHAR(256)    							                -- to OS
	, podcast_uuid          													VARCHAR(32)     							NOT NULL        -- to OS
	, podcast_url           													VARCHAR(128)    							NOT NULL        -- to OS
	, md5_description       													VARCHAR(32)     							NOT NULL
	, md5_title             													VARCHAR(32)     							NOT NULL
	, md5_podcast_url       													VARCHAR(32)     							NOT NULL
);
CREATE UNIQUE INDEX idx_md5_file_name       ON podcast_active	(file_name);
CREATE UNIQUE INDEX idx_md5_podcast_uuid    ON podcast_active	(podcast_uuid);
CREATE UNIQUE INDEX idx_md5_podcast_url     ON podcast_active	(md5_podcast_url);


DROP TABLE IF EXISTS quarantine;
CREATE TABLE quarantine (
      quarantine_id         													INTEGER         							NOT NULL PRIMARY KEY AUTOINCREMENT
	, podcast_uuid          													VARCHAR(32)     							NOT NULL
	, original_podcast_uuid 													VARCHAR(32)     							NOT NULL
	, duplicate_file_name   													VARCHAR(45)     							NOT NULL
	, date_processed        													TIMESTAMPTZ     							NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX idx_podcast_uuid       ON quarantine    (podcast_uuid);


CREATE TABLE station (
	  station_uuid																UUID																					NOT NULL PRIMARY KEY
	, station_name																TEXT																					NOT NULL
	, call_sign																	TEXT																					NOT NULL
	, station_id																INTEGER																					NOT NULL
	, category_id																SMALLINT
	, format_id																	SMALLINT
	, organization_id															SMALLINT
	, band_id																	SMALLINT
	, geo_position																POINT
	, date_acquired																DATE
	, date_disposed																DATE
	, has_jump2go																BOOLEAN
	, is_searchable																BOOLEAN
	, frequency																	TEXT
	, website																	TEXT
	, slug																		TEXT
	, phonetic_name																TEXT
	, slogan																	TEXT
	, description																TEXT
	, date_created																TIMESTAMPTZ
	, date_modified																TIMESTAMPTZ
	, site_slug																	TEXT
	, seo_description															TEXT
	, status																	BOOLEAN
	, is_explicit																BOOLEAN
	, publish_state_id															INTEGER
);

