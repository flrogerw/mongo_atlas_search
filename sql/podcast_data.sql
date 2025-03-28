-- vim: set ts=4 expandtab
SET client_min_messages TO WARNING;
-- SET ROLE=:XXXX_admin;

-- DROP SCHEMA IF EXISTS podcast CASCADE;
-- CREATE SCHEMA podcast;

SET search_path TO podcast;


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
	, (410, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 10, 'guid_select',	'0',	'Native')
	, (420, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 20, 'guid_select',	'1',	'String')
	, (430, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 30, 'guid_select',	'2',	'URL')
	, (510, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 10, 'entity_type',	'1',	'Podcast')
	, (520, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 20, 'entity_type',	'2',	'Episode')
	, (530, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 30, 'entity_type',	'3',	'Show')
	, (540, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 40, 'entity_type',	'4',	'Chapter')
	, (550, '2024-01-01 01:23:45', '2024-01-01 01:23:45', 50, 'entity_type',	'5',	'Station')
;
