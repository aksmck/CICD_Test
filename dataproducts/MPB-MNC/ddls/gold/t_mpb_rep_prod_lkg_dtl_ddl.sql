CREATE TABLE IF NOT EXISTS $$unity_catalog_name.$$schema_name.$$delta_table_name (
  BUSINESS_UNIT STRING COMMENT 'Business Unit',
  SEGMENT STRING COMMENT 'Segment Name',
  SLS_REP STRING COMMENT 'Sales Representative Name; Source : SLS_REP',
  PROD_NAME STRING COMMENT 'Product Name; Source : PROD_NAME',
  MNC_PROD_ACCESS STRING COMMENT 'Membership Network Code; Source : MNC_CD',
  LKG_RANK INT COMMENT 'Leakage Rank; Source : LKG_RANK',
  PROD_ACCESS_CTGRY STRING COMMENT 'Product Access Category; Source : PROD_ACCESS_CTGRY',
  YR_MTH STRING COMMENT 'Year and Month; Source : YR_MTH',
  LKG_RANK_WGHT INT COMMENT 'Leakage Rank Weight',
  RECORD_LOAD_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  DATABRICKS_RUN_ID STRING COMMENT 'run id of the Databricks job run. This gets generated from the silver notebook run',
  DATABRICKS_JOB_ID STRING COMMENT 'job id of the Databricks job run. This gets generated from the silver notebook run',
  INSERT_TS TIMESTAMP COMMENT 'Timestamp when the Record was Inserted',
  UPDATE_TS TIMESTAMP COMMENT 'Timestamp when the Record was Inserted'
)USING DELTA LOCATION '$$delta_table_location' TBLPROPERTIES (
'delta.feature.allowColumnDefaults' = 'supported',
'delta.feature.appendOnly' = 'supported',
'delta.feature.invariants' = 'supported',
'delta.minReaderVersion' = '1',
'delta.minWriterVersion' = '7',
'delta.enableChangeDataFeed' = true,
'spark.sql.files.ignoreMissingFiles' = true,
'delta.autoOptimize.optimizeWrite' = true);