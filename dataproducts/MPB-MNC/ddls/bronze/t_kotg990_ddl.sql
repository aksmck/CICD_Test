CREATE TABLE IF NOT EXISTS $$unity_catalog_name.$$schema_name.$$delta_table_name (
  MANDT STRING COMMENT 'Client',
  KAPPL STRING COMMENT 'Application',
  KSCHL STRING COMMENT 'Material listing/exclusion type',
  VKORG STRING COMMENT 'Customer Sales Organization',
  KUNNR STRING COMMENT 'Customer Account ID',
  YYMEM_NETWRK STRING COMMENT 'Membership Network Code',
  DATBI STRING COMMENT 'Expiration Date',
  DATAB STRING COMMENT 'Effective Date',
  _rescued_data STRING,
  ADF_RUN_ID STRING COMMENT 'ID for specific pipeline run loaded from landing. This run_id gets generated when the records gets loaded from source to landing',
  ADF_JOB_ID STRING COMMENT 'ID of the trigger that invokes the pieline. This job_id gets generated when the records gets loaded from source to landing',
  RECORD_LOAD_TIME TIMESTAMP DEFAULT current_timestamp COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  INPUT_FILE_NAME STRING COMMENT 'File name which is getting populated from bronze layer',
  DATABRICKS_RUN_ID STRING COMMENT 'run id of the Databricks job run. This gets generated from the silver notebook run',
  DATABRICKS_JOB_ID STRING COMMENT 'job id of the Databricks job run. This gets generated from the silver notebook run',
  DATE_PART DATE GENERATED ALWAYS AS (DATE(RECORD_LOAD_TIME)) COMMENT 'This the date part generated from Record load time column',
  HOUR_PART INT GENERATED ALWAYS AS (HOUR(RECORD_LOAD_TIME)) COMMENT 'This the hour  part generated from Record load time column'
  ) USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION '$$delta_table_location' 
    TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.feature.appendOnly' = 'supported',
    'delta.feature.invariants' = 'supported',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '7',
    'spark.sql.files.ignoreMissingFiles'=true);