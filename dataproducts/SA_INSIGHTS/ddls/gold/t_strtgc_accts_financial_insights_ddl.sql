CREATE TABLE IF NOT EXISTS $$unity_catalog_name.$$schema_name.$$delta_table_name (
  BUSINESS_UNIT STRING COMMENT 'Business Unit'
  , SEGMENT STRING COMMENT 'Segment Name'
  , CHNNL STRING COMMENT 'Company Code or Business Units'
  , CUST_CHN_ID STRING COMMENT 'Customer Chain Identifier'
  , CUST_CHN_NAME STRING COMMENT 'Customer chain name'
  , PROD_HIER_1_NUM STRING COMMENT 'Product hierarchy 1 Number'
  , SLS_CTGRY_CD STRING COMMENT 'Category Code of sales'
  , PROD_CTGRY STRING COMMENT 'Product category'
  , PROD_SUB_CTGRY STRING COMMENT 'Product sub category'
  , PRGRM_NAME STRING COMMENT 'Type of drug'
  , THRPUTC_CLASS_CD STRING COMMENT 'Therapeutic Class Code'
  , THRPUTC_CLASS_DSCRPTN STRING COMMENT 'Therapeutic Class Description'
  , NDC_NUM STRING COMMENT 'National Drug Code Number'
  , EM_ITEM_NUM STRING COMMENT 'EM item number'
  , SELL_DSCRPTN STRING COMMENT 'Material Name Navigational/Sell Description'
  , TIME_BUCKET_ID STRING COMMENT 'Time bucket identifier'
  , CUST_CHN_GRP STRING COMMENT 'Customer chain group'
  , BRND_NAME STRING COMMENT 'Brand Name of the Drug'
  , TIME_BUCKET_START_DT DATE COMMENT 'Start date of the time bucket'
  , TIME_BUCKET_END_DT DATE COMMENT 'End date of the time bucket'
  , PREV_TIME_BUCKET_START_DT DATE COMMENT 'Start date of the previous time bucket'
  , PREV_TIME_BUCKET_END_DT DATE COMMENT 'End date of the previous time bucket'
  , SELL_DAYS_CURR DECIMAL(38,0) COMMENT 'Current sell days'
  , SELL_DAYS_PREV DECIMAL(38,0) COMMENT 'Previous sell days'
  , STNDRD_COST_CURR DECIMAL(19,2) COMMENT 'Current standard cost'
  , TOTAL_EXPNS_CURR DECIMAL(19,2) COMMENT 'Current total expenses'
  , ANNUAL_OPER_PROFIT_CURR DECIMAL(19,2) COMMENT 'Current annual operating profit'
  , NET_REVENUE_CURR DECIMAL(19,2) COMMENT 'Current net revenue'
  , GROSS_PROFIT_CURR DECIMAL(19,2) COMMENT 'Current gross profit'
  , STNDRD_COST_PREV DECIMAL(19,2) COMMENT 'Previous standard cost'
  , TOTAL_EXPNS_PREV DECIMAL(19,2) COMMENT 'Previous total expenses'
  , ANNUAL_OPER_PROFIT_PREV DECIMAL(19,2) COMMENT 'Previous annual operating profit'
  , NET_REVENUE_PREV DECIMAL(19,2) COMMENT 'Previous net revenue'
  , GROSS_PROFIT_PREV DECIMAL(19,2) COMMENT 'Previous gross profit'
  , RECORD_LOAD_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table'
  , DATABRICKS_RUN_ID STRING COMMENT 'run id of the Databricks job run. This gets generated from the silver notebook run'
  , DATABRICKS_JOB_ID STRING COMMENT 'job id of the Databricks job run. This gets generated from the silver notebook run'
  , INSERT_TS TIMESTAMP COMMENT 'Timestamp when the Record was Inserted'
  , UPDATE_TS TIMESTAMP COMMENT 'Timestamp when the Record was Inserted'
)USING DELTA LOCATION '$$delta_table_location' TBLPROPERTIES (
'delta.feature.allowColumnDefaults' = 'supported',
'delta.feature.appendOnly' = 'supported',
'delta.feature.invariants' = 'supported',
'delta.minReaderVersion' = '1',
'delta.minWriterVersion' = '7',
'delta.enableChangeDataFeed' = true,
'spark.sql.files.ignoreMissingFiles' = true,
'delta.autoOptimize.optimizeWrite' = true);