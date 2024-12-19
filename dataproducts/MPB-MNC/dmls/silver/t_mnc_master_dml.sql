MERGE INTO $$target_table_name AS target
USING (
  SELECT
    *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY $$bronze_temp_table_name.MNC_CD, $$bronze_temp_table_name.BRAND
        ORDER BY
          substr($$bronze_temp_table_name.INPUT_FILE_NAME, instr($$bronze_temp_table_name.INPUT_FILE_NAME, 'day=') + 4, 10) DESC,
          substr($$bronze_temp_table_name.INPUT_FILE_NAME, instr($$bronze_temp_table_name.INPUT_FILE_NAME, 'hr=') + 3, 2) DESC
      ) AS rn
    FROM
      $$bronze_temp_table_name
  ) subquery
  WHERE rn = 1
) AS source
ON concat_ws('-',
  coalesce(target.MNC_CD, ''),
  coalesce(target.BRAND, '')
) = concat_ws('-',
  coalesce(target.MNC_CD, ''),
  coalesce(target.BRAND, '')
)
WHEN MATCHED THEN
UPDATE SET
  target.MNC_CD = source.MNC_CD,
  target.BRAND = source.BRAND,
  target._rescued_data = source._rescued_data,
  target.RECORD_LOAD_TIME = current_timestamp(),
  target.ADF_RUN_ID = source.ADF_RUN_ID,
  target.ADF_JOB_ID = source.ADF_JOB_ID,
  target.DATABRICKS_RUN_ID = '$$DATABRICKS_RUN_ID',
  target.DATABRICKS_JOB_ID = '$$DATABRICKS_JOB_ID',
  target.INPUT_FILE_NAME = source.INPUT_FILE_NAME,
  target.INTEGRATION_KEY = source.INTEGRATION_KEY,
  target.UPDATE_TS = current_timestamp(),
  target.DATE_PART = DATE(current_timestamp()),
  target.HOUR_PART = HOUR(current_timestamp())
WHEN NOT MATCHED BY target THEN
INSERT (
  MNC_CD,
  BRAND,
	_rescued_data,
	RECORD_LOAD_TIME,
	ADF_RUN_ID,
	ADF_JOB_ID,
	DATABRICKS_RUN_ID,
	DATABRICKS_JOB_ID,
	INPUT_FILE_NAME,
	INTEGRATION_KEY,
	INSERT_TS,
	UPDATE_TS
)
VALUES (
  source.MNC_CD,
  source.BRAND,
  source._rescued_data,
  current_timestamp(),
  source.ADF_RUN_ID,
  source.ADF_JOB_ID,
  '$$DATABRICKS_RUN_ID',
  '$$DATABRICKS_JOB_ID',
  source.INPUT_FILE_NAME,
  source.INTEGRATION_KEY,
  current_timestamp(),
  current_timestamp()
);