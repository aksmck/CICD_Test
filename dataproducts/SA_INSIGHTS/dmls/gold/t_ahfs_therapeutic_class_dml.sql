%sql
/*
Table Name: t_ahfs_therapeutic_class
Source: psas_fdp_usp_silver.t_ahfs_therapeutic_class
Target: psas_fdp_usp_gold.t_ahfs_therapeutic_class
Primary Key: thrputc_class_cd
Description: This is a DML script for the t_ahfs_therapeutic_class table, transfer of data from the t_ahfs_therapeutic_class Silver table to the t_ahfs_therapeutic_class Gold table.
Matches records based on THRPUTC_CLASS_CD for deletes, updates or inserts if no match is found.
*/
MERGE INTO $$target_table_name AS target using (
  SELECT
    *
  FROM
    (
      SELECT
        $$silver_temp_table_name. *,
        $$silver_temp_table_name.thrputc_class_cd AS merge_key,
        ROW_NUMBER() OVER(
          PARTITION BY $$silver_temp_table_name.thrputc_class_cd
          ORDER BY
            $$silver_temp_table_name.DATE_PART DESC,
            $$silver_temp_table_name.HOUR_PART DESC
        ) AS rn
      FROM
        $$silver_temp_table_name
      WHERE
        $$silver_temp_table_name._change_type not in ('update_preimage','delete')
    )
  WHERE
    rn = 1
  UNION
  SELECT
    *
  FROM
    (
      SELECT
        $$silver_temp_table_name. *,
        'Null' AS merge_key,
        ROW_NUMBER() OVER(
          PARTITION BY $$silver_temp_table_name.thrputc_class_cd
          ORDER BY
            $$silver_temp_table_name.DATE_PART DESC,
            $$silver_temp_table_name.HOUR_PART DESC
        ) AS rn
      FROM
        $$silver_temp_table_name
        JOIN $$target_table_name ON $$silver_temp_table_name.thrputc_class_cd = $$target_table_name.thrputc_class_cd
      WHERE
        $$target_table_name.expiration_date = to_date('99991231', 'yyyyMMdd')
        AND $$target_table_name.integration_key != $$silver_temp_table_name.integration_key
        AND $$silver_temp_table_name._change_type not in ('update_preimage','delete')
    )
  WHERE
    rn = 1
) source ON target.thrputc_class_cd = source.merge_key
WHEN matched
AND target.expiration_date = to_date('99991231', 'yyyyMMdd')
AND target.integration_key != source.integration_key THEN
UPDATE
SET
  target.expiration_date = date_sub(current_date(), 1),
  target.curr_flg = 'N',
  target.record_load_time = CURRENT_TIMESTAMP(),
  target.adf_run_id = source.adf_run_id,
  target.adf_job_id = source.adf_job_id,
  target.databricks_run_id = '$$DATABRICKS_RUN_ID',
  target.databricks_job_id = '$$DATABRICKS_JOB_ID',
  target.business_unit = source.business_unit,
  target.segment_name = source.segment_name,
  target.source_system_name = source.source_system_name,
  target.update_ts = CURRENT_TIMESTAMP(),
  target.date_part = Date(CURRENT_TIMESTAMP()),
  target.hour_part = Hour(CURRENT_TIMESTAMP())
  WHEN matched
  AND target.integration_key = source.integration_key
  AND target.curr_flg = 'Y' THEN
UPDATE
SET
  target.record_load_time = CURRENT_TIMESTAMP(),
  target.adf_run_id = source.adf_run_id,
  target.adf_job_id = source.adf_job_id,
  target.databricks_run_id = '$$DATABRICKS_RUN_ID',
  target.databricks_job_id = '$$DATABRICKS_JOB_ID',
  target.business_unit = source.business_unit,
  target.segment_name = source.segment_name,
  target.source_system_name = source.source_system_name,
  target.update_ts = CURRENT_TIMESTAMP(),
  target.date_part = Date(CURRENT_TIMESTAMP()),
  target.hour_part = Hour(CURRENT_TIMESTAMP())
  WHEN NOT matched BY target
  AND source.merge_key != 'Null' THEN
INSERT
  (
    thrputc_class_cd,
    thrputc_class_dscrptn,
    effective_date,
    expiration_date,
    curr_flg,
    adf_run_id,
    adf_job_id,
    databricks_run_id,
    databricks_job_id,
    input_file_name,
    business_unit,
    segment_name,
    source_system_name,
    integration_key,
    insert_ts,
    update_ts
  )
VALUES
  (
    source.thrputc_class_cd,
    source.thrputc_class_dscrptn,
    to_date('00000101', 'yyyyMMdd'),
    to_date('99991231', 'yyyyMMdd'),
    'Y',
    source.adf_run_id,
    source.adf_job_id,
    '$$DATABRICKS_RUN_ID',
    '$$DATABRICKS_JOB_ID',
    source.input_file_name,
    source.business_unit,
    source.segment_name,
    source.source_system_name,
    source.integration_key,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  )
  WHEN NOT matched BY target
  AND source.merge_key = 'Null' THEN
INSERT
  (
    thrputc_class_cd,
    thrputc_class_dscrptn,
    effective_date,
    expiration_date,
    curr_flg,
    adf_run_id,
    adf_job_id,
    databricks_run_id,
    databricks_job_id,
    input_file_name,
    business_unit,
    segment_name,
    source_system_name,
    integration_key,
    insert_ts,
    update_ts
  )
VALUES
  (
    source.thrputc_class_cd,
    source.thrputc_class_dscrptn,
    current_date(),
    to_date('99991231', 'yyyyMMdd'),
    'Y',
    source.adf_run_id,
    source.adf_job_id,
    '$$DATABRICKS_RUN_ID',
    '$$DATABRICKS_JOB_ID',
    source.input_file_name,
    source.business_unit,
    source.segment_name,
    source.source_system_name,
    source.integration_key,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  )
  WHEN NOT matched BY source
  AND target.expiration_date = to_date('99991231', 'yyyyMMdd')
  AND target.curr_flg != 'N' THEN
update
SET
  target.expiration_date = date_sub(current_date(), 1),
  target.curr_flg = 'N',
  target.record_load_time = CURRENT_TIMESTAMP(),
  target.databricks_run_id = '$$DATABRICKS_RUN_ID',
  target.databricks_job_id = '$$DATABRICKS_JOB_ID',
  target.update_ts = CURRENT_TIMESTAMP(),
  target.date_part = date(CURRENT_TIMESTAMP()),
  target.hour_part = hour(CURRENT_TIMESTAMP())