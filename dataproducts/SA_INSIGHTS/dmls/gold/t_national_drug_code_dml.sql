%sql
/*
Table Name: t_national_drug_code
Source: psas_fdp_usp_silver.t_national_drug_code
Target: psas_fdp_usp_gold.t_national_drug_code
Primary Key: ndc_num
Description: This is a DML script for the t_national_drug_code table, transfer of data from the t_national_drug_code Silver table to the t_national_drug_code Gold table.
Matches records based on NDC_NUM for deletes, updates or inserts if no match is found.
*/
MERGE INTO $$target_table_name AS target using (
  SELECT
    *
  FROM
    (
      SELECT
        $$silver_temp_table_name. *,
        $$silver_temp_table_name.ndc_num AS merge_key,
        ROW_NUMBER() OVER(
          PARTITION BY $$silver_temp_table_name.ndc_num
          ORDER BY
            $$silver_temp_table_name.DATE_PART DESC,
            $$silver_temp_table_name.HOUR_PART DESC
        ) AS rn
      FROM
        $$silver_temp_table_name
      WHERE
        $$silver_temp_table_name._change_type != 'update_preimage'
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
          PARTITION BY $$silver_temp_table_name.ndc_num
          ORDER BY
            $$silver_temp_table_name.DATE_PART DESC,
            $$silver_temp_table_name.HOUR_PART DESC
        ) AS rn
      FROM
        $$silver_temp_table_name
        JOIN $$target_table_name ON $$silver_temp_table_name.ndc_num = $$target_table_name.ndc_num
      WHERE
        $$target_table_name.expiration_date = to_date('99991231', 'yyyyMMdd')
        AND $$target_table_name.integration_key != $$silver_temp_table_name.integration_key
        AND $$silver_temp_table_name._change_type != 'update_preimage'
    )
  WHERE
    rn = 1
) source ON target.ndc_num = source.merge_key
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
    ndc_num,
    gnrc_name,
    prev_ndc_num,
    obsolete_dt,
    dose_frm_dscrptn,
    drg_strnth_dscrptn,
    gnrc_ndctr,
    gnrc_cd_num,
    thrputc_class_cd,
    specif_thrputc_class_cd,
    pkg_sz,
    drg_frm_cd,
    admin_rte_cd,
    gnrc_cd_num_seq,
    repl_ndc_num,
    dea_cd,
    adtl_dscrptn,
    brnd_name,
    gnrc_prc_ndctr,
    lbl_nam,
    mfr_nam,
    ndc_cnfg_ndctr,
    pkg_dscrptn,
    rte_dscrtn,
    top_200_ndctr,
    unit_dose_ndctr,
    ffp_ul_curr_effective_dt,
    ffp_ul_curr_unit_prc,
    gnrc_thrputc_class_cd,
    stndrd_thrputc_class_cd,
    drg_class_cd,
    ingr_cd_num,
    orange_bk_cd,
    curr_blu_bk_effective_dt,
    curr_blu_bk_unit_prc,
    curr_blu_bk_pkg_dt,
    curr_blu_bk_pkg_prc,
    lblr_id,
    patent_expiration_dt,
    exclsvty_expiration_dt,
    drg_strnth_num,
    shlf_pack_num,
    ptnt_pkg_insert_ndctr,
    dispense_cnt,
    -- upd_dt,
    -- crte_dt,
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
    source.ndc_num,
    source.gnrc_name,
    source.prev_ndc_num,
    source.obsolete_dt,
    source.dose_frm_dscrptn,
    source.drg_strnth_dscrptn,
    source.gnrc_ndctr,
    source.gnrc_cd_num,
    source.thrputc_class_cd,
    source.specif_thrputc_class_cd,
    source.pkg_sz,
    source.drg_frm_cd,
    source.admin_rte_cd,
    source.gnrc_cd_num_seq,
    source.repl_ndc_num,
    source.dea_cd,
    source.adtl_dscrptn,
    source.brnd_name,
    source.gnrc_prc_ndctr,
    source.lbl_nam,
    source.mfr_nam,
    source.ndc_cnfg_ndctr,
    source.pkg_dscrptn,
    source.rte_dscrtn,
    source.top_200_ndctr,
    source.unit_dose_ndctr,
    source.ffp_ul_curr_effective_dt,
    source.ffp_ul_curr_unit_prc,
    source.gnrc_thrputc_class_cd,
    source.stndrd_thrputc_class_cd,
    source.drg_class_cd,
    source.ingr_cd_num,
    source.orange_bk_cd,
    source.curr_blu_bk_effective_dt,
    source.curr_blu_bk_unit_prc,
    source.curr_blu_bk_pkg_dt,
    source.curr_blu_bk_pkg_prc,
    source.lblr_id,
    source.patent_expiration_dt,
    source.exclsvty_expiration_dt,
    source.drg_strnth_num,
    source.shlf_pack_num,
    source.ptnt_pkg_insert_ndctr,
    source.dispense_cnt,
    -- source.upd_dt,
    -- source.crte_dt,
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
    ndc_num,
    gnrc_name,
    prev_ndc_num,
    obsolete_dt,
    dose_frm_dscrptn,
    drg_strnth_dscrptn,
    gnrc_ndctr,
    gnrc_cd_num,
    thrputc_class_cd,
    specif_thrputc_class_cd,
    pkg_sz,
    drg_frm_cd,
    admin_rte_cd,
    gnrc_cd_num_seq,
    repl_ndc_num,
    dea_cd,
    adtl_dscrptn,
    brnd_name,
    gnrc_prc_ndctr,
    lbl_nam,
    mfr_nam,
    ndc_cnfg_ndctr,
    pkg_dscrptn,
    rte_dscrtn,
    top_200_ndctr,
    unit_dose_ndctr,
    ffp_ul_curr_effective_dt,
    ffp_ul_curr_unit_prc,
    gnrc_thrputc_class_cd,
    stndrd_thrputc_class_cd,
    drg_class_cd,
    ingr_cd_num,
    orange_bk_cd,
    curr_blu_bk_effective_dt,
    curr_blu_bk_unit_prc,
    curr_blu_bk_pkg_dt,
    curr_blu_bk_pkg_prc,
    lblr_id,
    patent_expiration_dt,
    exclsvty_expiration_dt,
    drg_strnth_num,
    shlf_pack_num,
    ptnt_pkg_insert_ndctr,
    dispense_cnt,
    -- upd_dt,
    -- crte_dt,
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
    source.ndc_num,
    source.gnrc_name,
    source.prev_ndc_num,
    source.obsolete_dt,
    source.dose_frm_dscrptn,
    source.drg_strnth_dscrptn,
    source.gnrc_ndctr,
    source.gnrc_cd_num,
    source.thrputc_class_cd,
    source.specif_thrputc_class_cd,
    source.pkg_sz,
    source.drg_frm_cd,
    source.admin_rte_cd,
    source.gnrc_cd_num_seq,
    source.repl_ndc_num,
    source.dea_cd,
    source.adtl_dscrptn,
    source.brnd_name,
    source.gnrc_prc_ndctr,
    source.lbl_nam,
    source.mfr_nam,
    source.ndc_cnfg_ndctr,
    source.pkg_dscrptn,
    source.rte_dscrtn,
    source.top_200_ndctr,
    source.unit_dose_ndctr,
    source.ffp_ul_curr_effective_dt,
    source.ffp_ul_curr_unit_prc,
    source.gnrc_thrputc_class_cd,
    source.stndrd_thrputc_class_cd,
    source.drg_class_cd,
    source.ingr_cd_num,
    source.orange_bk_cd,
    source.curr_blu_bk_effective_dt,
    source.curr_blu_bk_unit_prc,
    source.curr_blu_bk_pkg_dt,
    source.curr_blu_bk_pkg_prc,
    source.lblr_id,
    source.patent_expiration_dt,
    source.exclsvty_expiration_dt,
    source.drg_strnth_num,
    source.shlf_pack_num,
    source.ptnt_pkg_insert_ndctr,
    source.dispense_cnt,
    -- source.upd_dt,
    -- source.crte_dt,
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
--   WHEN NOT matched BY source
--   AND target.expiration_date = to_date('99991231', 'yyyyMMdd')
--   AND target.curr_flg != 'N' THEN
-- update
-- SET
--   target.expiration_date = date_sub(current_date(), 1),
--   target.curr_flg = 'N',
--   target.record_load_time = CURRENT_TIMESTAMP(),
--   target.databricks_run_id = '$$DATABRICKS_RUN_ID',
--   target.databricks_job_id = '$$DATABRICKS_JOB_ID',
--   target.update_ts = CURRENT_TIMESTAMP(),
--   target.date_part = date(CURRENT_TIMESTAMP()),
--   target.hour_part = hour(CURRENT_TIMESTAMP())