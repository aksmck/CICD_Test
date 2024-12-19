create or replace temp view copa as
select 
    sap_cust_num
    , material_id_skey
    , cust_chn_id_skey
    , sales_category_id_skey
    , product_hierarchy_1_id_skey
    , cmpny_cd
    , cust_chn_id
    , sls_ctgry_cd
    , prod_hier_1_num
    , mtrl_grp2_cd
    , mtrl_num
    , post_dt
    , sum(gross_profit) as gross_profit
    , sum(net_revenue) as net_revenue
    , sum(stndrd_cost) as stndrd_cost
    , sum(admin_expns) as admin_expns
    , sum(crdt_expns) as crdt_expns
    , sum(dlvry_expns) as dlvry_expns
    , sum(ops_payroll_expns) as ops_payroll_expns
    , sum(other_dstrbtn_expns) as other_dstrbtn_expns
    , sum(occpncy_expns) as occpncy_expns
    , sum(retail_selling_expns) as retail_selling_expns
    , sum(field_spprt) as field_spprt
    , sum(regional_office_expns) as regional_office_expns
    , sum(home_office_expns_amt) as home_office_expns_amt
from $$fdp_catalog.psas_fdp_usp_gold.vw_pharma_profitability_actuals_fpa
where cmpny_cd in ('8000','8545','8001','7503')
    and fpa_cust_seg_cd in ('C','D','W')
    and trans_type_cd <> 'P'
    and post_dt >= (case when month(current_date()) > 3 then add_months(date_trunc('year', current_date()), -21) else add_months(date_trunc('year', current_date()), -33) end)
group by 1,2,3,4,5,6,7,8,9,10,11,12
;

create or replace temp view category_ranking as
select 
  mtrl_num  
  , max(prod_hier_1_num) as prod_hier_1_num
  , max(mtrl_grp2_cd) as mtrl_grp2_cd
from (
  select
    mtrl_num
    , prod_hier_1_num
    , mtrl_grp2_cd
    , post_dt
    , rank() over (partition by mtrl_num order by post_dt desc) as rnk
  from copa
  where mtrl_num is not null 
    and trim(mtrl_num) != ''
    and post_dt < current_date()
)
where rnk = 1
group by 1
;

create or replace temp view copa_daily as
select
    copa.cmpny_cd
    , case 
        when ltrim('0', copa.cust_chn_id) in ('676') then 'ALBERTSON\'S COMPANIES'
        when ltrim('0', copa.cust_chn_id) in ('202') then 'COSTCO'
        when ltrim('0', copa.cust_chn_id) in ('78', '639', '647', '815', '816', '936', '945') then 'CVS'
        when ltrim('0', copa.cust_chn_id) in ('336') then 'DISCOUNT DRUG MART'
        when ltrim('0', copa.cust_chn_id) in ('68') then 'HEB'
        when ltrim('0', copa.cust_chn_id) in ('766') then 'KINNEY'
        when ltrim('0', copa.cust_chn_id) in ('243', '607') then 'LEWIS DRUGS, INC.'
        when ltrim('0', copa.cust_chn_id) in ('380') then 'Magellan'
        when ltrim('0', copa.cust_chn_id) in ('953') then 'MEIJER'
        when ltrim('0', copa.cust_chn_id) in ('115') then 'OPRX GEN'
        when ltrim('0', copa.cust_chn_id) in ('659') then 'OPRX HDP'
        when ltrim('0', copa.cust_chn_id) in ('317') then 'OPRX OCARE'
        when ltrim('0', copa.cust_chn_id) in ('111') then 'OPRX OSP'
        when ltrim('0', copa.cust_chn_id) in ('112') then 'OPTUM RX'
        when ltrim('0', copa.cust_chn_id) in ('869') then 'PUBLIX'
        when ltrim('0', copa.cust_chn_id) in ('26', '80', '252') then 'RITE AID'
        when ltrim('0', copa.cust_chn_id) in ('929') then 'SUPERVALU'
        when ltrim('0', copa.cust_chn_id) in ('48') then 'THRIFTY WHITE'
        when ltrim('0', copa.cust_chn_id) in ('41', '712') then 'TOPCO'
        when ltrim('0', copa.cust_chn_id) in ('227') then 'WALMART'
        when ltrim('0', copa.cust_chn_id) in ('321') then 'WEGMANS'
        else 'OTHER'
    end as cust_chn_grp
    , copa.cust_chn_id
    , t_cust_chain_id.cust_chain_name as cust_chn_name
    , case when category_ranking.prod_hier_1_num is null then ltrim('0',copa.prod_hier_1_num) else ltrim('0',category_ranking.prod_hier_1_num) end as prod_hier_1_num
    , copa.sls_ctgry_cd
    , case 
        when category_ranking.prod_hier_1_num like '8545%' then 'MPB' 
        when t_sac_product_hierarchy1_mapping.product_category is null 
            then (case 
                when t_product_hierarchy_1.type_prod = 'NA' then 'OTHER' 
                else t_product_hierarchy_1.type_prod 
            end)
        else t_sac_product_hierarchy1_mapping.product_category 
    end as prod_ctgry
    , case 
        when category_ranking.mtrl_grp2_cd in ('V1','V2','V3','V4') then 'VACCINE' 
        when category_ranking.mtrl_grp2_cd in ('W2') then 'GLP-1 Novel' 
        when category_ranking.mtrl_grp2_cd in ('W1') then 'GLP-1 Other'
        when t_sac_product_hierarchy1_mapping.product_category is null 
            then (case 
                when t_product_hierarchy_1.type_prod = 'NA' then 'OTHER' 
                else t_product_hierarchy_1.type_prod 
            end)
        else t_sac_product_hierarchy1_mapping.product_category 
    end as prod_sub_ctgry
    , t_sales_category.type_sales as prgrm_name
    , q_material.thrptc_clss_cde as thrputc_class_cd
    , t_ahfs_therapeutic_class.thrputc_class_dscrptn
    , t_national_drug_code.ndc_num
    , t_national_drug_code.brnd_name
    , ltrim('0',copa.mtrl_num) as em_item_num
    , q_material.mtrl_nme_nvgton as sell_dscrptn
    , copa.post_dt
    , sum(copa.stndrd_cost) as stndrd_cost
    , sum(copa.net_revenue) as net_revenue
    , sum(copa.gross_profit) as gross_profit
    , sum(copa.admin_expns) as admin_expns
    , sum(copa.crdt_expns) as crdt_expns
    , sum(copa.dlvry_expns) as dlvry_expns
    , sum(copa.ops_payroll_expns) as ops_payroll_expns
    , sum(copa.other_dstrbtn_expns) as other_dstrbtn_expns
    , sum(copa.occpncy_expns) as occpncy_expns
    , sum(copa.retail_selling_expns) as retail_selling_expns
    , sum(copa.field_spprt) as field_spprt
    , sum(copa.regional_office_expns) as regional_office_expns
    , sum(copa.home_office_expns_amt) as home_office_expns_amt
from copa
left join $$fdp_catalog.psas_fdp_all_gold.vw_q_material_pharma_bw q_material
    on copa.material_id_skey = q_material.material_id_skey and itm_ctvty_cde = 'A'
left join $$fdp_catalog.psas_fdp_all_gold.vw_t_cust_chain_id_pharma_bw t_cust_chain_id
    on copa.cust_chn_id_skey = t_cust_chain_id.cust_chn_id_skey
left join category_ranking
    on copa.mtrl_num = category_ranking.mtrl_num
left join $$fdp_catalog.psas_fdp_all_gold.vw_t_product_hierarchy_1_pharma_bw t_product_hierarchy_1
    on ltrim('0',t_product_hierarchy_1.prod_id) = (case when category_ranking.prod_hier_1_num is null then ltrim('0',copa.prod_hier_1_num) else ltrim('0',category_ranking.prod_hier_1_num) end) and t_product_hierarchy_1.curr_flg = 'Y'
left join $$fdp_catalog.psas_fdp_usp_gold.t_sac_product_hierarchy1_mapping
    on ltrim('0',t_sac_product_hierarchy1_mapping.product) = (case when category_ranking.prod_hier_1_num is null then ltrim('0',copa.prod_hier_1_num) else ltrim('0',category_ranking.prod_hier_1_num) end)
left join $$fdp_catalog.psas_fdp_all_gold.vw_t_sales_category_pharma_bw t_sales_category
    on copa.sales_category_id_skey = t_sales_category.sales_category_id_skey
left join $$catalog_name.edp_psas_di_usp_gold.t_national_drug_code
    on ltrim('0',q_material.ndc_nmbr) = ltrim('0',t_national_drug_code.ndc_num) and copa.post_dt between t_national_drug_code.effective_date and t_national_drug_code.expiration_date
left join $$catalog_name.edp_psas_di_usp_gold.t_ahfs_therapeutic_class
    on ltrim('0',q_material.thrptc_clss_cde) = ltrim('0',t_ahfs_therapeutic_class.thrputc_class_cd) and copa.post_dt between  t_ahfs_therapeutic_class.effective_date and t_ahfs_therapeutic_class.expiration_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;

create or replace temp view final_data as
select
    coalesce(copa_daily.cmpny_cd, 'OTHER') as chnnl
    , coalesce(copa_daily.cust_chn_id, 'OTHER') as cust_chn_id
    , coalesce(upper(copa_daily.cust_chn_name), 'OTHER') as cust_chn_name
    , coalesce(copa_daily.prod_hier_1_num, 'OTHER') as prod_hier_1_num
    , coalesce(copa_daily.sls_ctgry_cd, 'OTHER') as sls_ctgry_cd
    , coalesce(upper(case when copa_daily.prod_ctgry = 'NA' then 'OTHER' else copa_daily.prod_ctgry end), 'OTHER') as prod_ctgry
    , coalesce(copa_daily.thrputc_class_cd, 'OTHER') as thrputc_class_cd
    , coalesce(upper(copa_daily.thrputc_class_dscrptn), 'OTHER') as thrputc_class_dscrptn
    , coalesce(copa_daily.ndc_num, 'OTHER') as ndc_num
    , coalesce(upper(copa_daily.brnd_name), 'OTHER') as brnd_name
    , coalesce(copa_daily.em_item_num, 'OTHER') as em_item_num
    , time_bucket.time_bucket_id
    , coalesce(upper(copa_daily.cust_chn_grp), 'OTHER') as cust_chn_grp
    , coalesce(upper(copa_daily.prod_sub_ctgry), 'OTHER') as prod_sub_ctgry
    , coalesce(upper(copa_daily.prgrm_name), 'OTHER') as prgrm_name
    , coalesce(upper(copa_daily.sell_dscrptn), 'OTHER') as sell_dscrptn
    , time_bucket.time_bucket_start_dt
    , time_bucket.time_bucket_end_dt
    , time_bucket.prev_time_bucket_start_dt
    , time_bucket.prev_time_bucket_end_dt
    , time_bucket.sell_days_curr
    , time_bucket.sell_days_prev
    , sum(case when copa_daily.post_dt between time_bucket.time_bucket_start_dt and time_bucket.time_bucket_end_dt then copa_daily.net_revenue end) as net_revenue_curr
    , sum(case when copa_daily.post_dt between time_bucket.time_bucket_start_dt and time_bucket.time_bucket_end_dt then copa_daily.gross_profit end) as gross_profit_curr
    , sum(case when copa_daily.post_dt between time_bucket.time_bucket_start_dt and time_bucket.time_bucket_end_dt then copa_daily.stndrd_cost end) as stndrd_cost_curr
    , sum(case when copa_daily.post_dt between time_bucket.time_bucket_start_dt and time_bucket.time_bucket_end_dt then (coalesce(copa_daily.admin_expns, 0) + coalesce(copa_daily.crdt_expns, 0) + coalesce(copa_daily.dlvry_expns, 0) + coalesce(copa_daily.ops_payroll_expns, 0) + coalesce(copa_daily.other_dstrbtn_expns, 0) + coalesce(copa_daily.occpncy_expns, 0) + coalesce(copa_daily.retail_selling_expns, 0) + coalesce(copa_daily.field_spprt, 0) + coalesce(copa_daily.regional_office_expns, 0) + coalesce(copa_daily.home_office_expns_amt, 0)) end) as total_expns_curr
    , sum(case when copa_daily.post_dt between time_bucket.time_bucket_start_dt and time_bucket.time_bucket_end_dt then (coalesce(copa_daily.gross_profit, 0) - (coalesce(copa_daily.admin_expns, 0) + coalesce(copa_daily.crdt_expns, 0) + coalesce(copa_daily.dlvry_expns, 0) + coalesce(copa_daily.ops_payroll_expns, 0) + coalesce(copa_daily.other_dstrbtn_expns, 0) + coalesce(copa_daily.retail_selling_expns, 0) + coalesce(copa_daily.field_spprt, 0) + coalesce(copa_daily.regional_office_expns, 0) + coalesce(copa_daily.home_office_expns_amt, 0))) end) as annual_oper_profit_curr
    , sum(case when copa_daily.post_dt between time_bucket.prev_time_bucket_start_dt and time_bucket.prev_time_bucket_end_dt then copa_daily.net_revenue end) as net_revenue_prev
    , sum(case when copa_daily.post_dt between time_bucket.prev_time_bucket_start_dt and time_bucket.prev_time_bucket_end_dt then copa_daily.gross_profit end) as gross_profit_prev
    , sum(case when copa_daily.post_dt between time_bucket.prev_time_bucket_start_dt and time_bucket.prev_time_bucket_end_dt then copa_daily.stndrd_cost end) as stndrd_cost_prev
    , sum(case when copa_daily.post_dt between time_bucket.prev_time_bucket_start_dt and time_bucket.prev_time_bucket_end_dt then (coalesce(copa_daily.admin_expns, 0) + coalesce(copa_daily.crdt_expns, 0) + coalesce(copa_daily.dlvry_expns, 0) + coalesce(copa_daily.ops_payroll_expns, 0) + coalesce(copa_daily.other_dstrbtn_expns, 0) + coalesce(copa_daily.occpncy_expns, 0) + coalesce(copa_daily.retail_selling_expns, 0) + coalesce(copa_daily.field_spprt, 0) + coalesce(copa_daily.regional_office_expns, 0) + coalesce(copa_daily.home_office_expns_amt, 0)) end) as total_expns_prev
    , sum(case when copa_daily.post_dt between time_bucket.prev_time_bucket_start_dt and time_bucket.prev_time_bucket_end_dt then (coalesce(copa_daily.gross_profit, 0) - (coalesce(copa_daily.admin_expns, 0) + coalesce(copa_daily.crdt_expns, 0) + coalesce(copa_daily.dlvry_expns, 0) + coalesce(copa_daily.ops_payroll_expns, 0) + coalesce(copa_daily.other_dstrbtn_expns, 0) + coalesce(copa_daily.retail_selling_expns, 0) + coalesce(copa_daily.field_spprt, 0) + coalesce(copa_daily.regional_office_expns, 0) + coalesce(copa_daily.home_office_expns_amt, 0))) end) as annual_oper_profit_prev
from copa_daily
inner join (select
        t_time_bucket.time_bucket_id,
        t_time_bucket.time_bucket_start_date as time_bucket_start_dt,
        t_time_bucket.time_bucket_end_date as time_bucket_end_dt,
        add_months(t_time_bucket.time_bucket_start_date, -12) as prev_time_bucket_start_dt,
        add_months(t_time_bucket.time_bucket_end_date, -12) as prev_time_bucket_end_dt,
        t_time_bucket.sell_days_current as sell_days_curr,
        sum(t_date.sell_day_ind) as sell_days_prev
    from
        $$psas_catalog.gold_master.t_time_bucket
    left join $$psas_catalog.gold_master.t_date 
        on t_date.cal_dt between add_months(t_time_bucket.time_bucket_start_date, -12) and add_months(t_time_bucket.time_bucket_end_date, -12)
    where
        t_time_bucket.time_bucket_id regexp '^(CFYTD|PFY|CQTD|CMTD|M([1-9]|1[0-3])|Q([1-3]))$'
    group by 1,2,3,4,5,6) time_bucket
on (copa_daily.post_dt between time_bucket.time_bucket_start_dt and time_bucket.time_bucket_end_dt) or (copa_daily.post_dt between time_bucket.prev_time_bucket_start_dt and time_bucket.prev_time_bucket_end_dt)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
;

truncate table $$target_table_name
;

insert into $$target_table_name (
  chnnl
  ,	cust_chn_id
  ,	cust_chn_name
  , prod_hier_1_num
  ,	sls_ctgry_cd
  ,	prod_ctgry
  ,	prod_sub_ctgry
  ,	prgrm_name
  ,	thrputc_class_cd
  ,	thrputc_class_dscrptn
  , ndc_num
  ,	em_item_num
  ,	sell_dscrptn
  ,	time_bucket_id
  ,	cust_chn_grp
  ,	brnd_name
  ,	time_bucket_start_dt
  ,	time_bucket_end_dt
  ,	prev_time_bucket_start_dt
  ,	prev_time_bucket_end_dt
  ,	sell_days_curr
  ,	sell_days_prev
  ,	net_revenue_curr
  ,	gross_profit_curr
  ,	stndrd_cost_curr
  ,	total_expns_curr
  ,	annual_oper_profit_curr
  ,	net_revenue_prev
  ,	gross_profit_prev
  ,	stndrd_cost_prev
  ,	total_expns_prev
  ,	annual_oper_profit_prev
  ,	business_unit
  ,	segment
  ,	record_load_time
  ,	databricks_run_id
  ,	databricks_job_id
  ,	insert_ts
  ,	update_ts
)
select
  trim(chnnl) as chnnl
  , trim(cust_chn_id) as cust_chn_id
  , trim(cust_chn_name) as cust_chn_name
  , trim(prod_hier_1_num) as prod_hier_1_num
  , trim(sls_ctgry_cd) as sls_ctgry_cd
  , trim(prod_ctgry) as prod_ctgry
  , trim(prod_sub_ctgry) as prod_sub_ctgry
  , trim(prgrm_name) as prgrm_name
  , trim(thrputc_class_cd) as thrputc_class_cd
  , trim(thrputc_class_dscrptn) as thrputc_class_dscrptn
  , trim(ndc_num) as ndc_num
  , trim(em_item_num) as em_item_num
  , trim(sell_dscrptn) as sell_dscrptn
  , trim(time_bucket_id) as time_bucket_id
  , trim(cust_chn_grp) as cust_chn_grp
  , trim(brnd_name) as brnd_name
  , time_bucket_start_dt
  , time_bucket_end_dt
  , prev_time_bucket_start_dt
  , prev_time_bucket_end_dt
  , sell_days_curr
  , sell_days_prev
  , cast(net_revenue_curr as decimal(19,2)) as net_revenue_curr
  , cast(gross_profit_curr as decimal(19,2)) as gross_profit_curr
  , cast(stndrd_cost_curr as decimal(19,2)) as stndrd_cost_curr
  , cast(total_expns_curr as decimal(19,2)) as total_expns_curr
  , cast(annual_oper_profit_curr as decimal(19,2)) as annual_oper_profit_curr
  , cast(net_revenue_prev as decimal(19,2)) as net_revenue_prev
  , cast(gross_profit_prev as decimal(19,2)) as gross_profit_prev
  , cast(stndrd_cost_prev as decimal(19,2)) as stndrd_cost_prev
  , cast(total_expns_prev as decimal(19,2)) as total_expns_prev
  , cast(annual_oper_profit_prev as decimal(19,2)) as annual_oper_profit_prev
  , 'PSAS' AS business_unit
  , 'US PHARMA' AS segment
  , CURRENT_TIMESTAMP() AS record_load_time
  , '$$DATABRICKS_RUN_ID' AS databricks_run_id
  , '$$DATABRICKS_JOB_ID' AS databricks_job_id
  , CURRENT_TIMESTAMP() AS insert_ts
  , CURRENT_TIMESTAMP() AS update_ts
from final_data
;