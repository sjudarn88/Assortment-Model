#!/bin/bash

store=$1
div=$2
soar=$3
season=$4

echo Preparing Elasticity Data For store $store ... 
bq query --allow_large_results 
        --use_legacy_sql=False 
        --project_id=syw-analytics-ff 
        --replace 
        --destination_table apparel_ao_v2.elasticity_data_store_$store'_'$soar'_'$div 
        --parameter=str:INT64:$store 
     ### calculate by week log value (unit) of each class for the last year.
        "with cls_lvl_sales as ( 
                select  c.soar_no
                       , c.div_no
                       , c.ln_no
                       , c.cls_no
                       , extract(year from cast(b.TranDt as date))*100 + extract(week from cast(b.TranDt as date)) as wk_nbr 
                       , log(SUM(b.UnitQty)) as UNITS 
                   from \`syw-analytics-repo-prod.l2_enterpriseanalytics.postrandtl\` as b 
                        LEFT JOIN \`syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu\` as c 
                                ON b.ProdIrlNbr=c.prd_irl_no left join \`syw-analytics-repo-prod.lci_dw_views.sprs_product\` z 
                                on b.ProdIrlNbr = z.prd_irl_no 
                   WHERE b.TranDt >'2017-04-01' and b.TranDt <'2018-03-31' 
                                AND b.FmtSbtyp IN ('A','B','C','D','M') AND b.SrsKmtInd='S' 
                                and b.trantypeind = 'S' AND b.SrsDvsnNbr NOT IN (0,79) 
                                and c.soar_no in (101,102,103,104,105) 
                                and b.locnnbr in (select storeB from \`apparel_jaccard_pp.sim_stores_10_fw\` 
                                                        where storeA = @str) 
                                and b.UnitQty > 0 
                   group by 1,2,3,4,5) 
                   
      ###calculate by week historical price for each item for the last year.
        , historic_price as ( 
                select  cast(a.location_nbr as int64) as locn_nbr
                       , b.soar_no
                       , cast(a.sears_division_nbr as int64) as div_no
                       , b.ln_no, b.cls_no
                       , cast(a.sears_item_nbr as int64) as sears_item_nbr
                       , cast(a.sears_sku_nbr as int64) as sears_sku_nbr
                       , cast(a.product_internal_nbr as int64) as prodirlnbr
                       , extract(year from cast(a.price_start_dt as date))*100 + extract(week from cast(a.price_start_dt as date)) as price_start_wk
                       , min(cast(a.price_amt as float64)) as price_amt 
                   from \`shc-pricing-prod.bq_pricing_it.smith__mrkdn_sears_prcm_current\` as a 
                        left join \`syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu\` as b 
                        on cast(a.product_internal_nbr as int64) = b.PRD_IRL_NO 
                   where b.soar_no in (101,102,103,104,105) and price_start_dt >= '2017-04-01' 
                         and price_start_dt <= '2018-03-31' 
                   group by 1,2,3,4,5,6,7,8,9 ) 
                   
       ###calculate by week log value(average price) for each class for selected stores.
        , cls_lvl_price as ( 
                select  soar_no
                        , div_no
                        , ln_no
                        , cls_no
                        , price_start_wk
                        , log(avg(price_amt)) as avg_prc_amt 
                    from historic_price 
                    where locn_nbr in (select storeB from \`apparel_jaccard_pp.sim_stores_10_fw\` where storeA = @str) 
                          and price_amt > 0 
                    group by 1,2,3,4,5 ) 
         , cls_lvl_price2 as ( 
               select *
                        , lead(price_start_wk, 1, 999999) over(partition by soar_no, div_no, ln_no, cls_no order by price_start_wk) as next_price_start_wk 
                   from cls_lvl_price ) 
                   
                   
         select   b.soar_no
                 , b.div_no
                 , b.ln_no
                 , b.cls_no
                 , concat(cast(b.soar_no as string), '_', cast(b.div_no as string), '_', cast(b.ln_no as string), '_', cast(b.cls_no as string)) as div_ln_cls 
                 , wk_nbr
                 , b.UNITS as log_UNITS
                 , a.avg_prc_amt as log_price 
              from cls_lvl_sales as b 
                   left join cls_lvl_price2 as a 
                        on b.soar_no = a.soar_no and b.div_no = a.div_no and b.ln_no = a.ln_no and b.cls_no = a.cls_no 
                        and (b.wk_nbr >= a.price_start_wk and b.wk_nbr < a.next_price_start_wk)"

echo Writing to Google Storage store $store ... 
bq extract apparel_ao_v2.elasticity_data_store_$store'_'$soar'_'$div gs://ao-v2-bucket/elasticity-files/elasticity_data_store_$store'_'$soar'_'$div'_'$season.csv

echo Moving to Instance--
gsutil cp gs://ao-v2-bucket/elasticity-files/elasticity_data_store_$store'_'$soar'_'$div'_'$season.csv ./$store'_'$soar'_'$div'_'$season/

exit 0
