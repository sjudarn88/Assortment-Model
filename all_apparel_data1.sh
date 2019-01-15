with sears_div as (
select SOAR_NO, SOAR_NM, DIV_NO, DIV_NM from `syw-analytics-repo-prod.crm_perm_tbls.sywr_srs_soar_bu`
where SOAR_NM like '%APPAREL%' or SOAR_NM like '%TOOL%' or SOAR_NM like '%HOME%'
group by 1,2,3,4
)
------------------------
, sears_div2 as (
select  srsgrp_kmtbsnss_desc, srsgrp_kmtbsnss_nbr ,srsctgry_kmtunit_nbr, srsctgry_kmtunit_desc, srsvrtclbsnss_kmtdvsn_desc, srsvrtclbsnss_kmtdvsn_nbr, srsdvsn_kmtdept_desc, srsdvsn_kmtdept_nbr  
from `syw-analytics-repo-prod.l2_enterpriseanalytics.merchlvl90skuandksn`
where srsdvsn_kmtdept_nbr in (select DIV_NO from sears_div)
and srsgrp_kmtbsnss_nbr <> 1 -- exclude KMART (Miscellaneous
and srsgrp_kmtbsnss_nbr = 803 -- sears apparel
group by 1,2,3,4,5,6,7,8
order by 1,2,3,4,5,6,7,8
)
------------------------
, receipts as (
select hier.gro_nbr, hier.vbs_nbr, hier.div_nbr, hier.ln_nbr, hier.sbl_nbr, hier.cls_nbr, rcp.sku_id, rcp.locn_nbr,
	 (cal.acctg_yr*100 + cal.acctg_wk) as wk_nbr, -- rcp.wk_end_dt,
	 sum(rcp.reciept_units) as ttl_reciept_units,
	 sum(rcp.reciept_cost_dlr) as ttl_reciept_cost
from
(
select distinct SKU_ID as sku_id, LOCN_NBR as locn_nbr, DAY_NBR as day_nbr, RCP_UN_QT as reciept_units, RCP_CST_DLR as reciept_cost_dlr --, WK_END_DT as wk_end_dt
from `syw-analytics-repo-prod.alex_arp_views_prd.fact_srs_dly_opr_rcp`
where DAY_NBR BETWEEN "2017-01-29" and "2018-01-27" --2017
--where DAY_NBR between "2015-02-01" and "2016-01-30" --2015
--where DAY_NBR between "2016-01-31" and "2017-01-28" --2016
--where DAY_NBR between "2016-01-31" and "2018-01-27" --2016-2017
) rcp
inner join `syw-analytics-repo-prod.lci_public_tbls.lcixt44_445_calendar` cal
on rcp.day_nbr = cal.acctg_dt
inner join
(
select
srsgrp_kmtbsnss_nbr as gro_nbr, srsvrtclbsnss_kmtdvsn_nbr as vbs_nbr,
srsdvsn_kmtdept_nbr as div_nbr, srsline_kmtctgryclstr_nbr as ln_nbr,
srssbline_kmtctgry_id as sbl_nbr, srsclss_kmtsbctgry_nbr as cls_nbr, srssku_kmtksn_id as sku_id
from
`syw-analytics-repo-prod.l2_enterpriseanalytics.merchlvl90skuandksn`
where srsgrp_kmtbsnss_nbr in (select  srsgrp_kmtbsnss_nbr from sears_div2) 
and srskmtind = "S"
group by 1,2,3,4,5,6,7
) hier
on hier.sku_id = rcp.sku_id
group by 1,2,3,4,5,6,7,8,9
)
------------------------
, inventory as (
SELECT hier.gro_nbr, hier.vbs_nbr, hier.div_nbr, hier.ln_nbr, hier.sbl_nbr, hier.cls_nbr, inv.sku_id, inv.locn_nbr,
	  inv.wk_nbr, inv.wk_end_dt,
	  SUM(CASE WHEN inv.INS_TYP_CD IN ('H') AND inv.INS_SUB_TYP_CD NOT IN ('R','D','N') THEN inv.TTL_UN_QT ELSE 0 END)  on_hand_inv_units,
	  SUM(CASE WHEN inv.INS_TYP_CD IN ('H') AND inv.INS_SUB_TYP_CD IN ('D','N') THEN inv.TTL_UN_QT ELSE 0 END)  damaged_or_unsellable_inv_units,
	  SUM(CASE WHEN inv.INS_TYP_CD IN ('I') THEN inv.TTL_UN_QT ELSE 0 END)  in_transit_inv_units,
	  SUM(CASE WHEN inv.INS_TYP_CD IN ('O') THEN inv.TTL_UN_QT ELSE 0 END) on_order_inv_units
FROM (
	select SKU_ID as sku_id, LOCN_NBR as locn_nbr, WK_NBR as wk_nbr, WK_END_DT as wk_end_dt, INS_TYP_CD, INS_SUB_TYP_CD, TTL_UN_QT
	from `syw-analytics-repo-prod.alex_arp_views_prd.fact_srs_wkly_opr_ins`
	where WK_END_DT between date("2017-02-05") and date("2018-02-03")
	--where WK_END_DT between "2016-02-07" and "2017-02-04" --2016
	--where WK_END_DT between "2015-02-08" and "2016-02-06" --2015
  --where WK_END_DT between date("2016-01-31") and date("2018-01-27") --2016-2017
) inv
inner join
(
select
	srsgrp_kmtbsnss_nbr as gro_nbr, srsvrtclbsnss_kmtdvsn_nbr as vbs_nbr,
	srsdvsn_kmtdept_nbr as div_nbr, srsline_kmtctgryclstr_nbr as ln_nbr,
	srssbline_kmtctgry_id as sbl_nbr, srsclss_kmtsbctgry_nbr as cls_nbr, srssku_kmtksn_id as sku_id
from
	`syw-analytics-repo-prod.l2_enterpriseanalytics.merchlvl90skuandksn`
	where srsgrp_kmtbsnss_nbr in (select  srsgrp_kmtbsnss_nbr from sears_div2) 
   and srskmtind = "S"
group by 1,2,3,4,5,6,7
) hier
on hier.sku_id = inv.sku_id
GROUP BY 1,2,3,4,5,6,7,8,9,10
)
------------------------

, inventory_wk_prod_data as (
Select
a.*,
case when ttl_reciept_units is null then 0 else ttl_reciept_units end as ttl_reciept_units
,case when ttl_reciept_cost is null then 0 else ttl_reciept_cost end as ttl_reciept_cost
,CASE WHEN d.SoldQty is null THEN 0 ELSE d.SoldQty END as SoldQty,
b.ITM_NO,
b.NAT_SLL_PRC,
c.Product_Type,
c.IMA_Product_Type,
CONCAT(CAST(a.div_nbr AS STRING), "_", CAST(a.ln_nbr AS STRING), "_", CAST(a.cls_nbr AS STRING)) as Class_Product_Type,
CONCAT(CAST(a.div_nbr AS STRING), "_", CAST(a.ln_nbr AS STRING)) as Line_Product_Type from
(
   select div_nbr, ln_nbr, sbl_nbr, cls_nbr, sku_id,
          SUBSTR( CAST(SKU_ID as STRING),  1, LENGTH(CAST(SKU_ID as STRING)) - 3) as PRD_IRL_NO,
          locn_nbr,	wk_nbr,	wk_end_dt,	on_hand_inv_units,	damaged_or_unsellable_inv_units,
          in_transit_inv_units,	on_order_inv_units
   from `apparel_als_all_app.inventory`
   --where div_nbr in (7, 41, 43)  -- (7, 31, 2, 16, 41, 43) -- and --locn_nbr = 1125 and div_nbr = 7 -- and sku_id in (8214498010) -- and div_nbr = 7
   ) a
LEFT JOIN
(select
    DIV_NO,
    PRD_IRL_NO,
    ITM_NO,
    NAT_SLL_PRC
 from `syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu`
 where ITM_PURG_DT is null) b
ON (a.div_nbr = b.DIV_NO AND a.PRD_IRL_NO = CAST(b.PRD_IRL_NO AS STRING))
LEFT JOIN
(select DIV_NO,PRD_IRL_NO, Product_Type, IMA_Product_Type  from `syw-analytics-ff.apparel_als_all_app.basic_spin_attributes_apparel` ) c
ON (cast(b.PRD_IRL_NO as int64) = cast(c.PRD_IRL_NO as int64))
LEFT JOIN (
  SELECT
      wknbr
      ,locnnbr as store
      --,ringinglocnnbr
      --,OrignlFcltyNbr
      ,trantypeind
      ,b.cls_ds
      ,a.skuid
      ,a.ProdIrlNbr
      ,a.SrsItmNbr AS Item
      ,b.prd_ds AS item_dsc
      ,z.ssn_cd
      --,NetSellAmt
      --,lineitmgrosssoldamt
      --,SlsMrgnAmt
      --,sywearnamt
      --,sywburnamt
      ,SUM(CASE WHEN UnitQty = 0 THEN 1 ELSE UnitQty END) as SoldQty
      --,SUM(NetSellAmt) AS sales
      --,SUM(lineitmgrosssoldamt) as grssoldamt
      --,SUM(SlsMrgnAmt) AS margin
      --,SUM(sywearnamt) as sywearnamt
      --,SUM(sywburnamt) as sywburnamt
      --,COUNT(DISTINCT CONCAT(cast(LyltyCardNbr as string), cast(TranDt as string))) AS trips
      --,SUM(UnitQty) UNITS
    FROM `syw-analytics-repo-prod.l2_enterpriseanalytics.postrandtl` a
    LEFT JOIN `syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu` B ON a.ProdIrlNbr=b.prd_irl_no
    LEFT JOIN `syw-analytics-repo-prod.crm_mart1_tbls.crm_sears_retail_fty` C ON a.OrignlFcltyNbr=C.fty_id_no
    left join `syw-analytics-repo-prod.lci_dw_views.sprs_product` z
     on a.ProdIrlNbr = z.prd_irl_no
    WHERE TranDt >='2017-01-01' -- date to be changed according to the reqiurement
    AND a.FmtSbtyp IN ('A','B','C','D','M')
    --AND lyltycardnbr IS NOT NULL  -- force member only sales
    AND SrsKmtInd='S'
    and trantypeind = 'S'
    --and b.ln_ds = 'SIMPLY STYLED KNITS'
    --and b.cls_ds = 'SHORT SLEEVE'
    --and ProdIrlNbr = 17252887
    AND SrsDvsnNbr NOT IN (0,79)
    --and LyltyCardNbr is not null
    and wknbr > 201700
    --and (z.ssn_cd like '%H%' or z.ssn_cd like '%F%')
    --and locnnbr = 1125 --and skuid in (10438687053, 8217238039, 9195438999)
    GROUP BY 1,2,3,4,5,6,7,8,9
    ---order by store,skuid,wknbr asc
) d
ON (a.locn_nbr = d.store and a.wk_nbr = d.wknbr and a.sku_id = d.skuid)
LEFT JOIN (
  select
    locn_nbr
    ,wk_nbr
    ,sku_id
    ,sum(ttl_reciept_units) as ttl_reciept_units
    ,sum(ttl_reciept_cost) as ttl_reciept_cost
    from `apparel_als_all_app.receipts`
    group by 1,2,3
) e
ON (a.locn_nbr = e.locn_nbr and a.wk_nbr = e.wk_nbr and a.sku_id = e.sku_id)
--where a.locn_nbr = 1125 and CAST(a.sku_id as STRING) like '18253933%'
--order by locn_nbr, a.sku_id, wk_nbr asc
)
------------------------
--Product Level assortment offered weekly
, stores_weekly_assortment_proc as (
select
                      div_nbr, Product_Type, Season, Season_Correspondence,
                      wk_nbr, locn_nbr,
                      STRING_AGG(DISTINCT prod_id, "," ORDER BY prod_id) as assortment_id
                    from (
                        select
                          a.Product_Type,
                          b.SSN_CD,
                          b.Season_Year,
                          b.Season,
                          CASE
                            WHEN Season != 'special' and year <= Season_Year THEN 'current'
                            WHEN Season != 'special' and year > Season_Year THEN 'aged'
                            ELSE 'special' END as Season_Correspondence,
                          a.year,
                          a.wk_nbr,
                          a.locn_nbr,
                          a.div_nbr,
                          a.itm_no,
                          a.prd_irl_no,
                          CONCAT(CAST(a.div_nbr AS STRING), "-", a.prd_irl_no, '-', SSN_CD) as prod_id
                        from (
                            select wk_nbr,
                              CAST(SUBSTR(CAST(wk_nbr AS STRING), 0, 4) AS INT64) as year,
                              --Product_Type,
                              CAST(locn_nbr as STRING) as locn_nbr,
                              div_nbr, ITM_NO,
                              PRD_IRL_NO,
                              Product_Type,
                              IMA_Product_Type,
                              Class_Product_Type
                            from `apparel_als_all_app.inventory_wk_prod_data` 
                            where
                              PRD_IRL_NO is not null and
                              wk_nbr > 201700 and wk_nbr < 201800 --probbly we need to do it by the season of the item, but this should work as well, need to check
                              and on_hand_inv_units > 0 or SoldQty > 0 or ttl_reciept_units > 1
                        ) a
                        LEFT JOIN  (
                            select
                              a.*,
                              b.ITM_PURG_DT
                            from (
                                select DIV_NO
                                  ,ITM_NO
                                  ,PRD_IRL_NO
                                  ,SSN_CD
                                  ,CASE
                                    WHEN SSN_CD in ('C3','F2','H2','U2','S2') THEN 2012
                                    WHEN SSN_CD in ('C4','F3','H3','U3','S3') THEN 2013
                                    WHEN SSN_CD in ('C5','F4','H4','U4','S4') THEN 2014
                                    WHEN SSN_CD in ('C6','F5','H5','U5','S5') THEN 2015
                                    WHEN SSN_CD in ('C7','F6','H6','U6','S6') THEN 2016
                                    WHEN SSN_CD in ('C8','F7','H7','U7','S7') THEN 2017
                                    WHEN SSN_CD in ('C9','F8','H8','U8','S8') THEN 2018
                                    WHEN SSN_CD in ('C10','F9','H9','U9','S9') THEN 2019
                                    ELSE 0 END AS Season_Year
                                  ,CASE
                                    WHEN SSN_CD like 'C%' THEN 'basic'
                                    WHEN SSN_CD like 'F%' THEN 'fall'
                                    WHEN SSN_CD like 'H%' THEN 'winter'
                                    WHEN SSN_CD like 'U%' THEN 'summer'
                                    WHEN SSN_CD like 'S%' THEN 'spring'
                                    WHEN SSN_CD = '' THEN 'NA'
                                    ELSE 'special' END as Season
                                from `syw-analytics-repo-prod.lci_dw_views.sprs_product`
                            ) a
                            LEFT JOIN (
                              select
                                DIV_NO,
                                ITM_NO,
                                PRD_IRL_NO,
                                ITM_PURG_DT
                              from `syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu`
                            ) b
                            ON (a.PRD_IRL_NO = b.PRD_IRL_NO)
                        ) b
                        ON (a.PRD_IRL_NO = CAST(b.PRD_IRL_NO AS STRING))
                    )
                    --where Product_Type is not null
                    group by 1,2,3,4,5, 6
--       ) where locn_nbr = '1079' and assortment_id like '%17976630%'
 --      order by wk_nbr
)
------------------------
, stores_weekly_assortment_proc_uniq_ids as (
SELECT
  *
FROM (
  SELECT
    ROW_NUMBER() OVER(PARTITION BY div_nbr, Product_Type, Season, Season_Correspondence ) unique_id,
    CONCAT("prd_", CAST(div_nbr AS STRING), "_", Product_Type, "_", Season, "_", Season_Correspondence) AS prefix,
    div_nbr,
    Product_Type,
    Season,
    Season_Correspondence,
    assortment_id
  FROM (
    SELECT
      DISTINCT div_nbr,
      Product_Type,
      Season,
      Season_Correspondence,
      assortment_id
    FROM
      `apparel_als_all_app.stores_weekly_assortment_proc` 
    WHERE
      assortment_id IS NOT NULL  and Product_Type is not null) )
WHERE
  prefix IS NOT NULL
)
------------------------
--Class Level assortment offered weekly
, stores_weekly_assortment_proc_cls as (
select div_nbr, 
Product_Type, 
Class_Product_Type, 
Season, Season_Correspondence,
wk_nbr, locn_nbr,
STRING_AGG(DISTINCT prod_id, "," ORDER BY prod_id) as assortment_id
from (
       select
a.Product_Type,
a.Class_Product_Type,
b.SSN_CD,
b.Season_Year,
b.Season,
CASE
WHEN Season != 'special' and year <= Season_Year THEN 'current'
WHEN Season != 'special' and year > Season_Year THEN 'aged'
ELSE 'special' END as Season_Correspondence,
a.year,
a.wk_nbr,
a.locn_nbr,
a.div_nbr,
a.itm_no,
a.prd_irl_no,
CONCAT(CAST(a.div_nbr AS STRING), "-", a.prd_irl_no, '-', SSN_CD
) as prod_id
from (
select wk_nbr,
CAST(SUBSTR(CAST(wk_nbr AS STRING), 0, 4) AS INT64) as year,
--Product_Type,
CAST(locn_nbr as STRING) as locn_nbr,
div_nbr, ITM_NO,
PRD_IRL_NO,
Product_Type,
IMA_Product_Type,
Class_Product_Type
from `apparel_als_all_app.inventory_wk_prod_data`  
where
PRD_IRL_NO is not null and
wk_nbr > 201700 and wk_nbr < 201800 --probbly we need to do it by the season of the item, but this should work as well, need to check
and on_hand_inv_units > 0 or SoldQty > 0 or ttl_reciept_units > 1
) a
LEFT JOIN  (
select
a.*,
b.ITM_PURG_DT
from (
select DIV_NO
,ITM_NO
,PRD_IRL_NO
,SSN_CD
,CASE
WHEN SSN_CD in ('C3','F2','H2','U2','S2') THEN 2012
WHEN SSN_CD in ('C4','F3','H3','U3','S3') THEN 2013
WHEN SSN_CD in ('C5','F4','H4','U4','S4') THEN 2014
WHEN SSN_CD in ('C6','F5','H5','U5','S5') THEN 2015
WHEN SSN_CD in ('C7','F6','H6','U6','S6') THEN 2016
WHEN SSN_CD in ('C8','F7','H7','U7','S7') THEN 2017
WHEN SSN_CD in ('C9','F8','H8','U8','S8') THEN 2018
WHEN SSN_CD in ('C10','F9','H9','U9','S9') THEN 2019
ELSE 0 END AS Season_Year
                                  ,CASE
                                    WHEN SSN_CD like 'C%' THEN 'basic'
                                    WHEN SSN_CD like 'F%' THEN 'fall'
                                    WHEN SSN_CD like 'H%' THEN 'winter'
                                    WHEN SSN_CD like 'U%' THEN 'summer'
                                    WHEN SSN_CD like 'S%' THEN 'spring'
                                    WHEN SSN_CD = '' THEN 'NA'
                                    ELSE 'special' END as Season
                                from `syw-analytics-repo-prod.lci_dw_views.sprs_product`
                            ) a
LEFT JOIN (
select
DIV_NO,
ITM_NO,
PRD_IRL_NO,
ITM_PURG_DT
from `syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu`
) b
  ON (a.PRD_IRL_NO = b.PRD_IRL_NO)
  ) b
ON (a.PRD_IRL_NO = CAST(b.PRD_IRL_NO AS STRING))
)
where Product_Type is not null
group by 1,2,3,4,5, 6,7
)
----------------------------------
, stores_weekly_assortment_proc_cls_uniq_ids as (
/*SELECT
  *
FROM (
  SELECT
    RANK() OVER(PARTITION BY div_nbr, Product_Type, Season, Season_Correspondence ,Class_Product_Type, 
    Season, Season_Correspondence ORDER BY assortment_id) unique_id,
    CONCAT("cls_", Product_Type, "_", Season, "_", Season_Correspondence) AS prefix,
    div_nbr,
    Product_Type,
    Season,
    Season_Correspondence,
    assortment_id
  FROM (
    SELECT
      DISTINCT div_nbr,
      Class_Product_Type,
      Product_Type,
      Season,
      Season_Correspondence,
      assortment_id
    FROM
      `apparel_als_all_app.stores_weekly_assortment_proc_cls`
    WHERE
      assortment_id IS NOT NULL ) )
WHERE
  prefix IS NOT NULL
 
 */
 SELECT
  *
FROM (
  SELECT
    RANK() OVER(PARTITION BY div_nbr, Product_Type, Season, Season_Correspondence ,Class_Product_Type, 
    Season, Season_Correspondence ORDER BY assortment_id) unique_id,
    CONCAT("cls_", Class_Product_Type, "_", Product_Type, "_", Season, "_", Season_Correspondence) AS prefix,
    div_nbr,
    Product_Type,
    Class_Product_Type,
    Season,
    Season_Correspondence,
    assortment_id
  FROM (
    SELECT
      DISTINCT div_nbr,
      Class_Product_Type,
      Product_Type,
      Season,
      Season_Correspondence,
      assortment_id
    FROM
      `apparel_als_all_app.stores_weekly_assortment_proc_cls`
    WHERE
      assortment_id IS NOT NULL ) )
WHERE
  prefix IS NOT NULL)
--------------------------------------------
--Line Level assortment offered weekly
, stores_weekly_assortment_proc_ln as (
select div_nbr, 
Product_Type, 
Line_Product_Type, 
Season, Season_Correspondence,
wk_nbr, locn_nbr,
STRING_AGG(DISTINCT prod_id, "," ORDER BY prod_id) as assortment_id
from (
       select
a.Product_Type,
a.Line_Product_Type,
b.SSN_CD,
b.Season_Year,
b.Season,
CASE
WHEN Season != 'special' and year <= Season_Year THEN 'current'
WHEN Season != 'special' and year > Season_Year THEN 'aged'
ELSE 'special' END as Season_Correspondence,
a.year,
a.wk_nbr,
a.locn_nbr,
a.div_nbr,
a.itm_no,
a.prd_irl_no,
CONCAT(CAST(a.div_nbr AS STRING), "-", a.prd_irl_no, '-', SSN_CD) as prod_id
from (
select wk_nbr,
CAST(SUBSTR(CAST(wk_nbr AS STRING), 0, 4) AS INT64) as year,
--Product_Type,
CAST(locn_nbr as STRING) as locn_nbr,
div_nbr, ITM_NO,
PRD_IRL_NO,
Product_Type,
IMA_Product_Type,
Line_Product_Type
from `apparel_als_all_app.inventory_wk_prod_data`  
where
PRD_IRL_NO is not null and
wk_nbr > 201700 and wk_nbr < 201800 --probbly we need to do it by the season of the item, but this should work as well, need to check
and on_hand_inv_units > 0 or SoldQty > 0 or ttl_reciept_units > 1
) a
LEFT JOIN  (
select
a.*,
b.ITM_PURG_DT
from (
select DIV_NO
,ITM_NO
,PRD_IRL_NO
,SSN_CD
,CASE
WHEN SSN_CD in ('C3','F2','H2','U2','S2') THEN 2012
WHEN SSN_CD in ('C4','F3','H3','U3','S3') THEN 2013
WHEN SSN_CD in ('C5','F4','H4','U4','S4') THEN 2014
WHEN SSN_CD in ('C6','F5','H5','U5','S5') THEN 2015
WHEN SSN_CD in ('C7','F6','H6','U6','S6') THEN 2016
WHEN SSN_CD in ('C8','F7','H7','U7','S7') THEN 2017
WHEN SSN_CD in ('C9','F8','H8','U8','S8') THEN 2018
WHEN SSN_CD in ('C10','F9','H9','U9','S9') THEN 2019
ELSE 0 END AS Season_Year
                                  ,CASE
                                    WHEN SSN_CD like 'C%' THEN 'basic'
                                    WHEN SSN_CD like 'F%' THEN 'fall'
                                    WHEN SSN_CD like 'H%' THEN 'winter'
                                    WHEN SSN_CD like 'U%' THEN 'summer'
                                    WHEN SSN_CD like 'S%' THEN 'spring'
                                    WHEN SSN_CD = '' THEN 'NA'
                                    ELSE 'special' END as Season
                                from `syw-analytics-repo-prod.lci_dw_views.sprs_product`
                            ) a
LEFT JOIN (
select
DIV_NO,
ITM_NO,
PRD_IRL_NO,
ITM_PURG_DT
from `syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu`
) b
  ON (a.PRD_IRL_NO = b.PRD_IRL_NO)
  ) b
ON (a.PRD_IRL_NO = CAST(b.PRD_IRL_NO AS STRING))
)
where Product_Type is not null
group by 1,2,3,4,5, 6,7
)
--------------------------------------------
--Line Level assortment offered weekly
, stores_weekly_assortment_proc_ima as (
select
                       div_nbr, Product_Type, IMA_Product_Type, Season, Season_Correspondence,
                       wk_nbr, locn_nbr,
                       STRING_AGG(DISTINCT prod_id, "," ORDER BY prod_id) as assortment_id
                     from (
                         select
                           a.Product_Type,
                           a.IMA_Product_Type,
                           b.SSN_CD,
                           b.Season_Year,
                           b.Season,
                           CASE
                             WHEN Season != 'special' and year <= Season_Year THEN 'current'
                             WHEN Season != 'special' and year > Season_Year THEN 'aged'
                             ELSE 'special' END as Season_Correspondence,
                           a.year,
                           a.wk_nbr,
                           a.locn_nbr,
                           a.div_nbr,
                           a.itm_no,
                           a.prd_irl_no,
                           CONCAT(CAST(a.div_nbr AS STRING), "-", a.prd_irl_no, '-', SSN_CD) as prod_id
                         from (
                             select wk_nbr,
                               CAST(SUBSTR(CAST(wk_nbr AS STRING), 0, 4) AS INT64) as year,
                               --Product_Type,
                               CAST(locn_nbr as STRING) as locn_nbr,
                               div_nbr, ITM_NO,
                               PRD_IRL_NO,
                               Product_Type,
                               IMA_Product_Type,
                               Class_Product_Type
                             from `apparel_als_all_app.inventory_wk_prod_data` 
                             where
                               PRD_IRL_NO is not null and
                               wk_nbr > 201700 and wk_nbr < 201800 --probbly we need to do it by the season of the item, but this should work as well, need to check
                               and on_hand_inv_units > 0 or SoldQty > 0 or ttl_reciept_units > 1
                         ) a
                         LEFT JOIN  (
                             select
                               a.*,
                               b.ITM_PURG_DT
                             from (
                                 select DIV_NO
                                   ,ITM_NO
                                   ,PRD_IRL_NO
                                   ,SSN_CD
                                   ,CASE
                                     WHEN SSN_CD in ('C3','F2','H2','U2','S2') THEN 2012
                                     WHEN SSN_CD in ('C4','F3','H3','U3','S3') THEN 2013
                                     WHEN SSN_CD in ('C5','F4','H4','U4','S4') THEN 2014
                                     WHEN SSN_CD in ('C6','F5','H5','U5','S5') THEN 2015
                                     WHEN SSN_CD in ('C7','F6','H6','U6','S6') THEN 2016
                                     WHEN SSN_CD in ('C8','F7','H7','U7','S7') THEN 2017
                                     WHEN SSN_CD in ('C9','F8','H8','U8','S8') THEN 2018
                                     WHEN SSN_CD in ('C10','F9','H9','U9','S9') THEN 2019
                                     ELSE 0 END AS Season_Year
                                   ,CASE
                                     WHEN SSN_CD like 'C%' THEN 'basic'
                                     WHEN SSN_CD like 'F%' THEN 'fall'
                                     WHEN SSN_CD like 'H%' THEN 'winter'
                                     WHEN SSN_CD like 'U%' THEN 'summer'
                                     WHEN SSN_CD like 'S%' THEN 'spring'
                                     WHEN SSN_CD = '' THEN 'NA'
                                     ELSE 'special' END as Season
                                 from `syw-analytics-repo-prod.lci_dw_views.sprs_product`
                             ) a
                             LEFT JOIN (
                               select
                                 DIV_NO,
                                 ITM_NO,
                                 PRD_IRL_NO,
                                 ITM_PURG_DT
                               from `syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu`
                             ) b
                             ON (a.PRD_IRL_NO = b.PRD_IRL_NO)
                         ) b
                         ON (a.PRD_IRL_NO = CAST(b.PRD_IRL_NO AS STRING))
                     )
                     --where Product_Type is not null
                     group by 1,2,3,4,5, 6, 7
 --       ) where locn_nbr = '1079' and assortment_id like '%17976630%'
  --      order by wk_nbr
)
-----------------------------------
, stores_weekly_assortment_proc_ln_uniq_ids as (
/*SELECT
  *
FROM (
  SELECT
    RANK() OVER(PARTITION BY div_nbr, Product_Type, Line_Product_Type, Season, Season_Correspondence ORDER BY assortment_id) unique_id,
    CONCAT("ln_", Product_Type, "_", Season, "_", Season_Correspondence) AS prefix,
    div_nbr,
    Product_Type,
    Season,
    Season_Correspondence,
    assortment_id
  FROM (
    SELECT
      DISTINCT div_nbr,
      Line_Product_Type,
      Product_Type,
      Season,
      Season_Correspondence,
      assortment_id
    FROM
      `apparel_als_all_app.stores_weekly_assortment_proc_ln`
    WHERE
      assortment_id IS NOT NULL ) )
WHERE
  prefix IS NOT NULL
  */
  SELECT
  *
FROM (
  SELECT
    RANK() OVER(PARTITION BY div_nbr, Product_Type, Line_Product_Type, Season, Season_Correspondence ORDER BY assortment_id) unique_id,
    CONCAT("ln_", Line_Product_Type, "_", Product_Type, "_", Season, "_", Season_Correspondence) AS prefix,
    div_nbr,
    Product_Type,
    Line_Product_Type,
    Season,
    Season_Correspondence,
    assortment_id
  FROM (
    SELECT
      DISTINCT div_nbr,
      Line_Product_Type,
      Product_Type,
      Season,
      Season_Correspondence,
      assortment_id
    FROM
      `apparel_als_all_app.stores_weekly_assortment_proc_ln`
    WHERE
      assortment_id IS NOT NULL ) )
WHERE
  prefix IS NOT NULL
 )
 ----------------------------------
 -----------------------------------
, stores_weekly_assortment_proc_ima_uniq_ids as (
/*
SELECT
  *
FROM (
  SELECT
    ROW_NUMBER() OVER(PARTITION BY div_nbr, Product_Type, IMA_Product_Type, Season, Season_Correspondence) as unique_id,
    CONCAT("ima_", Product_Type, "_", Season, "_", Season_Correspondence) AS prefix,
    div_nbr,
    Product_Type,
    IMA_Product_Type,
    Season,
    Season_Correspondence,
    assortment_id
  FROM (
    SELECT
      DISTINCT div_nbr,
      Product_Type,
      IMA_Product_Type,
      Season,
      Season_Correspondence,
      assortment_id
    FROM
      `syw-analytics-ff.apparel_als_all_app.stores_weekly_assortment_proc_ima`
    WHERE
      assortment_id IS NOT NULL and Product_Type is not null and IMA_Product_Type is not null ) )
WHERE
  prefix IS NOT NULL
*/
SELECT
  *
FROM (
  SELECT
    ROW_NUMBER() OVER(PARTITION BY div_nbr, Product_Type, IMA_Product_Type, Season, Season_Correspondence) as unique_id,
    CONCAT("ima_", IMA_Product_Type, "_", Product_Type, "_", Season, "_", Season_Correspondence) AS prefix,
    div_nbr,
    Product_Type,
    IMA_Product_Type,
    Season,
    Season_Correspondence,
    assortment_id
  FROM (
    SELECT
      DISTINCT div_nbr,
      Product_Type,
      IMA_Product_Type,
      Season,
      Season_Correspondence,
      assortment_id
    FROM
      `syw-analytics-ff.apparel_als_all_app.stores_weekly_assortment_proc_ima`
    WHERE
      assortment_id IS NOT NULL and Product_Type is not null and IMA_Product_Type is not null ) )
WHERE
  prefix IS NOT NULL
 )
 ----------------------------------
 /*
SELECT
 member_id,
 wknbr,
 store,
 ringinglocnnbr,
 trantypeind,
 mrchndssoldstscd,
 div_no,
 ln_no,
 cls_no,
 skuid,
 ProdIrlNbr,
 ssn_cd,
 prod_id,
 a.Season_Correspondence,
 a.Season,
 b.Product_Type,
 NetSellAmt,
 lineItmGrossSoldAmt,
 slsMrgnAmt,
 sywEarnAmt,
 sywBurnAmt,
 UnitQty,
 assortment_id_prod,
 assortment_id_line,
 assortment_id_cls,
 assortment_id_imaprod,
 '_ALL_' AS assortment_id_na
FROM (
 SELECT
   *,
   CASE
     WHEN Season != 'special' AND year <= Season_Year THEN 'current'
     WHEN Season != 'special'
   AND year > Season_Year THEN 'aged'
     ELSE 'special'
   END AS Season_Correspondence
 FROM (
   SELECT
     CAST(LyltyCardNbr AS string) AS member_id,
     wknbr,
     CAST(SUBSTR(CAST(wknbr AS STRING), 0, 4) AS INT64) AS year,
     locnnbr AS store,
     ringinglocnnbr,
     trantypeind,
     a.mrchndssoldstscd,
     b.div_no,
     b.ln_no,
     b.cls_no,
     b.cls_ds,
     a.skuid,
     a.ProdIrlNbr,
     b.prd_ds AS item_dsc,
     z.ssn_cd,
     CONCAT(CAST(b.div_no AS STRING), "-", CAST(a.ProdIrlNbr AS STRING), '-', z.ssn_cd) AS prod_id,
     CASE
       WHEN SSN_CD IN ('C3', 'F2', 'H2', 'U2', 'S2') THEN 2012
       WHEN SSN_CD IN ('C4', 'F3', 'H3', 'U3', 'S3') THEN 2013
       WHEN SSN_CD IN ('C5', 'F4', 'H4', 'U4', 'S4') THEN 2014
       WHEN SSN_CD IN ('C6',
       'F5',
       'H5',
       'U5',
       'S5') THEN 2015
       WHEN SSN_CD IN ('C7', 'F6', 'H6', 'U6', 'S6') THEN 2016
       WHEN SSN_CD IN ('C8',
       'F7',
       'H7',
       'U7',
       'S7') THEN 2017
       WHEN SSN_CD IN ('C9', 'F8', 'H8', 'U8', 'S8') THEN 2018
       WHEN SSN_CD IN ('C10',
       'F9',
       'H9',
       'U9',
       'S9') THEN 2019
       ELSE 0
     END AS Season_Year,
     CASE
       WHEN SSN_CD LIKE 'C%' THEN 'basic'
       WHEN SSN_CD LIKE 'F%' THEN 'fall'
       WHEN SSN_CD LIKE 'H%' THEN 'winter'
       WHEN SSN_CD LIKE 'U%' THEN 'summer'
       WHEN SSN_CD LIKE 'S%' THEN 'spring'
       WHEN SSN_CD = '' THEN 'NA'
       ELSE 'special'
     END AS Season,
     SUM(NetSellAmt) AS NetSellAmt,
     SUM(lineitmgrosssoldamt) AS lineItmGrossSoldAmt,
     SUM(SlsMrgnAmt) AS slsMrgnAmt,
     SUM(sywearnamt) AS sywEarnAmt,
     SUM(sywburnamt) AS sywBurnAmt,
     SUM(UnitQty) AS UnitQty
     --,SUM(NetSellAmt) AS sales
     --,SUM(lineitmgrosssoldamt) as grssoldamt
     --,SUM(SlsMrgnAmt) AS margin
     --,SUM(sywearnamt) as sywearnamt
     --,SUM(sywburnamt) as sywburnamt
     --,COUNT(DISTINCT CONCAT(cast(LyltyCardNbr as string), cast(TranDt as string))) AS trips
     --,SUM(UnitQty) UNITS
   FROM
     `syw-analytics-repo-prod.l2_enterpriseanalytics.postrandtl` a
   LEFT JOIN
     `syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu` B
   ON
     a.ProdIrlNbr=b.prd_irl_no
   LEFT JOIN
     `syw-analytics-repo-prod.crm_mart1_tbls.crm_sears_retail_fty` C
   ON
     a.OrignlFcltyNbr=C.fty_id_no
   LEFT JOIN
     `syw-analytics-repo-prod.lci_dw_views.sprs_product` z
   ON
     a.ProdIrlNbr = z.prd_irl_no
   WHERE
     TranDt >'2017-01-01' -- date to be changed according to the reqiurement
     AND a.FmtSbtyp IN ('A',
       'B',
       'C',
       'D',
       'M')
     --AND lyltycardnbr IS NOT NULL  -- force member only sales
     AND SrsKmtInd='S'
     AND trantypeind = 'S'
     --and b.ln_ds = 'SIMPLY STYLED KNITS'
     --and b.cls_ds = 'SHORT SLEEVE'
     --and ProdIrlNbr = 17252887
     AND SrsDvsnNbr NOT IN (0,
       79)
     AND LyltyCardNbr IS NOT NULL
     AND wknbr > 201700
     AND b.div_no IN (select div_nbr from `syw-analytics-ff.apparel_als_all_app.inventory` group by 1)
     --and (z.ssn_cd like '%H%' or z.ssn_cd like '%F%')
   GROUP BY
     1,
     2,
     3,
     4,
     5,
     6,
     7,
     8,
     9,
     10,
     11,
     12,
     13,
     14,
     15 ) ) a
LEFT JOIN (
 SELECT
   DISTINCT Product_Type,
   PRD_IRL_NO,
   IMA_Product_Type,
   CONCAT(CAST(DIV_NO AS STRING), "_", CAST(LN_NO AS STRING), "_", CAST(CLS_NO AS STRING)) AS Class_Product_Type,
   CONCAT(CAST(DIV_NO AS STRING), "_", CAST(LN_NO AS STRING)) AS Line_Product_Type
 FROM
   `syw-analytics-ff.apparel_als_all_app.basic_spin_attributes_apparel` ) b
ON
 (a.ProdIrlNbr = b.PRD_IRL_NO)
LEFT JOIN (
 SELECT
   a1.div_nbr,
   a1.Product_Type AS Product_Type_B,
   a1.Season_Correspondence AS Season_Correspondence_B,
   a1.Season AS Season_B,
   wk_nbr,
   locn_nbr,
   CONCAT(prefix, "_", CAST(unique_id AS STRING)) AS assortment_id_prod
 FROM
   `apparel_als_all_app.stores_weekly_assortment_proc` a1
 LEFT JOIN `apparel_als_all_app.stores_weekly_assortment_proc_uniq_ids` a2
 ON (a1.div_nbr = a2.div_nbr AND a1.Product_Type = a2.Product_Type AND a1.Season = a2.Season AND a1.Season_Correspondence = a2.Season_Correspondence and a1.assortment_id = a2.assortment_id)) c
ON
 (a.wknbr = c.wk_nbr
   AND CAST(a.store AS STRING) = c.locn_nbr
   AND a.div_no = c.div_nbr
   AND b.Product_Type = c.Product_Type_B
   AND a.Season_Correspondence = c.Season_Correspondence_B
   AND a.Season = c.Season_B )
LEFT JOIN (
 SELECT
   a1.div_nbr,
   a1.Product_Type AS Product_Type_D,
   a1.Class_Product_Type,
   a1.Season_Correspondence AS Season_Correspondence_D,
   a1.Season AS Season_D,
   wk_nbr,
   locn_nbr,
   CONCAT(prefix, "_", CAST(unique_id AS STRING)) AS assortment_id_cls
 FROM
   `apparel_als_all_app.stores_weekly_assortment_proc_cls` a1
 LEFT JOIN `apparel_als_all_app.stores_weekly_assortment_proc_cls_uniq_ids` a2
 ON (a1.div_nbr = a2.div_nbr AND a1.Product_Type = a2.Product_Type AND a1.Season = a2.Season AND a1.Season_Correspondence = a2.Season_Correspondence and a1.assortment_id = a2.assortment_id)) d
ON
 (a.wknbr = d.wk_nbr
   AND CAST(a.store AS STRING) = d.locn_nbr
   AND a.div_no = d.div_nbr
   AND b.Product_Type = d.Product_Type_D
   AND a.Season_Correspondence = d.Season_Correspondence_D
   AND a.Season = d.Season_D
   AND b.Class_Product_Type = d.Class_Product_Type )
LEFT JOIN (
 SELECT
   a1.div_nbr,
   a1.Product_Type AS Product_Type_E,
   a1.IMA_Product_Type,
   a1.Season_Correspondence AS Season_Correspondence_E,
   a1.Season AS Season_E,
   wk_nbr,
   locn_nbr,
   CONCAT(prefix, "_", CAST(unique_id AS STRING)) AS assortment_id_imaprod
 FROM
   `apparel_als_all_app.stores_weekly_assortment_proc_ima` a1
   LEFT JOIN `apparel_als_all_app.stores_weekly_assortment_proc_ima_uniq_ids` a2
   ON (a1.div_nbr = a2.div_nbr AND a1.Product_Type = a2.Product_Type AND a1.Season = a2.Season AND a1.Season_Correspondence = a2.Season_Correspondence and a1.assortment_id = a2.assortment_id)) e
ON
 (a.wknbr = e.wk_nbr
   AND CAST(a.store AS STRING) = e.locn_nbr
   AND a.div_no = e.div_nbr
   AND b.Product_Type = e.Product_Type_E
   AND a.Season_Correspondence = e.Season_Correspondence_E
   AND a.Season = e.Season_E
   AND b.IMA_Product_Type = e.IMA_Product_Type )
LEFT JOIN (
 SELECT
   a1.div_nbr,
   a1.Product_Type AS Product_Type_D,
   a1.Line_Product_Type,
   a1.Season_Correspondence AS Season_Correspondence_D,
   a1.Season AS Season_D,
   wk_nbr,
   locn_nbr,
   CONCAT(prefix, "_", CAST(unique_id AS STRING)) AS assortment_id_line
 FROM
   `apparel_als_all_app.stores_weekly_assortment_proc_ln` a1
 LEFT JOIN `apparel_als_all_app.stores_weekly_assortment_proc_ln_uniq_ids` a2
   ON (a1.div_nbr = a2.div_nbr AND a1.Product_Type = a2.Product_Type AND a1.Season = a2.Season AND a1.Season_Correspondence = a2.Season_Correspondence and a1.assortment_id = a2.assortment_id)) f
ON
 (a.wknbr = f.wk_nbr
   AND CAST(a.store AS STRING) = f.locn_nbr
   AND a.div_no = f.div_nbr
   AND b.Product_Type = f.Product_Type_D
   AND a.Season_Correspondence = f.Season_Correspondence_D
   AND a.Season = f.Season_D
   AND b.Line_Product_Type = f.Line_Product_Type )
WHERE
 assortment_id_prod IS NOT NULL
 AND assortment_id_cls IS NOT NULL
 AND assortment_id_ln IS NOT NULL
 AND assortment_id_imaprod IS NOT NULL
*/
SELECT
 member_id,
 wknbr,
 store,
 ringinglocnnbr,
 trantypeind,
 mrchndssoldstscd,
 div_no,
 ln_no,
 cls_no,
 skuid,
 ProdIrlNbr,
 ssn_cd,
 prod_id,
 a.Season_Correspondence,
 a.Season,
 b.Product_Type,
 NetSellAmt,
 lineItmGrossSoldAmt,
 slsMrgnAmt,
 sywEarnAmt,
 sywBurnAmt,
 UnitQty,
 assortment_id_prod,
 assortment_id_line,
 assortment_id_cls,
 assortment_id_imaprod,
 '_ALL_' AS assortment_id_na
FROM (
 SELECT
   *,
   CASE
     WHEN Season != 'special' AND year <= Season_Year THEN 'current'
     WHEN Season != 'special'
   AND year > Season_Year THEN 'aged'
     ELSE 'special'
   END AS Season_Correspondence
 FROM (
   SELECT
     CAST(LyltyCardNbr AS string) AS member_id,
     wknbr,
     CAST(SUBSTR(CAST(wknbr AS STRING), 0, 4) AS INT64) AS year,
     locnnbr AS store,
     ringinglocnnbr,
     trantypeind,
     a.mrchndssoldstscd,
     b.div_no,
     b.ln_no,
     b.cls_no,
     b.cls_ds,
     a.skuid,
     a.ProdIrlNbr,
     b.prd_ds AS item_dsc,
     z.ssn_cd,
     CONCAT(CAST(b.div_no AS STRING), "-", CAST(a.ProdIrlNbr AS STRING), '-', z.ssn_cd) AS prod_id,
     CASE
       WHEN SSN_CD IN ('C3', 'F2', 'H2', 'U2', 'S2') THEN 2012
       WHEN SSN_CD IN ('C4', 'F3', 'H3', 'U3', 'S3') THEN 2013
       WHEN SSN_CD IN ('C5', 'F4', 'H4', 'U4', 'S4') THEN 2014
       WHEN SSN_CD IN ('C6',
       'F5',
       'H5',
       'U5',
       'S5') THEN 2015
       WHEN SSN_CD IN ('C7', 'F6', 'H6', 'U6', 'S6') THEN 2016
       WHEN SSN_CD IN ('C8',
       'F7',
       'H7',
       'U7',
       'S7') THEN 2017
       WHEN SSN_CD IN ('C9', 'F8', 'H8', 'U8', 'S8') THEN 2018
       WHEN SSN_CD IN ('C10',
       'F9',
       'H9',
       'U9',
       'S9') THEN 2019
       ELSE 0
     END AS Season_Year,
     CASE
       WHEN SSN_CD LIKE 'C%' THEN 'basic'
       WHEN SSN_CD LIKE 'F%' THEN 'fall'
       WHEN SSN_CD LIKE 'H%' THEN 'winter'
       WHEN SSN_CD LIKE 'U%' THEN 'summer'
       WHEN SSN_CD LIKE 'S%' THEN 'spring'
       WHEN SSN_CD = '' THEN 'NA'
       ELSE 'special'
     END AS Season,
     SUM(NetSellAmt) AS NetSellAmt,
     SUM(lineitmgrosssoldamt) AS lineItmGrossSoldAmt,
     SUM(SlsMrgnAmt) AS slsMrgnAmt,
     SUM(sywearnamt) AS sywEarnAmt,
     SUM(sywburnamt) AS sywBurnAmt,
     SUM(UnitQty) AS UnitQty
     --,SUM(NetSellAmt) AS sales
     --,SUM(lineitmgrosssoldamt) as grssoldamt
     --,SUM(SlsMrgnAmt) AS margin
     --,SUM(sywearnamt) as sywearnamt
     --,SUM(sywburnamt) as sywburnamt
     --,COUNT(DISTINCT CONCAT(cast(LyltyCardNbr as string), cast(TranDt as string))) AS trips
     --,SUM(UnitQty) UNITS
   FROM
     `syw-analytics-repo-prod.l2_enterpriseanalytics.postrandtl` a
   LEFT JOIN
     `syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu` B
   ON
     a.ProdIrlNbr=b.prd_irl_no
   LEFT JOIN
     `syw-analytics-repo-prod.crm_mart1_tbls.crm_sears_retail_fty` C
   ON
     a.OrignlFcltyNbr=C.fty_id_no
   LEFT JOIN
     `syw-analytics-repo-prod.lci_dw_views.sprs_product` z
   ON
     a.ProdIrlNbr = z.prd_irl_no
   WHERE
     TranDt >'2017-01-01' -- date to be changed according to the reqiurement
     AND a.FmtSbtyp IN ('A',
       'B',
       'C',
       'D',
       'M')
     --AND lyltycardnbr IS NOT NULL  -- force member only sales
     AND SrsKmtInd='S'
     AND trantypeind = 'S'
     --and b.ln_ds = 'SIMPLY STYLED KNITS'
     --and b.cls_ds = 'SHORT SLEEVE'
     --and ProdIrlNbr = 17252887
     AND SrsDvsnNbr NOT IN (0,
       79)
     AND LyltyCardNbr IS NOT NULL
     AND wknbr > 201700
     AND b.div_no IN (select div_nbr from `syw-analytics-ff.apparel_als_all_app.inventory` group by 1)
     --and (z.ssn_cd like '%H%' or z.ssn_cd like '%F%')
   GROUP BY
     1,
     2,
     3,
     4,
     5,
     6,
     7,
     8,
     9,
     10,
     11,
     12,
     13,
     14,
     15 ) ) a
LEFT JOIN (
 SELECT
   DISTINCT Product_Type,
   PRD_IRL_NO,
   IMA_Product_Type,
   CONCAT(CAST(DIV_NO AS STRING), "_", CAST(LN_NO AS STRING), "_", CAST(CLS_NO AS STRING)) AS Class_Product_Type,
   CONCAT(CAST(DIV_NO AS STRING), "_", CAST(LN_NO AS STRING)) AS Line_Product_Type
 FROM
   `syw-analytics-ff.apparel_als_all_app.basic_spin_attributes_apparel` ) b
ON
 (CAST(a.ProdIrlNbr AS STRING) = b.PRD_IRL_NO)
LEFT JOIN (
 SELECT
   a1.div_nbr,
   a1.Product_Type AS Product_Type_B,
   a1.Season_Correspondence AS Season_Correspondence_B,
   a1.Season AS Season_B,
   wk_nbr,
   locn_nbr,
   CONCAT(prefix, "_", CAST(unique_id AS STRING)) AS assortment_id_prod
 FROM
   `apparel_als_all_app.stores_weekly_assortment_proc` a1
 LEFT JOIN `apparel_als_all_app.stores_weekly_assortment_proc_uniq_ids` a2
 ON (a1.div_nbr = a2.div_nbr AND a1.Product_Type = a2.Product_Type AND a1.Season = a2.Season AND a1.Season_Correspondence = a2.Season_Correspondence and a1.assortment_id = a2.assortment_id)) c
ON
 (a.wknbr = c.wk_nbr
   AND CAST(a.store AS STRING) = c.locn_nbr
   AND a.div_no = c.div_nbr
   AND b.Product_Type = c.Product_Type_B
   AND a.Season_Correspondence = c.Season_Correspondence_B
   AND a.Season = c.Season_B )
LEFT JOIN (
 SELECT
   a1.div_nbr,
   a1.Product_Type AS Product_Type_D,
   a1.Class_Product_Type,
   a1.Season_Correspondence AS Season_Correspondence_D,
   a1.Season AS Season_D,
   wk_nbr,
   locn_nbr,
   CONCAT(prefix, "_", CAST(unique_id AS STRING)) AS assortment_id_cls
 FROM
   `apparel_als_all_app.stores_weekly_assortment_proc_cls` a1
 LEFT JOIN `apparel_ao_v2_fw.stores_weekly_assortment_proc_cls_uniq_ids` a2
 ON (a1.div_nbr = a2.div_nbr AND a1.Product_Type = a2.Product_Type AND a1.Season = a2.Season AND a1.Season_Correspondence = a2.Season_Correspondence and a1.assortment_id = a2.assortment_id)) d
ON
 (a.wknbr = d.wk_nbr
   AND CAST(a.store AS STRING) = d.locn_nbr
   AND a.div_no = d.div_nbr
   AND b.Product_Type = d.Product_Type_D
   AND a.Season_Correspondence = d.Season_Correspondence_D
   AND a.Season = d.Season_D
   AND b.Class_Product_Type = d.Class_Product_Type )
LEFT JOIN (
 SELECT
   a1.div_nbr,
   a1.Product_Type AS Product_Type_E,
   a1.IMA_Product_Type,
   a1.Season_Correspondence AS Season_Correspondence_E,
   a1.Season AS Season_E,
   wk_nbr,
   locn_nbr,
   CONCAT(prefix, "_", CAST(unique_id AS STRING)) AS assortment_id_imaprod
 FROM
   `apparel_als_all_app.stores_weekly_assortment_proc_ima` a1
   LEFT JOIN `apparel_ao_v2_fw.stores_weekly_assortment_proc_ima_uniq_ids` a2
   ON (a1.div_nbr = a2.div_nbr AND a1.Product_Type = a2.Product_Type AND a1.Season = a2.Season AND a1.Season_Correspondence = a2.Season_Correspondence and a1.assortment_id = a2.assortment_id)) e
ON
 (a.wknbr = e.wk_nbr
   AND CAST(a.store AS STRING) = e.locn_nbr
   AND a.div_no = e.div_nbr
   AND b.Product_Type = e.Product_Type_E
   AND a.Season_Correspondence = e.Season_Correspondence_E
   AND a.Season = e.Season_E
   AND b.IMA_Product_Type = e.IMA_Product_Type )
LEFT JOIN (
 SELECT
   a1.div_nbr,
   a1.Product_Type AS Product_Type_D,
   a1.Line_Product_Type,
   a1.Season_Correspondence AS Season_Correspondence_D,
   a1.Season AS Season_D,
   wk_nbr,
   locn_nbr,
   CONCAT(prefix, "_", CAST(unique_id AS STRING)) AS assortment_id_line
 FROM
   `apparel_als_all_app.stores_weekly_assortment_proc_ln` a1
 LEFT JOIN `apparel_ao_v2_fw.stores_weekly_assortment_proc_ln_uniq_ids` a2
   ON (a1.div_nbr = a2.div_nbr AND a1.Product_Type = a2.Product_Type AND a1.Season = a2.Season AND a1.Season_Correspondence = a2.Season_Correspondence and a1.assortment_id = a2.assortment_id)) f
ON
 (a.wknbr = f.wk_nbr
   AND CAST(a.store AS STRING) = f.locn_nbr
   AND a.div_no = f.div_nbr
   AND b.Product_Type = f.Product_Type_D
   AND a.Season_Correspondence = f.Season_Correspondence_D
   AND a.Season = f.Season_D
   AND b.Line_Product_Type = f.Line_Product_Type )
WHERE
 assortment_id_prod IS NOT NULL
 AND assortment_id_cls IS NOT NULL
 AND assortment_id_line IS NOT NULL
 AND assortment_id_imaprod IS NOT NULL
