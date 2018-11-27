#!/bin/bash

store=$1
div=$2
soar=$3
season=$4

echo Preparing Store Data For store $store $season ... 
bq query 
--allow_large_results
--use_legacy_sql=False 
--destination_table apparel_ao_v2_allstores.store_data_store_$store'_'$season 
--parameter=str:INT64:$store 
# per store, rank all member based on descending sale units
"WITH   member_rank AS (   
    SELECT     locnnbr
               ,lyltycardnbr
               , ROW_NUMBER() OVER(PARTITION BY locnnbr ORDER BY UNITS DESC) AS mem_rnk   
          FROM     \`apparel_jaccard_pp.store_members_all_uf_fw\` )
,store_members_all AS (   
    SELECT     *   
          FROM     member_rank )
# per store, filter out top 15K member 
,store_members_all_filtered AS (   
    SELECT     *  
          FROM     member_rank   
          WHERE     mem_rnk <= 15000 )
,store_members AS (   
    SELECT     *   
          FROM     store_members_all_filtered )
#per member of base store, ranking its similar member based on weighted-pair distances
,member_ranking AS (   
    SELECT     a.locnnbr AS storeA
              ,a.entityA
              ,a.entityB
              ,a.d_avg
              ,b.locnnbr AS storeB
              ,ROW_NUMBER() OVER(PARTITION BY a.locnnbr, a.entityA ORDER BY a.d_avg) AS rnk   
           FROM     \`apparel_jaccard_pp.similar_members_fw\` AS a   
                LEFT JOIN     store_members AS b   
                ON     a.entityA = b.lyltycardnbr   
           WHERE     b.lyltycardnbr IS NOT NULL )
# per top 15k member, choose top 10 similar members
,similar_top10_members AS (   
    SELECT     *   
          FROM     member_ranking   WHERE     rnk <= 10 )
#per store, calculate number of similar members of other stores as 'num_sim_mem'    
,unique_similar_members AS (   
    SELECT     *
              ,COUNT(entityB) OVER(PARTITION BY storeA) AS num_sim_mem   
           FROM (SELECT       
                        a.storeA
                        ,a.entityB
                        ,a.storeB
                        ,b.locn_desc     
                     FROM       similar_top10_members AS a     
                          LEFT JOIN       \`MM_REC_LL.Store_Location\` AS b     
                          ON       a.storeB = b.locn_nbr     
                     WHERE       a.storeB IS NOT NULL     
                     GROUP BY       1,       2,       3,       4 ) )
# per store for each similar member, look at their transaction record to find what product they purchased, exclude those purchased at other stores.
# limit items in perticular sears apparel,transactiontype,soar no, ssn_cd
,items_purchased_by_similar_members AS (   
    SELECT     a.storeA
               ,a.num_sim_mem
               ,c.soar_no
               ,c.div_no
               ,c.div_nm
               ,c.ln_no
               ,c.ln_ds
               ,c.cls_no
               ,c.cls_ds
               ,b.prodirlnbr
               ,c.prd_ds
               ,STRING_AGG(DISTINCT CAST(b.locnnbr AS string), ',') AS store_pool
               ,COUNT(DISTINCT a.entityB)AS num_mem_who_purchased_item
               ,SUM(b.UnitQty) AS UNITS   
           FROM     unique_similar_members AS a   
                LEFT JOIN     \`syw-analytics-repo-prod.l2_enterpriseanalytics.postrandtl\` AS b   
                ON     a.entityB = b.lyltycardnbr   
                LEFT JOIN     \`syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu\` AS c   
                ON     b.ProdIrlNbr=c.prd_irl_no   
                LEFT JOIN     \`syw-analytics-repo-prod.lci_dw_views.sprs_product\` z   
                ON     b.ProdIrlNbr = z.prd_irl_no   
           WHERE     b.TranDt >'2017-04-30' AND b.TranDt <'2017-12-31'     
                      AND b.FmtSbtyp IN ('A','B','C','D','M')     
                      AND b.lyltycardnbr IS NOT NULL     
                      AND b.SrsKmtInd='S'     
                      AND b.trantypeind = 'S'     
                      AND b.SrsDvsnNbr NOT IN (0,79)     
                      AND c.soar_no IN (101,102,103,104,105)     
                      AND a.storeA <> b.locnnbr     
                      AND z.ssn_cd IN ('H7','F7')   
                      GROUP BY  1,2,3,4,5,6,7,8,9,10,11   
                      HAVING     UNITS > 0 )
# base store purchased items at the same period, and sum the quantity
,items_purchased_by_store_members AS (   
    SELECT     b.locnnbr
              ,c.soar_no
              ,c.div_no
              ,c.div_nm
              ,c.ln_no
              ,c.ln_ds
              ,c.cls_no
              ,c.cls_ds
              ,b.prodirlnbr
              ,c.PRD_DS
              ,SUM(b.UnitQty) AS UNITS   
          FROM     \`syw-analytics-repo-prod.l2_enterpriseanalytics.postrandtl\` AS b   
                LEFT JOIN     \`syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu\` AS c   
                ON     b.ProdIrlNbr=c.prd_irl_no   
                LEFT JOIN     \`syw-analytics-repo-prod.lci_dw_views.sprs_product\` z   
                ON     b.ProdIrlNbr = z.prd_irl_no  
          WHERE     b.TranDt >'2017-04-30' AND b.TranDt <'2017-12-31'     
                    AND b.FmtSbtyp IN ('A','B','C','D','M')     
                    AND b.lyltycardnbr IS NOT NULL     
                    AND b.SrsKmtInd='S'     
                    AND b.trantypeind = 'S'     
                    AND b.SrsDvsnNbr NOT IN (0,79)     
                    AND b.locnnbr <> 9300     
                    AND b.ringinglocnnbr <> 9300     
                    AND c.soar_no IN (101,102,103,104,105)     
                    AND b.locnnbr IN ( SELECT       
                                                storeA    
                                            FROM       items_purchased_by_similar_members     
                                            GROUP BY  1)     
                    AND z.ssn_cd IN ('H7','F7')   
           GROUP BY  1,2,3,4,5,6,7,8,9,10   
           HAVING     UNITS > 0 )
# filter out the product which is not sold at base store.
,items_to_be_introduced AS (   
    SELECT     a.storeA
              ,a.num_sim_mem
              ,a.soar_no
              ,a.div_no
              ,a.div_nm
              ,a.ln_no
              ,a.ln_ds
              ,a.cls_no
              ,a.cls_ds
              ,a.prodirlnbr
              ,a.prd_ds
              ,a.store_pool
              ,a.num_mem_who_purchased_item
              ,a.UNITS   
        FROM     items_purchased_by_similar_members AS a   
                LEFT JOIN     items_purchased_by_store_members AS b   
                ON     a.storeA = b.locnnbr AND a.prodirlnbr = b.prodirlnbr   
        WHERE     b.locnnbr IS NULL AND b.prodirlnbr IS NULL   
        ORDER BY     storeA )
,sample AS (   
    SELECT     *   
        FROM     items_to_be_introduced )
# per item,calculate different types of inventory by store by week for the last year.
,inv AS (   
    SELECT     CAST(FLOOR(SKU_ID/1000) AS int64) AS prodirlnbr
               ,LOCN_NBR AS locn_nbr
               ,WK_NBR AS wk_nbr
               ,WK_END_DT AS wk_end_dt
               ,SUM(CASE WHEN INS_TYP_CD IN ('H') AND INS_SUB_TYP_CD NOT IN ('R', 'D', 'N') THEN TTL_UN_QT ELSE 0 END) on_hand_inv_units
               ,SUM(CASE WHEN INS_TYP_CD IN ('H') AND INS_SUB_TYP_CD IN ('D', 'N') THEN TTL_UN_QT ELSE 0 END) damaged_or_unsellable_inv_units
               ,SUM(CASE WHEN INS_TYP_CD IN ('I') THEN TTL_UN_QT ELSE 0 END) in_transit_inv_units
               ,SUM(CASE WHEN INS_TYP_CD IN ('O') THEN TTL_UN_QT ELSE 0 END) on_order_inv_units   
          FROM     \`syw-analytics-repo-prod.alex_arp_views_prd.fact_srs_wkly_opr_ins\`  
          WHERE     WK_END_DT BETWEEN DATE('2017-01-01') AND DATE('2017-12-31')     
                    AND TTL_UN_QT > 0   
          GROUP BY  1,2,3,4 )
 #per item per store, calculate the total receipts of last year
,rcp AS (   
    SELECT     CAST(FLOOR(SKU_ID/1000) AS int64) AS prodirlnbr
               ,locn_nbr
               ,SUM(reciept_units) AS ttl_reciept_units   
          FROM (     SELECT       DISTINCT SKU_ID AS sku_id
                                 ,LOCN_NBR AS locn_nbr
                                 ,DAY_NBR AS day_nbr
                                 ,RCP_UN_QT AS reciept_units
                                 ,RCP_CST_DLR AS reciept_cost_dlr     
                           FROM       \`syw-analytics-repo-prod.alex_arp_views_prd.fact_srs_dly_opr_rcp\`    
                           WHERE       DAY_NBR BETWEEN '2017-01-01' AND '2017-12-31' )   
          GROUP BY     1,2   
          ORDER BY     2 )
# per item per store,from items_to_be_introduced select max_on_hand_inv_units<6, and don't have receipt_units.
,store_items AS (   
    SELECT     s.storeA
               ,s.num_sim_mem
               ,s.div_no
               ,s.div_nm
               ,s.ln_no
               ,s.ln_ds
               ,s.cls_no
               ,s.cls_ds
               ,s.prd_ds
               ,s.prodirlnbr
               ,e.ttl_reciept_units AS str_receipt_units
               ,f.max_on_hand_inv_units AS str_max_on_hand   
          FROM     sample AS s   
                LEFT JOIN (  SELECT * FROM rcp     
                               WHERE    prodirlnbr IN ( SELECT prodirlnbr FROM sample GROUP BY  1) ) AS e   
                ON  s.prodirlnbr = e.prodirlnbr AND s.storeA = e.locn_nbr   
                LEFT JOIN (  SELECT  prodirlnbr
                                     ,locn_nbr
                                     ,MAX(on_hand_inv_units) AS max_on_hand_inv_units     
                                FROM  inv  GROUP BY  1,2) AS f   
                ON  s.prodirlnbr = f.prodirlnbr AND s.storeA = f.locn_nbr   
           WHERE  e.ttl_reciept_units IS NULL     
                  AND coalesce(f.max_on_hand_inv_units, 0) < 6 
# calculate total sales units per item for mrchndssoldstscd IN ('R','P') 
,all_sales_L1Y AS (   
    SELECT     b.locnnbr
               ,c.soar_no
               ,c.div_no
               ,c.div_nm
               ,c.ln_no
               ,c.ln_ds
               ,c.cls_no
               ,c.cls_ds
               ,b.prodirlnbr
               ,c.itm_no
               ,c.PRD_DS
               ,c.NAT_SLL_PRC
               ,c.NAT_CST_PRC
               ,SUM(b.UnitQty) AS UNITS   
         FROM     \`syw-analytics-repo-prod.l2_enterpriseanalytics.postrandtl\` AS b   
                LEFT JOIN     \`syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu\` AS c   
                ON     b.ProdIrlNbr=c.prd_irl_no   
                LEFT JOIN     \`syw-analytics-repo-prod.lci_dw_views.sprs_product\` z   
                ON     b.ProdIrlNbr = z.prd_irl_no   
         WHERE     b.TranDt >'2017-04-30' AND b.TranDt <'2017-12-31'     
                   AND b.SrsKmtInd='S'     
                   AND b.trantypeind = 'S'     
                   AND b.SrsDvsnNbr NOT IN (0,79)     
                   AND b.locnnbr <> 9300     
                   AND b.ringinglocnnbr <> 9300     
                   AND c.soar_no IN (101,102,103,104,105)     
                   AND b.mrchndssoldstscd IN ('R','P')   
         GROUP BY  1,2,3,4,5,6,7,8,9,10,11,12,13   
         HAVING    UNITS > 0 )
,cluster_sales AS (   
    SELECT     a.storeA
               ,b.*
               ,a.sim_mem_cnt   
         FROM     \`apparel_jaccard_pp.sim_stores_10_fw\` AS a   
                LEFT JOIN     all_sales_L1Y AS b   
                ON     a.storeB = b.locnnbr   
         WHERE  a.storeA IN ( SELECT storeA  FROM items_purchased_by_similar_members GROUP BY 1)     
                 AND CONCAT(CAST(a.storeA AS string), '_', CAST(b.prodirlnbr AS string)) 
                 IN ( SELECT   CONCAT(CAST(storeA AS string), '_', CAST(prodirlnbr AS string))  FROM store_items))
,cluster_sales_rank AS (   
    SELECT     *
               ,MAX(UNITS) OVER(PARTITION BY storeA, prodirlnbr) AS cluster_max
               ,MAX(sim_mem_cnt) OVER(PARTITION BY storeA) AS max_sim_mem_cnt   
          FROM     cluster_sales ) 
                              


========================running queries==============================
SELECT   a11.* EXCEPT(sim_mem_cnt,max_sim_mem_cnt)
        ,'new' AS type
        ,NULL AS sell_thru
        ,NULL AS otd_amt
        ,NULL AS otd_pc
        ,a11.sim_mem_cnt
        ,a11.max_sim_mem_cnt
        ,a12.div_units AS locnnbr_div_units
        ,a13.div_units AS storeA_div_units 
    FROM   cluster_sales_rank a11 
         LEFT JOIN ( SELECT     x11.locnnbr
                                ,x11.soar_no
                                ,x11.div_no
                                ,SUM(x11.UNITS) AS div_units   
                         FROM   all_sales_L1Y x11   
                         GROUP BY   1,2,3 ) a12 
         ON   a11.locnnbr =a12.locnnbr   AND a11.soar_no = a12.soar_no   AND a11.div_no = a12.div_no 
         LEFT JOIN ( SELECT     x11.locnnbr
                                ,x11.soar_no
                                ,x11.div_no
                                ,SUM(x11.UNITS) AS div_units   
                         FROM    all_sales_L1Y x11   
                         GROUP BY   1,2,3 ) a13 
          ON   a11.storeA =a13.locnnbr   AND a11.soar_no = a13.soar_no   AND a11.div_no = a13.div_no 
    WHERE   a11.UNITS = a11.cluster_max   
            AND a11.soar_no IN (101, 102,103, 104, 105)   
            AND a11.storeA = @str 
=====================UNION ALL=========================            
UNION ALL 

SELECT   @str AS storeA
        ,b.locnnbr
        ,c.soar_no
        ,c.div_no
        ,c.div_nm
        ,c.ln_no
        ,c.ln_ds
        ,c.cls_no
        ,c.cls_ds
        ,b.prodirlnbr
        ,c.ITM_NO
        ,c.PRD_DS
        ,c.NAT_SLL_PRC
        ,c.NAT_CST_PRC
        ,SUM(b.UnitQty) AS UNITS
        ,0 AS cluster_max
        ,'existing' AS type
        ,SUM(b.UnitQty)/ (SUM(b.UnitQty) + AVG(d.on_hand_inv_units)) AS sell_thru
        ,SUM(b.UnitQty*b.otdamt )/SUM(b.UnitQty) AS otd_amt
        ,(SUM(b.UnitQty*b.otdamt )/SUM(b.UnitQty))/c.NAT_SLL_PRC AS otd_pc
        ,NULL AS sim_mem_cnt
        ,NULL AS max_sim_mem_cnt
        ,NULL AS locnnbr_div_units
        ,NULL AS storeA_div_units 
    FROM   \`syw-analytics-repo-prod.l2_enterpriseanalytics.postrandtl\` AS b 
            LEFT JOIN   \`syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu\` AS c 
            ON   b.ProdIrlNbr=c.prd_irl_no 
            LEFT JOIN   \`syw-analytics-repo-prod.lci_dw_views.sprs_product\` z 
            ON   b.ProdIrlNbr = z.prd_irl_no 
            LEFT JOIN ( SELECT     prodirlnbr
                            ,on_hand_inv_units   
                        FROM ( SELECT   prodirlnbr
                                        ,locn_nbr
                                        ,wk_nbr
                                        ,on_hand_inv_units
                                        ,RANK() OVER (PARTITION BY prodirlnbr, locn_nbr ORDER BY wk_nbr DESC) AS rn     
                                    FROM    inv     
                                    WHERE   locn_nbr = @str )   
                        WHERE     rn = 1 ) AS d 
     ON   b.ProdIrlNbr = d.prodirlnbr 
     WHERE   b.TranDt >'2017-04-30' AND b.TranDt <'2017-12-31'   
                AND b.FmtSbtyp IN ('A','B','C','D','M')   
                AND b.mrchndssoldstscd IN ('R','P')   
                AND b.SrsKmtInd='S'   
                AND b.trantypeind = 'S'   
                AND b.SrsDvsnNbr NOT IN (0,79)   
                AND b.locnnbr <> 9300   
                AND b.ringinglocnnbr <> 9300   
                AND c.soar_no IN (101,102,103,104,105)   
                AND b.locnnbr = @str   
                AND z.ssn_cd IN ('H7','F7')   
                AND c.soar_no IN (101, 102, 103, 104, 105)   
                AND c.NAT_SLL_PRC > 0 
      GROUP BY   1,2,3,4,5,6,7,8,9,10,11,12,13,14 
      HAVING   UNITS > 0"

echo Writing to Google Storage store $store ... 
bq extract apparel_ao_v2_allstores.store_data_store_$store'_'$season gs://ao-v2-bucket/store-data-fw/store_data_store_$store'_'$soar'_'$div'_'$season.csv

echo Moving to the local instance...
gsutil cp gs://ao-v2-bucket/store-data-fw/store_data_store_$store'_'$soar'_'$div'_'$season.csv ./$store'_'$soar'_'$div'_'$season/

exit 0
