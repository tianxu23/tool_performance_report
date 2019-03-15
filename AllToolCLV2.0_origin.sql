--Upgraded Tool performance dashboard  to CLV2.0 

CREATE MULTISET VOLATILE TABLE impression AS
(SELECT
    COALESCE(ams_prgrm_id, -999) AS ams_prgrm_id
    ,IMPRSN_DT
    ,COALESCE(pblshr_id, -999) AS pblshr_id
    ,ams_tool_id
    ,SUM(CASE WHEN trfc_src_cd IN (1, 2, 3) THEN 1 ELSE 0 END) AS impr_mobile
    ,COUNT(IMPRSN_CNTNR_ID) AS impr_all
FROM prs_ams_v.AMS_IMPRSN_CNTNR
WHERE IMPRSN_DT between '2018-02-01' and  current_date 
    AND ams_trans_rsn_cd = 0
GROUP BY 1, 2, 3, 4
HAVING impr_all > 0
) WITH DATA PRIMARY INDEX (IMPRSN_DT, pblshr_id, ams_prgrm_id)
ON COMMIT PRESERVE ROWS;




sel min(click_dt) from click
--Click 
--- DROP TABLE click;
CREATE MULTISET VOLATILE TABLE click AS
(SELECT
    COALESCE(ams_prgrm_id, -999) AS ams_prgrm_id
    ,click_dt
    ,COALESCE(pblshr_id, -999) AS pblshr_id
    ,ams_tool_id
    ,SUM(CASE WHEN trfc_src_cd IN (1, 2, 3) THEN 1 ELSE 0 END) AS click_mobile
    ,COUNT(1) AS click_all
FROM prs_ams_v.ams_click
WHERE click_dt between '2018-02-01' and  current_date 
    AND ams_trans_rsn_cd = 0
GROUP BY 1, 2, 3, 4
HAVING click_all > 0
) WITH DATA PRIMARY INDEX (click_dt, pblshr_id, ams_prgrm_id)
ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (AMS_PRGRM_ID ,CLICK_DT ,PBLSHR_ID,AMS_TOOL_ID)   ON click;
COLLECT STATISTICS COLUMN (AMS_PRGRM_ID ,IMPRSN_DT ,PBLSHR_ID,AMS_TOOL_ID)   ON impression;
    



CREATE volatile TABLE impr_click as (
sel b.IMPRSN_DT AS cal_dt
,b.ams_prgrm_id
,b.pblshr_id
,b.AMS_TOOL_ID
,COALESCE(a.click_mobile, 0) AS click_mobile
,COALESCE(a.click_all, 0) AS click_all
,COALESCE(b.impr_mobile, 0) AS impr_mobile
,COALESCE(b.impr_all, 0) AS impr_all
From click a
full join impression b on a.CLICK_dt = b.IMPRSN_DT and a. ams_prgrm_id = b.ams_prgrm_id and a.pblshr_id = b.pblshr_id and COALESCE(a.ams_tool_id, '(no value)') = COALESCE(b.ams_tool_id, '(no value)')
)WITH DATA PRIMARY INDEX(cal_dt
,ams_prgrm_id
,pblshr_id
,ams_tool_id) ON COMMIT PRESERVE ROWS
;
--Transection  

CREATE MULTISET VOLATILE TABLE click_table AS
(
SEL 
	CAST(CLICK_TS AS DATE) AS CLICK_dt,
	click_id,
	AMS_PRGRM_ID,
	ams_tool_id,
	pblshr_id
FROM PRS_AMS_V.AMS_CLICK a			
WHERE 1=1			
---AND AMS_TRANS_RSN_CD=0
AND CLICK_dt between  '2018-02-01'  - 365  and  current_date  --- back to 1 year click
) WITH DATA PRIMARY INDEX (CLICK_dt, pblshr_id, ams_prgrm_id,ams_tool_id)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (click_id) On click_table;

DROP TABLE TRANS;
CREATE MULTISET VOLATILE TABLE TRANS  AS
(SELECT
    fam.CK_TRANS_DT AS ck_trans_dt
    ,fam.ams_prgrm_id 
	,fam.EPN_PBLSHR_ID
	,c.ams_tool_id
   -- ,COALESCE(c.ams_tool_id, '(no value)') as ams_tool_id
    ,SUM(CASE WHEN  clv_dup_ind =0 and DEVICE_TYPE_ID IN (1)  THEN coalesce(GMB_PLAN_RATE_AMT,0) ELSE 0 END) AS GMB_24HR_desktop -- GMB
    ,SUM(CASE WHEN  clv_dup_ind =0 and DEVICE_TYPE_ID IN (2,3)  THEN coalesce(GMB_PLAN_RATE_AMT,0) ELSE 0 END) AS GMB_BBOWAC_mobile -- GMB
    ,SUM(CASE WHEN DEVICE_TYPE_ID IN (1)  THEN coalesce(fam.IGMB_PLAN_RATE_AMT,0)ELSE 0 END) AS fam2_IGMB_desktop
    ,SUM(CASE WHEN DEVICE_TYPE_ID IN (2,3)  THEN coalesce(fam.IGMB_PLAN_RATE_AMT,0)ELSE 0 END) AS iGMB_BBOWAC_mobile
    ,count(distinct case when DEVICE_TYPE_ID IN (1)  THEN CK_TRANS_ID||ITEM_ID end) AS fam2_trx_desktop
	,count(distinct case when DEVICE_TYPE_ID IN (2,3)  THEN CK_TRANS_ID||ITEM_ID end) AS fam3_trx_mobile
    ,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) and DEVICE_TYPE_ID IN (1) THEN 1 ELSE 0 END) AS fam2_norb_desktop
	   ,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) and DEVICE_TYPE_ID IN (2,3) THEN 1 ELSE 0 END) AS fam3_norb_mobile
    --,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) THEN  INCR_FCTR ELSE 0 END ) AS INORB
    FROM  PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT AS fam
LEFT OUTER JOIN click_table AS c
	ON fam.RVR_ID = c.click_id 
WHERE fam.MPX_CHNL_ID = 6
AND fam.CK_TRANS_DT  between  '2018-02-01' and  current_date 
--AND fam.client_id = fam.client_id_global                             ---- added to sync with FAM3, excluding GBH/Geox from reporting.
AND fam.EPN_PBLSHR_ID <> -999
GROUP BY 1,2,3,4

) WITH DATA PRIMARY INDEX (ck_trans_dt, ams_prgrm_id,ams_tool_id)
ON COMMIT PRESERVE ROWS;

------ Spend 
CREATE volatile TABLE MPX_spend_2 as (
select
TRANS_DT,
AMS_PRGRM_ID,
ams_tool_id,
AMS_PBLSHR_ID,
--CASE WHEN a.trfc_src_cd <> 0 THEN 'Mobile' ELSE 'Desktop' END as DEVICE ,
sum( CASE WHEN a.trfc_src_cd <> 0 THEN COALESCE(ERNG_USD,0.00) else 0 end) as Spend_Mobile,
sum( CASE WHEN a.trfc_src_cd = 0 THEN COALESCE(ERNG_USD,0.00) else 0 end) as Spend_Desktop,
sum(COALESCE(ERNG_USD,0.00)) as Spend
FROM prs_ams_v.AMS_PBLSHR_ERNG a
where
TRANS_DT   between  '2018-02-01' and  current_date 
group by 1,2,3,4
) WITH DATA PRIMARY INDEX(TRANS_DT
,AMS_PRGRM_ID
,ams_tool_id,AMS_PBLSHR_ID) on commit preserve rows;

COLLECT STATISTICS COLUMN (AMS_PRGRM_ID ,CLICK_DT ,PBLSHR_ID,AMS_TOOL_ID)   ON click;
COLLECT STATISTICS COLUMN (AMS_PRGRM_ID ,IMPRSN_DT ,PBLSHR_ID,AMS_TOOL_ID)   ON impression;
COLLECT STATISTICS COLUMN (CK_TRANS_DT ,AMS_PRGRM_ID  ,EPN_PBLSHR_ID) ON TRANS;
COLLECT STATISTICS COLUMN (AMS_TOOL_ID,AMS_PRGRM_ID,AMS_PBLSHR_ID) ON TRANS;




DROP TABLE dtl_pb_tool;
CREATE volatile TABLE dtl_pb_tool as (
sel a.CLICK_DT AS cal_dt
,a.ams_prgrm_id
,a.pblshr_id
,a.AMS_TOOL_ID
,COALESCE(a.click_mobile, 0) AS click_mobile
,COALESCE(a.click_all, 0) AS click_all
,COALESCE(a.impr_mobile, 0) AS impr_mobile
,COALESCE(a.impr_all, 0) AS impr_all
,COALESCE(fam2.GMB_24HR_desktop,0.00) AS GMB_24HR_desktop
,0 AS GMB_24HR_all
,COALESCE(fam2.fam2_IGMB_desktop, 0.00) AS fam2_iGMB_desktop
,0 AS fam2_iGMB_all
,COALESCE(fam2.fam2_trx_desktop, 0) AS fam2_trx_desktop
,0 AS fam2_trx_all
,COALESCE(fam2.fam2_norb_desktop, 0) AS fam2_norb_desktop
,0 AS fam2_norb_all
,COALESCE(fam2.GMB_BBOWAC_mobile, 0.00) AS GMB_BBOWAC_mobile
,0 AS GMB_BBOWAC_all
,COALESCE(fam2.iGMB_BBOWAC_mobile, 0.00) AS iGMB_BBOWAC_mobile
,0 AS iGMB_BBOWAC_all
,COALESCE(fam2.fam3_trx_mobile, 0) AS fam3_trx_mobile
,0 AS fam3_trx_all
,COALESCE(fam2.fam3_norb_mobile, 0) AS fam3_norb_mobile
,0 AS fam3_norb_all
,COALESCE(b.Spend_Mobile, 0) AS Spend_Mobile
,COALESCE(b.Spend_Desktop, 0) AS Spend_Desktop
,COALESCE(b.Spend, 0) AS Spend_All
From impr_click a
full join TRANS fam2 on fam2.ck_trans_dt = a.cal_dt and fam2. ams_prgrm_id = a.ams_prgrm_id and a.pblshr_id = fam2.EPN_PBLSHR_ID and COALESCE(a.ams_tool_id, '(no value)') = COALESCE(fam2.ams_tool_id, '(no value)')
full join MPX_spend_2 b on a.CLICK_dt = b.TRANS_DT and a. ams_prgrm_id = b.ams_prgrm_id and a.pblshr_id = b.AMS_PBLSHR_ID and COALESCE(a.ams_tool_id, '(no value)') = COALESCE(b.ams_tool_id, '(no value)')
and fam2.ck_trans_dt = b.TRANS_DT and fam2. ams_prgrm_id = b.ams_prgrm_id and b.AMS_PBLSHR_ID = fam2.EPN_PBLSHR_ID  and COALESCE(b.ams_tool_id, '(no value)') = COALESCE(fam2.ams_tool_id, '(no value)')

)WITH DATA PRIMARY INDEX(cal_dt
,ams_prgrm_id
,pblshr_id
,ams_tool_id) ON COMMIT PRESERVE ROWS
;

show table TRANS


DELETE FROM p_cac_epn_t.tool_performance_clv2
WHERE cal_dt >= (SEL MIN(cal_dt) FROM dtl_pb_tool)
;

sel top 10 * from  App_mrktng_l2_v.new_bm 

--Drop table p_cac_epn_t.tool_performance_clv2;
--CREATE multiset TABLE p_cac_epn_t.tool_performance_clv2 AS (
INSERT INTO p_cac_epn_t.tool_performance_clv2
SEL 
    b.cal_dt
    ,b.ams_prgrm_id
    ,pg.prgrm_name
    ,b.pblshr_id
    ,pb.PBLSHR_CMPNY_NAME
	,bm.manual_bm as BM
	,bm.muanual_sub_bm as Sub_BM 
    ,b.ams_tool_id
    ,lkp.tool_name
    ,d.ams_tool_categ_name AS tool_categ_name
	,impr_mobile
	,impr_all
    ,click_mobile                  
    ,click_all                     
    ,GMB_24HR_desktop              
    ,GMB_24HR_all                  
    ,fam2_iGMB_desktop             
    ,fam2_iGMB_all                 
    ,fam2_trx_desktop              
    ,fam2_trx_all                  
    ,fam2_norb_desktop          

	
    ,fam2_norb_all                 
    ,GMB_BBOWAC_mobile             
    ,GMB_BBOWAC_all                
    ,iGMB_BBOWAC_mobile            
    ,iGMB_BBOWAC_all               
    ,fam3_trx_mobile               
    ,fam3_trx_all                  
    ,fam3_norb_mobile              
    ,fam3_norb_all     
	,Spend_Mobile
	,Spend_Desktop 
	,Spend_All 
FROM
dtl_pb_tool b
LEFT OUTER JOIN prs_ams_v.AMS_TOOL lkp
ON b.ams_tool_id = lkp.ams_tool_id
LEFT OUTER JOIN prs_ams_v.AMS_TOOL_CATEG d
ON lkp.tool_ctgry_cd = d.ams_tool_categ_cd
LEFT OUTER JOIN  prs_ams_v.ams_pblshr pb
ON b.pblshr_id = pb.ams_pblshr_id
LEFT JOIN prs_ams_v.AMS_PRGRM pg
ON b.AMS_PRGRM_ID = pg.AMS_PRGRM_ID
left join App_mrktng_l2_v.new_bm bm
on b.pblshr_id = bm.ams_pblshr_id

) WITH DATA PRIMARY INDEX ( cal_dt , ams_tool_id ) ;
;


DROP TABLE p_cac_epn_t.tool_performance_summary_clv2;
CREATE MULTISET TABLE p_cac_epn_t.tool_performance_summary_clv2 AS
(SEL
 	a.retail_wk_end_date
 	,a.ams_prgrm_id
 	,a.PRGRM_NAME
    ,AMS_TOOL_ID                   
    ,TOOL_NAME                     
    ,tool_categ_name 
	,a.click_mobile
	,a.click_all
	,a.impr_mobile
	,a.impr_all
	,a.GMB_24HR_desktop
	,a.fam2_iGMB_desktop
	,a.fam2_trx_desktop
	,a.fam2_norb_desktop
	,a.GMB_BBOWAC_mobile
	,a.iGMB_BBOWAC_mobile
	,a.fam3_trx_mobile
	,a.fam3_norb_mobile
	,a.Spend_Mobile
	,a.Spend_Desktop 
	,b.click_mobile                            AS  tot_click_mobile                        
	,b.click_all                                     AS  tot_click_all        
	,b.impr_mobile                            AS  tot_impr_mobile                        
	,b.impr_all                                     AS  tot_impr_all         
	,b.GMB_24HR_desktop           AS  tot_GMB_24HR_desktop      
	,b.fam2_iGMB_desktop            AS tot_fam2_iGMB_desktop        
	,b.fam2_trx_desktop                  AS  tot_fam2_trx_desktop             
	,b.fam2_norb_desktop             AS  tot_fam2_norb_desktop         
	,b.GMB_BBOWAC_mobile     AS  tot_GMB_BBOWAC_mobile
	,b.iGMB_BBOWAC_mobile    AS  tot_iGMB_BBOWAC_mobile
	,b.fam3_trx_mobile                     AS  tot_fam3_trx_mobile                
	,b.fam3_norb_mobile	              AS  tot_fam3_norb_mobile	         
	,b.Spend_Mobile                     AS  tot_Spend_Mobile
	,b.Spend_Desktop                  AS  tot_Spend_Desktop
FROM 
(SEL
 	retail_wk_end_date
    ,ams_prgrm_id                  
    ,PRGRM_NAME                                 
    ,AMS_TOOL_ID                   
    ,TOOL_NAME                     
    ,tool_categ_name               
    ,SUM(impr_mobile) impr_mobile                  
    ,SUM(impr_all) impr_all       
    ,SUM(click_mobile) click_mobile                  
    ,SUM(click_all) click_all          
    ,SUM(GMB_24HR_desktop) GMB_24HR_desktop                              
    ,SUM(fam2_iGMB_desktop) fam2_iGMB_desktop                     
    ,SUM(fam2_trx_desktop) fam2_trx_desktop                         
    ,SUM(fam2_norb_desktop) fam2_norb_desktop                       
    ,SUM(GMB_BBOWAC_mobile) GMB_BBOWAC_mobile                          
    ,SUM(iGMB_BBOWAC_mobile) iGMB_BBOWAC_mobile                    
    ,SUM(fam3_trx_mobile) fam3_trx_mobile                             
    ,SUM(fam3_norb_mobile) fam3_norb_mobile           
	,SUM(Spend_Mobile) Spend_Mobile
	,SUM(Spend_Desktop) Spend_Desktop
 FROM p_cac_epn_t.tool_performance_clv2 a
 INNER JOIN dw_cal_dt b
 	ON a.cal_dt = b.cal_dt  
 GROUP BY 1,2,3,4,5,6
 ) AS a
 INNER JOIN
 (SEL
 	retail_wk_end_date
    ,ams_prgrm_id                  
    ,PRGRM_NAME             
	,SUM(impr_mobile) impr_mobile                  
    ,SUM(impr_all) impr_all       
    ,SUM(click_mobile) click_mobile                  
    ,SUM(click_all) click_all                     
    ,SUM(GMB_24HR_desktop) GMB_24HR_desktop                        
    ,SUM(fam2_iGMB_desktop) fam2_iGMB_desktop                      
    ,SUM(fam2_trx_desktop) fam2_trx_desktop                   
    ,SUM(fam2_norb_desktop) fam2_norb_desktop                   
    ,SUM(GMB_BBOWAC_mobile) GMB_BBOWAC_mobile                        
    ,SUM(iGMB_BBOWAC_mobile) iGMB_BBOWAC_mobile                      
    ,SUM(fam3_trx_mobile) fam3_trx_mobile                          
    ,SUM(fam3_norb_mobile) fam3_norb_mobile         
	,SUM(Spend_Mobile) Spend_Mobile
	,SUM(Spend_Desktop) Spend_Desktop
 FROM p_cac_epn_t.tool_performance_clv2 a
 INNER JOIN dw_cal_dt b
 	ON a.cal_dt = b.cal_dt  
 GROUP BY 1,2,3
) AS b
ON a.retail_wk_end_date = b.retail_wk_end_date
AND a.ams_prgrm_id = b.ams_prgrm_id
) WITH DATA PRIMARY INDEX(retail_wk_end_date, AMS_TOOL_ID)
;





/*    ,a.click_mobile*1.0000/NULLIFZERO(b.click_mobile) AS "Click_Mobile %"
    ,a.click_all*1.0000/NULLIFZERO(b.click_all) AS "Click_All %"
    ,a.GMB_24HR_desktop*1.0000/NULLIFZERO(b.GMB_24HR_desktop) AS "GMB_Desktop %"
    ,a.GMB_BBOWAC_mobile*1.0000/NULLIFZERO(b.GMB_BBOWAC_mobile) AS "GMB_Mobile %"
    ,(COALESCE(a.GMB_24HR_desktop,0)+COALESCE(a.GMB_BBOWAC_mobile,0))
    	/NULLIFZERO((COALESCE(b.GMB_24HR_desktop,0)+COALESCE(b.GMB_BBOWAC_mobile,0))) AS "GMB_All %"
    ,a.fam2_iGMB_desktop*1.0000/NULLIFZERO(b.fam2_iGMB_desktop) AS "iGMB_Desktop %"
    ,a.iGMB_BBOWAC_mobile*1.0000/NULLIFZERO(b.iGMB_BBOWAC_mobile) AS "iGMB_Mobile %"
    ,(COALESCE(a.fam2_iGMB_desktop,0)+COALESCE(a.iGMB_BBOWAC_mobile,0))
    	/NULLIFZERO((COALESCE(b.fam2_iGMB_desktop,0)+COALESCE(b.iGMB_BBOWAC_mobile,0))) AS "iGMB_All %"
    ,a.fam2_trx_desktop*1.0000/NULLIFZERO(b.fam2_trx_desktop) AS "Trans_Desktop %"
    ,a.fam3_trx_mobile*1.0000/NULLIFZERO(b.fam3_trx_mobile) AS "Trans_Mobile %"
    ,(COALESCE(a.fam2_trx_desktop,0)+COALESCE(a.fam3_trx_mobile,0))
    	/NULLIFZERO((COALESCE(b.fam2_trx_desktop,0)+COALESCE(b.fam3_trx_mobile,0))) AS "Trans_All %"   
    ,a.fam2_norb_desktop*1.0000/NULLIFZERO(b.fam2_norb_desktop) AS "NORB_Desktop %"
    ,a.fam3_norb_mobile*1.0000/NULLIFZERO(b.fam3_norb_mobile) AS "NORB_Mobile %"
    ,(COALESCE(a.fam2_norb_desktop,0)+COALESCE(a.fam3_norb_mobile,0))
    	/NULLIFZERO((COALESCE(b.fam2_norb_desktop,0)+COALESCE(b.fam3_norb_mobile,0))) AS "NORB_All %"   */
    	
    	
    	