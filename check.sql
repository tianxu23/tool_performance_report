DROP TABLE CLICK_TABLE ;
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
AND CLICK_dt   between '2017-04-26'  and  '2018-05-27'
) WITH DATA PRIMARY INDEX (CLICK_dt, pblshr_id, ams_prgrm_id,ams_tool_id)
ON COMMIT PRESERVE ROWS;

--DROP TABLE TRANS;
CREATE MULTISET VOLATILE TABLE TRANS  AS
(SELECT
    fam.CK_TRANS_DT AS ck_trans_dt
    ,fam.ams_prgrm_id 
    --,fam.EPN_PBLSHR_ID
    ,c.ams_tool_id
   -- ,COALESCE(c.ams_tool_id, '(no value)') as ams_tool_id
    ,SUM(CASE WHEN  clv_dup_ind =0 and DEVICE_TYPE_ID IN (1)  THEN coalesce(GMB_PLAN_RATE_AMT,0) ELSE 0 END) AS GMB_24HR_desktop -- GMB
    ,SUM(CASE WHEN  clv_dup_ind =0 and DEVICE_TYPE_ID IN (2,3)  THEN coalesce(GMB_PLAN_RATE_AMT,0) ELSE 0 END) AS GMB_BBOWAC_mobile -- GMB
        ,SUM(CASE WHEN  clv_dup_ind =0 THEN coalesce(GMB_PLAN_RATE_AMT,0) ELSE 0 END) AS GMB -- GMB
    ,SUM(CASE WHEN DEVICE_TYPE_ID IN (1)  THEN coalesce(fam.IGMB_PLAN_RATE_AMT,0)ELSE 0 END) AS fam2_IGMB_desktop
    ,SUM(CASE WHEN DEVICE_TYPE_ID IN (2,3)  THEN coalesce(fam.IGMB_PLAN_RATE_AMT,0)ELSE 0 END) AS iGMB_BBOWAC_mobile
       ,SUM(coalesce(fam.IGMB_PLAN_RATE_AMT,0)) AS iGMB
    ,count(distinct case when DEVICE_TYPE_ID IN (1)  THEN CK_TRANS_ID||ITEM_ID end) AS fam2_trx_desktop
    ,count(distinct case when DEVICE_TYPE_ID IN (2,3)  THEN CK_TRANS_ID||ITEM_ID end) AS fam3_trx_mobile
        ,count(distinct  CK_TRANS_ID||ITEM_ID) AS trx
    ,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) and DEVICE_TYPE_ID IN (1) THEN 1 ELSE 0 END) AS fam2_norb_desktop
       ,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) and DEVICE_TYPE_ID IN (2,3) THEN 1 ELSE 0 END) AS fam3_norb_mobile
           ,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) THEN 1 ELSE 0 END) AS norb
    --,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) THEN  INCR_FCTR ELSE 0 END ) AS INORB
    FROM  PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT AS fam
LEFT OUTER JOIN click_table AS c
    ON fam.RVR_ID = c.click_id 
WHERE fam.MPX_CHNL_ID = 6
AND fam.CK_TRANS_DT   between '2018-04-26'  and  '2018-05-27'
--AND fam.client_id = fam.client_id_global                             ---- added to sync with FAM3, excluding GBH/Geox from reporting.
AND fam.EPN_PBLSHR_ID <> -999
and ams_tool_id =11006
GROUP BY 1,2,3

) WITH DATA PRIMARY INDEX (ck_trans_dt, ams_prgrm_id,ams_tool_id)
ON COMMIT PRESERVE ROWS;

sel * from trans

sel * from  p_tiansheng_t.tool_performance_clv2 where cal_dt between '2018-04-10' and '2018-05-27' and ams_tool_id = 11006

sel * from P_ePNPEM_T.mbai_pub_daily_pfm_0306 where trans_dt> current_date -3
