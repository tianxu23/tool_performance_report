import decimal
import pyodbc
import time
import csv
import datetime
import os
import sys
import subprocess
import numpy
import email
import smtplib
import shutil

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
from datetime import datetime as dt


os.chdir('C:/Users/tianxu/Documents/Tool_Performance')
pyodbc.pooling = False


#def main():
Login_info = open('C:/Work/LogInMozart_ts.txt', 'r')
server_name = Login_info.readline()
server_name = server_name[:server_name.index(';')+1]
UID = Login_info.readline()
UID = UID[:UID.index(';') + 1]
PWD = Login_info.readline()
PWD = PWD[:PWD.index(';') + 1]
Login_info.close()
#today_dt = datetime.date.today()
print 'Connecting Server to determine date info at: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '.'
conn = pyodbc.connect('DRIVER={Teradata};DBCNAME='+ server_name +'UID=' + UID + 'PWD=' + PWD)
curs = conn.cursor()


curs.execute('''

CREATE MULTISET VOLATILE TABLE impression AS
(SELECT
    COALESCE(ams_prgrm_id, -999) AS ams_prgrm_id
    ,IMPRSN_DT
    ,COALESCE(pblshr_id, -999) AS pblshr_id
    ,ams_tool_id
    ,SUM(CASE WHEN trfc_src_cd IN (1, 2, 3) THEN 1 ELSE 0 END) AS impr_mobile
    ,COUNT(IMPRSN_CNTNR_ID) AS impr_all
FROM prs_ams_v.AMS_IMPRSN_CNTNR
WHERE IMPRSN_DT between current_date -10 and   current_date - 2 
    AND ams_trans_rsn_cd = 0
GROUP BY 1, 2, 3, 4
HAVING impr_all > 0
) WITH DATA PRIMARY INDEX (IMPRSN_DT, pblshr_id, ams_prgrm_id)
ON COMMIT PRESERVE ROWS;
   
''')
conn.commit()

print 'impression table is ok now!'

curs.execute('''
CREATE MULTISET VOLATILE TABLE click AS
(SELECT
    COALESCE(ams_prgrm_id, -999) AS ams_prgrm_id
    ,click_dt
    ,COALESCE(pblshr_id, -999) AS pblshr_id
    ,ams_tool_id
    ,SUM(CASE WHEN trfc_src_cd IN (1, 2, 3) THEN 1 ELSE 0 END) AS click_mobile
    ,COUNT(1) AS click_all
FROM prs_ams_v.ams_click
WHERE click_dt  between current_date-10 and   current_date - 2 
    AND ams_trans_rsn_cd = 0
GROUP BY 1, 2, 3, 4
HAVING click_all > 0
) WITH DATA PRIMARY INDEX (CLICK_DT, pblshr_id, ams_prgrm_id)
ON COMMIT PRESERVE ROWS;
 
''')
conn.commit()
print 'click table is ok now'



curs.execute('''
   
COLLECT STATISTICS COLUMN (AMS_PRGRM_ID ,CLICK_DT ,PBLSHR_ID,AMS_TOOL_ID)   ON click;   
   
''')
conn.commit()

curs.execute('''
COLLECT STATISTICS COLUMN (AMS_PRGRM_ID ,IMPRSN_DT ,PBLSHR_ID,AMS_TOOL_ID)   ON impression;   
   
   
''')
conn.commit()
curs.execute('''
CREATE  MULTISET volatile TABLE impr_click as (
sel COALESCE(b.IMPRSN_DT,a.CLICK_dt) AS cal_dt
,COALESCE(b.ams_prgrm_id,a.ams_prgrm_id) AS ams_prgrm_id
,COALESCE(b.pblshr_id,a.pblshr_id) AS pblshr_id
,COALESCE(b.AMS_TOOL_ID,A.AMS_TOOL_ID) AS AMS_TOOL_ID
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
   
   
''')
conn.commit()
print "Be patient, you know, i'm working hard now, impr_click is ok"




curs.execute('''
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
AND CLICK_dt   between current_date - 375 and   current_date - 2 
) WITH DATA PRIMARY INDEX (CLICK_dt, pblshr_id, ams_prgrm_id,ams_tool_id)
ON COMMIT PRESERVE ROWS;        
   
''')
             
conn.commit()
curs.execute('''
CREATE MULTISET VOLATILE TABLE TRANS  AS
(SELECT
    fam.CK_TRANS_DT AS ck_trans_dt
    ,fam.ams_prgrm_id 
  ,fam.EPN_PBLSHR_ID
  ,c.ams_tool_id
   -- ,COALESCE(c.ams_tool_id, '(no value)') as ams_tool_id
    ,SUM(CASE WHEN  clv_dup_ind =0 and DEVICE_TYPE_ID IN (1)  THEN coalesce(GMB_PLAN_RATE_AMT,0) ELSE 0 END) AS GMB_24HR_desktop -- GMB
    ,SUM(CASE WHEN  clv_dup_ind =0 and DEVICE_TYPE_ID IN (2,3)  THEN coalesce(GMB_PLAN_RATE_AMT,0) ELSE 0 END) AS GMB_BBOWAC_mobile -- GMB
    ,SUM(CASE WHEN  clv_dup_ind =0 and DEVICE_TYPE_ID NOT IN (1,2,3)  THEN coalesce(GMB_PLAN_RATE_AMT,0) ELSE 0 END) AS GMB_other_device -- new_added
  ,SUM(CASE WHEN DEVICE_TYPE_ID IN (1)  THEN coalesce(fam.IGMB_PLAN_RATE_AMT,0)ELSE 0 END) AS fam2_IGMB_desktop
    ,SUM(CASE WHEN DEVICE_TYPE_ID IN (2,3)  THEN coalesce(fam.IGMB_PLAN_RATE_AMT,0)ELSE 0 END) AS iGMB_BBOWAC_mobile
  ,SUM(CASE WHEN DEVICE_TYPE_ID NOT IN (1,2,3)  THEN coalesce(fam.IGMB_PLAN_RATE_AMT,0)ELSE 0 END) AS iGMB_other_device -- new_added
    ,count(distinct case when DEVICE_TYPE_ID IN (1)  THEN CK_TRANS_ID||ITEM_ID end) AS fam2_trx_desktop
  ,count(distinct case when DEVICE_TYPE_ID IN (2,3)  THEN CK_TRANS_ID||ITEM_ID end) AS fam3_trx_mobile
  ,count(distinct case when DEVICE_TYPE_ID NOT IN (1,2,3)  THEN CK_TRANS_ID||ITEM_ID end) AS fam3_trx_other_device -- new_added
    ,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) and DEVICE_TYPE_ID IN (1) THEN 1 ELSE 0 END) AS fam2_norb_desktop
  ,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) and DEVICE_TYPE_ID IN (2,3) THEN 1 ELSE 0 END) AS fam3_norb_mobile
  ,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) and DEVICE_TYPE_ID NOT IN (1,2,3) THEN 1 ELSE 0 END) AS fam3_norb_other_device --new_added
    --,SUM(CASE WHEN  fam.CLV_BUYER_TYPE_CD IN (1,2) THEN  INCR_FCTR ELSE 0 END ) AS INORB
    FROM  PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT AS fam
LEFT OUTER JOIN click_table AS c
  ON fam.RVR_ID = c.click_id 
WHERE fam.MPX_CHNL_ID = 6
AND fam.CK_TRANS_DT  between  current_date-10 and   current_date - 2 
--AND fam.client_id = fam.client_id_global                             ---- added to sync with FAM3, excluding GBH/Geox from reporting.
AND fam.EPN_PBLSHR_ID <> -999
--and ams_tool_id =11006
GROUP BY 1,2,3,4

) WITH DATA PRIMARY INDEX (ck_trans_dt, ams_prgrm_id,ams_tool_id)
ON COMMIT PRESERVE ROWS;   
   
   
''')
conn.commit()
print "TRANS is ok"             

curs.execute('''
CREATE  MULTISET volatile TABLE MPX_spend_2 as (
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
TRANS_DT between  current_date -10 and   current_date - 2 
group by 1,2,3,4
) WITH DATA PRIMARY INDEX(TRANS_DT
,AMS_PRGRM_ID
,ams_tool_id,AMS_PBLSHR_ID) on commit preserve rows;   
   
   
''')
conn.commit()

print "mpx_spend_2!"  
             
curs.execute('''
   
COLLECT STATISTICS COLUMN (AMS_PRGRM_ID ,CLICK_DT ,PBLSHR_ID,AMS_TOOL_ID)  ON click;
   
''')

conn.commit()
curs.execute('''
   
COLLECT STATISTICS COLUMN (AMS_PRGRM_ID ,IMPRSN_DT ,PBLSHR_ID,AMS_TOOL_ID)   ON impression;
        
   
''')
conn.commit()             
curs.execute('''
    
COLLECT STATISTICS COLUMN (CK_TRANS_DT ,AMS_PRGRM_ID  ,EPN_PBLSHR_ID) ON TRANS;    
   
''')
conn.commit()
curs.execute('''
   
COLLECT STATISTICS COLUMN (AMS_TOOL_ID,AMS_PRGRM_ID,AMS_PBLSHR_ID) ON MPX_spend_2;    
   
   
''')
conn.commit()   
curs.execute('''
   

CREATE MULTISET volatile TABLE dtl_pb_tool as (
sel 
 COALESCE(a.cal_dt,fam2.ck_trans_dt,b.TRANS_DT) as cal_dt
,COALESCE(a.ams_prgrm_id, fam2. ams_prgrm_id, b.ams_prgrm_id) as ams_prgrm_id
,COALESCE(a.pblshr_id,fam2.EPN_PBLSHR_ID,b.AMS_PBLSHR_ID) as pblshr_id
,COALESCE(a.AMS_TOOL_ID,fam2.ams_tool_id,b.ams_tool_id) as AMS_TOOL_ID
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

,COALESCE(fam2.GMB_other_device, 0.00) AS GMB_other_device -- new_added
,COALESCE(fam2.iGMB_other_device, 0.00) AS iGMB_other_device -- new_added
,COALESCE(fam2.fam3_trx_other_device, 0) AS fam3_trx_other_device -- new_added
,COALESCE(fam2.fam3_norb_other_device, 0) AS fam3_norb_other_device -- new_added

,COALESCE(b.Spend_Mobile, 0) AS Spend_Mobile
,COALESCE(b.Spend_Desktop, 0) AS Spend_Desktop
,COALESCE(b.Spend, 0) AS Spend_All
From impr_click a
full join TRANS fam2 on fam2.ck_trans_dt = a.cal_dt and fam2. ams_prgrm_id = a.ams_prgrm_id and a.pblshr_id = fam2.EPN_PBLSHR_ID and COALESCE(a.ams_tool_id, '(no value)') = COALESCE(fam2.ams_tool_id, '(no value)')
full join MPX_spend_2 b on a.cal_dt = b.TRANS_DT and a. ams_prgrm_id = b.ams_prgrm_id and a.pblshr_id = b.AMS_PBLSHR_ID and COALESCE(a.ams_tool_id, '(no value)') = COALESCE(b.ams_tool_id, '(no value)')
and fam2.ck_trans_dt = b.TRANS_DT and fam2. ams_prgrm_id = b.ams_prgrm_id and b.AMS_PBLSHR_ID = fam2.EPN_PBLSHR_ID  and COALESCE(b.ams_tool_id, '(no value)') = COALESCE(fam2.ams_tool_id, '(no value)')
--and cal_dt is not null
)WITH DATA PRIMARY INDEX(cal_dt
,ams_prgrm_id
,pblshr_id
,ams_tool_id) ON COMMIT PRESERVE ROWS;   
   
''')
conn.commit()
print "Start to delete"              

curs.execute('''
delete from   p_tiansheng_t.tool_performance_clv2
where cal_DT between current_date-10 and current_date-2;   

   
''')
conn.commit()             
             
print "ok,delete! let's start to insert." 
             
curs.execute('''
insert into  p_tiansheng_t.tool_performance_clv2
 SEL 
    b.cal_dt
    ,b.ams_prgrm_id
    ,pg.prgrm_name
    ,b.pblshr_id
    ,pb.PBLSHR_CMPNY_NAME
  ,bm.manual_bm as BM
  ,bm.manual_sub_bm as Sub_BM 
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
  
  ,GMB_other_device        --new_added                    
    ,iGMB_other_device         --new_added   
    ,fam3_trx_other_device      --new_added         
    ,fam3_norb_other_device        -- new_added
  
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
on b.pblshr_id = bm.ams_pblshr_id;
   
''')
conn.commit()
             
print "successfully insert"
execfile('EmailSender_Tool.py')
print 'Send eMail'

conn.close()
