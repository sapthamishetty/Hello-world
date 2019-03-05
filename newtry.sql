select * from IAD_AMR.Report_a_prob_agg_new_media sample 100;


SHOW VIEW ccra_biz.gg_cf_watson;

select * from CCRA_BIZ_APP.jk_csx_serialized_4 sample 100;

create multiset volatile table temp003 as --this populates the 'Non-Tech Reason', which is the equivalent of the 'Issue_Desc' for TECH cases

(
sel * from ACA.case_question
where 
 question_ID='Q20022' 
  and latest_ind='Y' 
  and ans_id <> 'A1'
 AND CAST(row_added_ts- INTERVAL '7' HOUR AS DATE Format 'mm/dd/yyyy') >= current_date - 2
         AND  ROW_ADDED_DT   >= current_date - 2
 
 ) with data
 primary index ( ans_id)
 on commit preserve rows;
 
 COLLECT STATISTICS
            COLUMN ( CASE_ID ) ,
            COLUMN ( ROW_ADDED_DT ) ,
            COLUMN ( LATEST_IND ) ,
            COLUMN ( QUESTION_ID ) ,
            COLUMN ( ANS_ID )
                ON temp003 ;


CREATE multiset volatile table cqnta as --non tech answers
( SELECT * FROM ACA.ilog_answer ) with data
primary index (ans_id)
on commit preserve rows;

COLLECT STATISTICS COLUMN (ANS_ID) ON cqnta;


CREATE multiset volatile table iss as 
( SELECT * FROM ACA.cx_apc_issue ) with data --Issue table
primary index (issue_cd)
on commit preserve rows;

COLLECT STATISTICS COLUMN (issue_cd) ON iss;


CREATE multiset volatile table com as 
( SELECT * FROM ACA.cx_component ) with data --component table
primary index (comp_cd)
on commit preserve rows;

COLLECT STATISTICS COLUMN (comp_cd) ON com;


CREATE SET VOLATILE TABLE  cx_symptom2 AS ( --volatile table used to produce CAS Symptom level details. This will soon be replaced with data directly from ACA.Fact_Case_Detail
SELECT
DISTINCT(symptom_id),
web_desc,
symptom_type_cd
FROM aca.cx_symptom
WHERE symptom_id <> ''
AND  Row_Added_Ts >= current_date -3
 ) with data
 primary index ( symptom_id)
 on commit preserve rows;
 
 
 
 
DROP TABLE CCRA_BIZ_APP.jk_csx_serialized_3;
SHOW TABLE CCRA_BIZ_APP.jk_csx_serialized_3;

CREATE multiSET TABLE CCRA_BIZ_APP.jk_csx_serialized_3 (
case_id Varchar(20),
serial_nr Varchar(20),
case_opened_hour Varchar(20),
case_opened_date Varchar(20),
issue_cd Varchar(20),
issue_desc Varchar(20),
Issue_reason Varchar(20),
component_cd Varchar(20),
Component_Short_Desc Varchar(20),
case_source Varchar(20),
case_type_cd Varchar(20),
Program Varchar(20),
eligible_omni_group Varchar(20),
Affected_product Varchar(20),
eligible_product_group Varchar(20),
eligible_product Varchar(20), 
country_cd Varchar(20),
Country Varchar(20),
region_cd Varchar(20),
Region Varchar(20),
SW_Version_Zinger Varchar(20),
Sw_Version_Ans_Txt Varchar(20),
Case_Create_Fiscal_Period Varchar(20),
Wom_Extd Varchar(20), --23
Affected_Rollup Varchar(20),
product_group Varchar(20),
device_age1 Varchar(20),
Key_Ownership_Period Varchar(20), 
SSE_SETI Varchar(20),
Resolution_Ansid_Txt Varchar(20), 
resolution_answer Varchar(20),
BaseBand Varchar(20),
activation_dt Varchar(20),  
carrier Varchar(20), --30
issue_detail Varchar(20),
app_symptom Varchar(20),
Dispatch_Indicator Varchar(20),
esc_flag Varchar(20),
Phn_Interaction_Cnt Varchar(20), 
Phn_Interaction_Secs Varchar(20),
ACD_Talk_Ind Varchar(20),
ACD_Talk_Duration_Secs Varchar(20),
cal_week_end_dt Varchar(20),
fiscal_week_desc Varchar(20),
Ios_Version_Order_Nr Varchar(20),
iOS_Version_Tatsu Varchar(20),
IOS_Version_Moved_Off_TS Varchar(20),
IOS_Version_Days_Nr Varchar(20),
Previous_Os Varchar(20),
Previous_iOS_Moved_Off_Dt Varchar(20),
Days_In_Previous_iOS Varchar(20),
iOS_Build_Tatsu Varchar(20),
Build_Order_Nr Varchar(20),
Build_Variant_Cd Varchar(20),
Build_Ts Varchar(20),
Build_Moved_Off_Ts Varchar(20),
OS_Build_Days_Nr Varchar(20),
Previous_Build Varchar(20),
Previous_Build_Moved_Off Varchar(20),
Previous_Build_Variant_Cd Varchar(20),
Days_In_Previous_Build Varchar(20),
SW_TTF Varchar(20),
TTF_Days Varchar(20),
TTF_Weeks Varchar(20),
hw_flag Varchar(20),
Case_Origin_Channel_Txt Varchar(20),
Originating_Channel_Txt Varchar(20),
Symptom_desc Varchar(20),
solution_selected_cd Varchar(20),
top_category Varchar(20),
symptom_category Varchar(20),
symptom_type_cd Varchar(20),
CX_CAS_Symptom Varchar(20),
super_group Varchar(20),
Serialized_Flag Varchar(20),
MinzCases Varchar(20),
Minz Varchar(20),
last_refresh_time Varchar(20) --37
)
NO primary index 
partition by COLUMN (case_opened_date )
--partition by RANGE_N (CAST(case_opened_date AS DATE) BETWEEN DATE '2019-01-31' AND DATE '2019-03-31' EACH INTERVAL '1' DAY);

CREATE PROCEDURE jk_csx_serialized_proc()
BEGIN
INSERT INTO CCRA_BIZ_APP.jk_csx_serialized_5
SELECT
x.case_id,
x.serial_nr,
(x.Case_Row_Added_Ts - Interval '7' HOUR) case_opened_hour,
CAST(x.Case_Row_Added_Ts- INTERVAL '7' HOUR AS DATE) case_opened_date,
iss.issue_cd,
CASE 
    WHEN iss.ISSUE_TBL_SHORT_DESC in ('Frozen - Apple Logo', 'Frozen - Unresponsive') AND eligible_omni_group = 'iOS' THEN 'Power On - Device Unresponsive'
    WHEN iss.ISSUE_TBL_SHORT_DESC in ('Migrate / Restore iTunes Backup', 'Migrate/Restore iTunes Backup', 'Migrate /Restore iTunes Backup', 'Restore iTunes Backup to device') THEN 'Migrate/Restore iTunes Backup'
    WHEN iss.ISSUE_TBL_SHORT_DESC in ('Will Not Power On/Wired Charging', 'Will Not Power On/Charging') THEN 'Will Not Power On/Wired Charging'
    WHEN iss.ISSUE_TBL_SHORT_DESC in ('Third-party Apps (Non Apple Apps)', 'Third-party Apps (user installed)') THEN 'Third-party Apps (Non Apple Apps)'
    WHEN iss.ISSUE_TBL_SHORT_DESC in ('Unexpectedly Restarted') THEN 'Unexpected Restart'
    ELSE iss.ISSUE_TBL_SHORT_DESC
END AS issue_desc,
coalesce(issue_desc, cqnta.ans_txt) Issue_reason,
com.comp_cd AS component_cd,
com.Component_Short_Desc as Component_Short_Desc,
x.case_source_id AS case_source,
x.case_type_cd,
x.Eng_Proj_Cd_IB AS Program,
CASE 
WHEN Program IN ('D20', 'D21', 'D22', 'D32', 'D33', 'N84') THEN 'iOS'
WHEN Program IN ('J320', 'J321', 'J317', 'J318') THEN 'iOS'
WHEN Program IN ('J105A') THEN 'Apple TV'
WHEN Program LIKE ANY ('N111%', 'N121%', 'N131%', 'N141%' ) THEN 'Apple Watch'
ELSE x.Rptd_Pf_Elig_Omni_Grp_Nm
END AS eligible_omni_group,
x.Affected_Prod_Desc AS Affected_product,
x.Rptd_Pf_Elig_Prod_Line_Nm AS eligible_product_group,
x.eng_proj_desc_Pitem AS eligible_product, 
x.case_country_cd as country_cd,
x.case_country_desc AS Country,
x.Case_Region_Cd AS region_cd,
case 
    when x.Case_Country_Cd='USA' then 'USA'
    when x.Case_Country_Cd IN ('CHN','HKG','TWN') then 'China' 
    when x.Case_Country_Cd IN ('JPN') then 'Japan'
    when x.Case_Region_Cd IN ('AMR','ALAC') then 'AMR less US'
    when x.Case_Region_Cd IN ('APAC') then 'APAC less China'
    when x.Case_Region_Cd ='EMEIA' then 'Europe'
ELSE  'Other' END AS Region,
CASE 
    WHEN Program LIKE ANY ('N111%','N121%') AND z.sw_version_txt = '4.X' AND case_opened_date BETWEEN '2017-09-21' AND '2017-09-23' THEN '4.0'
    WHEN Program LIKE ANY ('N111%','N121%') AND z.sw_version_txt = '4.X' AND case_opened_date BETWEEN '2017-10-04' AND '2017-10-09' THEN '4.0.1'
    WHEN Program IN ('D20', 'D21', 'D22') AND z.sw_version_txt = '6.X' AND case_opened_date BETWEEN '2017-09-21' AND '2017-10-05' THEN '11.0'
    WHEN Program IN ('J105A') AND z.sw_version_txt = '11.X'  AND case_opened_date BETWEEN '2017-09-21' AND '2017-09-23' THEN '11.0'
    WHEN  z.sw_version_txt = '11.2.X'  AND case_opened_date BETWEEN '2017-12-12' AND '2017-12-14' THEN '11.2.1'
    WHEN  z.sw_version_txt = '10.1.X'  AND case_opened_date BETWEEN '2016-11-30' AND '2016-12-09' THEN '10.1.1' --data outage correction (ESTIMATE)
    WHEN z.sw_version_txt = '11.X' AND case_opened_date BETWEEN '2017-12-02' AND '2017-12-05' THEN '11.2' 
ELSE z.sw_version_txt 
END AS SW_Version_Zinger,
x.Sw_Version_Ans_Txt,
x.Case_Create_Fiscal_Period,
x.Wom_Extd, --23
CASE 
    WHEN UPPER(Affected_product) = 'MACOS' THEN 'macOS' 
    WHEN UPPER(Affected_product) LIKE ('%WATCH%') THEN 'Apple Watch'
    WHEN UPPER(Affected_product) LIKE ANY ('%IPHONE%','%IPAD%', '%IPOD TOUCH%') THEN 'iOS Device'
    WHEN UPPER(Affected_product) LIKE ('%APPLE TV%') THEN 'Apple TV'
    WHEN Affected_product = ('Apple ID') THEN 'Apple ID'
    WHEN Affected_product = ('ICLOUD') THEN 'iCloud'
    WHEN Affected_product = ('PHOTOS') THEN 'Photos'
    WHEN Affected_product = ('ITUNES STORE') THEN 'iTunes Store'
ELSE 'Other' END Affected_Rollup,
CASE 
    WHEN UPPER(Affected_product) = 'ICLOUD' THEN 'ICLOUD'
    WHEN UPPER(Affected_product) = 'ITUNES STORE' THEN 'ITUNES STORE'
    WHEN UPPER(Affected_product) = 'APPLE ID' THEN 'APPLE ID'
    WHEN eligible_omni_group = 'iOS' THEN 'iOS'
    WHEN eligible_omni_group = 'Mac' THEN 'Mac'
    WHEN eligible_omni_group = 'Apple TV' THEN 'Apple TV'
    WHEN eligible_omni_group = 'Apple Watch' THEN 'Apple Watch'
END AS product_group,
(case_opened_date - pf.first_day_dt) AS device_age1,
CASE
    WHEN pf.first_day_dt IS NULL THEN 'unknown'
    WHEN device_age1 <7 THEN '1st year 1st qtr 1st wk'
    WHEN device_age1 <28 THEN '1st year 1st qtr 2nd-4th wk'
    WHEN device_age1 <(7*8) THEN '1st year 1st qtr 5th-8th wk'
    WHEN device_age1 <(7*13)+1 THEN '1st year 1st qtr 9th-13th wk'
    WHEN device_age1 <(7*26)+2 THEN '1st year 2nd qtr'
    WHEN device_age1 <(7*39)+2 THEN '1st year 3rd qtr'
    WHEN device_age1 <(7*52)+3 THEN '1st year 4th qtr'
    WHEN device_age1 <(7*52)+368 THEN '2nd year'
    WHEN device_age1 <(7*52)+733 THEN '3rd year'
    WHEN device_age1 <(7*52)+1095 THEN '4th year'
    WHEN device_age1 <(7*52)+1460 THEN '5th year'
ELSE 'over 5-years'
END as Key_Ownership_Period, 
SONb.SSE_SETI AS SSE_SETI,
x.Resolution_Ansid_Txt, 
Resb.resolution_answer,
ACT.baseband AS BaseBand,
act.activation_dt,  
act.carrier, --30
IDb.issue_detail,
symb.app_symptom,
case when x.dispatch_ID is not null then 1 else 0 end as Dispatch_Indicator,
case when x.Case_Escalation_Ind = 'Y' then 1 else 0 end as esc_flag,
x.Phn_Interaction_Cnt, 
x.Phn_Interaction_Secs,
x.ACD_Talk_Ind,
x.ACD_Talk_Duration_Secs,
dt.cal_week_end_dt,
dt.fiscal_week_desc,
x.Ios_Version_Order_Nr
  ,x.Ios_Version_Cd AS iOS_Version_Tatsu
  ,x.IOS_Version_Moved_Off_TS 
  ,x.IOS_Version_Days_Nr
  ,x.Previous_Os
  ,x.Previous_iOS_Moved_Off_Dt
  ,x.Days_In_Previous_iOS
  ,x.Os_Build_Cd AS iOS_Build_Tatsu
  ,x.Build_Order_Nr
  ,x.Build_Variant_Cd
  ,x.Build_Ts 
  ,x.Build_Moved_Off_Ts
  ,x.OS_Build_Days_Nr
  ,x.Previous_Build
  ,x.Previous_Build_Moved_Off
  ,x.Previous_Build_Variant_Cd
  ,x.Days_In_Previous_Build
  ,x.Days_Btwn_BldUpd_CaseOpen AS SW_TTF,
CASE 
  WHEN (case_opened_date - activation_dt) <0 THEN 0 
  ELSE (case_opened_date - activation_dt) 
END AS TTF_Days,
TTF_Days/7 AS TTF_Weeks,
pf.hw_flag,
x.Case_Origin_Channel_Txt,
x.Originating_Channel_Txt,
x.Symptom_desc,
x.solution_selected_cd,
sct.web_desc as top_category,
sct.symptom_cat_desc as symptom_category,
s.symptom_type_cd,
s.web_desc as CX_CAS_Symptom,
sg.super_grp_name AS super_group,
CASE 
    WHEN x.serial_nr <>'' THEN 'Serialized'
    ELSE 'Non-Serialized'
END AS Serialized_Flag,
mz.case_ID AS MinzCases,
mz.duration AS Minz,
((current_timestamp)- interval '7' hour ) as last_refresh_time --37
FROM ACA.FACT_CASE_DETAIL x
--LEFT JOIN ARTEMIS_BIZ_APP.ACA_ACTIVATION ACT ON x.Serial_Nr = ACT.serial_nr AND case_opened_date  >= current_date - 2 --joining to IB table for additional details.
LEFT JOIN AC_Biz_App.ACA_ACTIVATION ACT ON x.Serial_Nr = ACT.serial_nr AND case_opened_date  >= current_date - 2 --joining to IB table for additional details
LEFT JOIN  CCRA_BIz_App.jk_serial_configCd CC ON x.serial_nr = CC.serial_nr AND program IN ('D10', 'D11', 'D20','D21', 'D22', 'D32', 'D33', 'N84') --this is not deprecated but still exists in the table
LEFT JOIN ACA.CX_IPHONE_ZINGER_DETAIL z ON x.case_id = z.case_id --no dups
LEFT JOIN (SELECT case_ID as cqnt_Case_ID,
                max(cqnt.ans_id) as ans_id
                   FROM temp003 cqnt 
                   where  cqnt.ans_id <> 'A1'
                   AND  cqnt.row_added_dt  >= current_date - 2
           GROUP BY 1
                     )cqntb 
                    on x.case_id = cqntb.cqnt_Case_ID 
LEFT JOIN cqnta on cqntb.ans_id = cqnta.ans_id --Join to Non-Tech answers
LEFT JOIN com on (x.component_cd = com.comp_cd) --join to component info    
LEFT JOIN iss on (x.issue_cd=iss.issue_cd) --join to issue info
LEFT JOIN (SELECT case_IDx as ID_Case_ID, --Join for Issue Detail Trigger at caseID level
               max(ID.issue_detail) as issue_detail
                   FROM CCRA_BIZ_APP.jk_issue_detail_csx ID 
                   where  ID.ans_id <> 'A1'
                   AND  ID.row_added_dt  >= current_date - 2
           GROUP BY 1
                     )IDb 
                    on x.case_id = IDb.ID_Case_ID 
left join ACA.CX_SUPER_GROUP sg on sg.super_grp_id=x.super_grp_id  --Join for CAS specific info
left join aca.CX_SYMPTOM_CATEGORY sct on x.symptom_categ_id = sct.symptom_categ_id --Join for CAS specific info
left join cx_symptom2 s on x.Symptom_ID = s.Symptom_ID --Join for CAS specific info
LEFT JOIN ACA.ppi_drivers pf ON x.case_id = pf.case_id --this join is to the 'PPI Data' and lets us pull in some additional details
    AND pf.case_added_dt  >= current_date - 2
LEFT JOIN (SELECT case_IDx as sym_Case_ID, --This join pulls in the symptom details at the case_Id level
               max(sym.app_symptom) as app_symptom
                   FROM CCRA_BIZ_APP.jk_app_symptom sym 
                   WHERE  sym.row_added_dt  >= current_date - 2
           GROUP BY 1
                     )symb 
                    on x.case_id = symb.sym_Case_ID 
LEFT JOIN (SELECT case_ID as res_Case_ID,
               max(res.resolution_answer) as resolution_answer
                   FROM CCRA_BIz.jk_resolution_answer res 
                   WHERE  res.row_added_dt  >= current_date - 2
           GROUP BY 1
                     )resb 
                    on x.case_id = resb.res_Case_ID 
LEFT JOIN (SELECT iLog_case_ID as SON_Case_ID, --This join pulls in the SETI details at the case_Id level
               max(SON.SSE_Radar || ' - ' || SON.clean_SubIssue) as SSE_SETI
                   FROM CCRA_BIz_app.jk_allIssues_sonar SON 
                   WHERE  SON.Rptg_Dt  >= current_date - 2
           GROUP BY 1
                     ) SONb 
                    on x.case_id = SONb.Son_Case_ID 
LEFT JOIN ( --This join pulls in the # of minutes associated with a case_id
select  case_id, 
sum(total_case_phone_aht_secs+total_case_chat_aht_secs) phone,
sum(acw_secs) acw,
phone-acw duration
FROM aca.case_interaction_metric_detail
WHERE rptg_dt  >= current_date - 2
GROUP BY 1)
 mz
    ON x.case_id = mz.case_id
JOIN ccra_biz.rbm_dates dt on case_opened_date=dt.cal_dt
WHERE case_opened_date   >= current_date - 2
    --AND x.serial_nr <>''  
    AND x.serial_nr is null
    AND issue_reason IS NOT NULL
    AND x.case_type_cd IN ('TECH', 'RETL','DENY', 'INFO', 'ADMIN', 'CUST', 'SALE')  --These case types are the only ones we generally ever care about as these are the 'complete' cases
END;  

SELECT COUNT(*) FROM CCRA_BIZ_APP.jk_csx_serialized_3;


select User
from dbc.AllRights
WHERE DatabaseName = CCRA_BIZ_APP.jk AND TableName = jk_csx_serialized_3
AND AccessRight = 'PE'

HELP STATS CCRA_BIZ_APP.jk_csx_serialized_3 COLUMN case_ID;

SHOW PARTITIONS FROM CCRA_BIZ_APP.jk_csx_serialized_3;
SELECT  DBC.ColumnsV.PartitioningColumn = 'Y'

DROP TABLE CCRA_BIZ_APP.jk_csx_serialized_5;
SHOW TABLE CCRA_BIZ_APP.jk_csx_serialized_35



CREATE multiSET TABLE CCRA_BIZ_APP.jk_csx_serialized_5 (
case_id varchar(20),
serial_nr varchar(20),
--case_opened_hour date,
--case_opened_date date,
issue_cd varchar(20),
issue_desc varchar(50),
Issue_reason varchar(50),
component_cd varchar(20),
Component_Short_Desc varchar(50),
case_source varchar(10),
case_type_cd varchar(10),
Program varchar(10)
)

INSERT INTO CCRA_BIZ_APP.jk_csx_serialized_5
SELECT
x.case_id,
x.serial_nr,
--(x.Case_Row_Added_Ts - Interval '7' HOUR) case_opened_hour,
--CAST(x.Case_Row_Added_Ts- INTERVAL '7' HOUR AS DATE) case_opened_date,
iss.issue_cd,
CASE 
    WHEN iss.ISSUE_TBL_SHORT_DESC in ('Frozen - Apple Logo', 'Frozen - Unresponsive') AND eligible_omni_group = 'iOS' THEN 'Power On - Device Unresponsive'
    WHEN iss.ISSUE_TBL_SHORT_DESC in ('Migrate / Restore iTunes Backup', 'Migrate/Restore iTunes Backup', 'Migrate /Restore iTunes Backup', 'Restore iTunes Backup to device') THEN 'Migrate/Restore iTunes Backup'
    WHEN iss.ISSUE_TBL_SHORT_DESC in ('Will Not Power On/Wired Charging', 'Will Not Power On/Charging') THEN 'Will Not Power On/Wired Charging'
    WHEN iss.ISSUE_TBL_SHORT_DESC in ('Third-party Apps (Non Apple Apps)', 'Third-party Apps (user installed)') THEN 'Third-party Apps (Non Apple Apps)'
    WHEN iss.ISSUE_TBL_SHORT_DESC in ('Unexpectedly Restarted') THEN 'Unexpected Restart'
    ELSE iss.ISSUE_TBL_SHORT_DESC
END AS issue_desc,
coalesce(issue_desc, cqnta.ans_txt) Issue_reason,
com.comp_cd AS component_cd,
com.Component_Short_Desc as Component_Short_Desc,
x.case_source_id AS case_source,
x.case_type_cd,
x.Eng_Proj_Cd_IB AS Program
FROM ACA.FACT_CASE_DETAIL x
--LEFT JOIN ARTEMIS_BIZ_APP.ACA_ACTIVATION ACT ON x.Serial_Nr = ACT.serial_nr AND case_opened_date  >= current_date - 2 --joining to IB table for additional details.
LEFT JOIN AC_Biz_App.ACA_ACTIVATION ACT ON x.Serial_Nr = ACT.serial_nr AND case_opened_date  >= current_date - 2 --joining to IB table for additional details
LEFT JOIN  CCRA_BIz_App.jk_serial_configCd CC ON x.serial_nr = CC.serial_nr AND program IN ('D10', 'D11', 'D20','D21', 'D22', 'D32', 'D33', 'N84') --this is not deprecated but still exists in the table
LEFT JOIN ACA.CX_IPHONE_ZINGER_DETAIL z ON x.case_id = z.case_id --no dups
LEFT JOIN (SELECT case_ID as cqnt_Case_ID,
                max(cqnt.ans_id) as ans_id
                   FROM temp003 cqnt 
                   where  cqnt.ans_id <> 'A1'
                   AND  cqnt.row_added_dt  >= current_date - 2
           GROUP BY 1
                     )cqntb 
                    on x.case_id = cqntb.cqnt_Case_ID 
LEFT JOIN cqnta on cqntb.ans_id = cqnta.ans_id --Join to Non-Tech answers
LEFT JOIN com on (x.component_cd = com.comp_cd) --join to component info    
LEFT JOIN iss on (x.issue_cd=iss.issue_cd) --join to issue info
LEFT JOIN (SELECT case_IDx as ID_Case_ID, --Join for Issue Detail Trigger at caseID level
               max(ID.issue_detail) as issue_detail
                   FROM CCRA_BIZ_APP.jk_issue_detail_csx ID 
                   where  ID.ans_id <> 'A1'
                   AND  ID.row_added_dt  >= current_date - 2
           GROUP BY 1
                     )IDb 
                    on x.case_id = IDb.ID_Case_ID 
left join ACA.CX_SUPER_GROUP sg on sg.super_grp_id=x.super_grp_id  --Join for CAS specific info
left join aca.CX_SYMPTOM_CATEGORY sct on x.symptom_categ_id = sct.symptom_categ_id --Join for CAS specific info
left join cx_symptom2 s on x.Symptom_ID = s.Symptom_ID --Join for CAS specific info
LEFT JOIN ACA.ppi_drivers pf ON x.case_id = pf.case_id --this join is to the 'PPI Data' and lets us pull in some additional details
    AND pf.case_added_dt  >= current_date - 2
LEFT JOIN (SELECT case_IDx as sym_Case_ID, --This join pulls in the symptom details at the case_Id level
               max(sym.app_symptom) as app_symptom
                   FROM CCRA_BIZ_APP.jk_app_symptom sym 
                   WHERE  sym.row_added_dt  >= current_date - 2
           GROUP BY 1
                     )symb 
                    on x.case_id = symb.sym_Case_ID 
LEFT JOIN (SELECT case_ID as res_Case_ID,
               max(res.resolution_answer) as resolution_answer
                   FROM CCRA_BIz.jk_resolution_answer res 
                   WHERE  res.row_added_dt  >= current_date - 2
           GROUP BY 1
                     )resb 
                    on x.case_id = resb.res_Case_ID 
LEFT JOIN (SELECT iLog_case_ID as SON_Case_ID, --This join pulls in the SETI details at the case_Id level
               max(SON.SSE_Radar || ' - ' || SON.clean_SubIssue) as SSE_SETI
                   FROM CCRA_BIz_app.jk_allIssues_sonar SON 
                   WHERE  SON.Rptg_Dt  >= current_date - 2
           GROUP BY 1
                     ) SONb 
                    on x.case_id = SONb.Son_Case_ID 
LEFT JOIN ( --This join pulls in the # of minutes associated with a case_id
select  case_id, 
sum(total_case_phone_aht_secs+total_case_chat_aht_secs) phone,
sum(acw_secs) acw,
phone-acw duration
FROM aca.case_interaction_metric_detail
WHERE rptg_dt  >= current_date - 2
GROUP BY 1)
 mz
    ON x.case_id = mz.case_id
JOIN ccra_biz.rbm_dates dt on case_opened_date=dt.cal_dt
WHERE case_opened_date   >= current_date - 2
    --AND x.serial_nr <>''  
    AND x.serial_nr is null
    AND issue_reason IS NOT NULL
    AND x.case_type_cd IN ('TECH', 'RETL','DENY', 'INFO', 'ADMIN', 'CUST', 'SALE')  --These case types are the only ones we generally ever care about as these are the 'complete' cases
  
  
  l