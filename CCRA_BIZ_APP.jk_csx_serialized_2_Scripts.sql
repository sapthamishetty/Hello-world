
DELETE FROM CCRA_BIZ_APP.jk_issue_detail_csx  --this table populates the "Issue Detail" Field. It's a group of questionIDs/Triggers in iLog
WHERE row_added_dt >= current_date - 7;

INSERT INTO CCRA_BIZ_APP.jk_issue_detail_csx

select
cq.case_ID case_IDx,
cq.Question_ID, 
cq.row_added_dt,
max(substring(ia.Ans_Txt  from 1 for 75)) issue_detail, 
max(cq.Ans_ID) Ans_ID
from aca.cx_case_question cq
LEFT JOIN ACA.cx_ilog_answer ia on cq.ans_id = ia.ans_id
LEFT JOIN ACA.cx_ilog_question iq on cq.question_id=iq.question_id

where cq.question_ID in ('Q65045', 'Q20277', 'Q20194', 'Q63395', 'Q64579', 'Q66067', 'Q66018','Q100405', 'Q100851', 'Q201893', 'Q66616', 'Q209531') 
AND cq.row_added_dt>=current_date-7
AND cq.Latest_Ind='Y'
GROUP BY 1,2,3; 


 Collect statistics column(case_IDx) on  CCRA_BIZ_APP.jk_issue_detail_csx;
 Collect statistics column(Question_Id) on CCRA_BIZ_APP.jk_issue_detail_csx;

DELETE FROM CCRA_BIZ_APP.jk_battery_health_trig --this populates case_id level details on one specific trigger question that I need to use often
WHERE row_added_dt >= current_date - 3;

INSERT INTO CCRA_BIZ_APP.jk_battery_health_trig

select
cq.case_ID case_IDx,
cq.Question_ID, 
cq.row_added_dt,
max(substring(ia.Ans_Txt  from 1 for 75)) battery_health_answer, 
max(cq.Ans_ID) Ans_ID
from aca.cx_case_question cq
LEFT JOIN ACA.cx_ilog_answer ia on cq.ans_id = ia.ans_id
LEFT JOIN ACA.cx_ilog_question iq on cq.question_id=iq.question_id

where cq.question_ID in ('Q204984', 'Q205060') 
AND cq.row_added_dt>=current_date-3
AND cq.Latest_Ind='Y'
GROUP BY 1,2,3;


 Collect statistics column(case_IDx) on  CCRA_BIZ_APP.jk_battery_health_trig;
 Collect statistics column(Question_Id) on CCRA_BIZ_APP.jk_battery_health_trig;



DELETE FROM CCRA_BIZ_APP.jk_app_symptom --one more trigger aggregation. I refer to this one as the 'Symptom' trigger, as it's typically phrased that way
WHERE row_added_dt >= current_date - 7;

INSERT INTO CCRA_BIZ_APP.jk_app_symptom

select
cq.case_ID case_IDx,
cq.Question_ID, 
cq.row_added_dt,
max(substring(ia.Ans_Txt  from 1 for 75))  app_symptom, 
max(cq.Ans_ID) Ans_ID
from aca.cx_case_question cq
LEFT JOIN ACA.cx_ilog_answer ia on cq.ans_id = ia.ans_id
LEFT JOIN ACA.cx_ilog_question iq on cq.question_id=iq.question_id

where cq.question_ID in 
(
'Q100282',
'Q100283',
'Q100284',
'Q100285',
'Q100286',
'Q100287',
'Q100288',
'Q100289',
'Q67049',
'Q67049',
'Q67050',
'Q67050',
'Q67051',
'Q67051',
'Q67052',
'Q67052',
'Q67053',
'Q67053',
'Q67054',
'Q67054',
'Q67197',
'Q67201',
'Q67202',
'Q67211',
'Q67212',
'Q67511',
'Q67512',
'Q67513',
'Q67514',
'Q67515',
'Q67516',
'Q67517',
'Q67518',
'Q67519',
'Q67536',
'Q209250',
'Q209536'


)

AND cq.row_added_dt >= current_date - 7
AND cq.Latest_Ind='Y'
GROUP BY 1,2,3;

 Collect statistics column(case_IDx) on  CCRA_BIZ_APP.jk_app_symptom;
 Collect statistics column(Question_Id) on CCRA_BIZ_APP.jk_app_symptom;



DELETE FROM CCRA_BIZ_APP.jk_app_account --another trigger detail
WHERE row_added_dt >= current_date - 7;

INSERT INTO CCRA_BIZ_APP.jk_app_account
select
cq.case_ID case_IDx,
cq.Question_ID, 
cq.row_added_dt,
max(substring(ia.Ans_Txt  from 1 for 75)) app_account, 
max(cq.Ans_ID) Ans_ID
from aca.cx_case_question cq
LEFT JOIN ACA.cx_ilog_answer ia on cq.ans_id = ia.ans_id
LEFT JOIN ACA.cx_ilog_question iq on cq.question_id=iq.question_id
where cq.question_ID in 
(
'Q100454',
'Q100218',
'Q67204',
'Q100309',
'Q100453'
)
AND cq.row_added_dt >= current_date - 7
AND cq.Latest_Ind='Y'
GROUP BY 1,2,3;

 Collect statistics column(case_IDx) on  CCRA_BIZ_APP.jk_app_account;
 Collect statistics column(Question_Id) on CCRA_BIZ_APP.jk_app_account;




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




DELETE FROM CCRA_BIZ_APP.jk_csx_serialized_2
WHERE case_opened_date  >= current_date - 2;

--CREATE SET TABLE CCRA_BIZ_APP.jk_csx_serialized_2 AS (

INSERT INTO CCRA_BIZ_APP.jk_csx_serialized_2

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
x.Eng_Proj_Cd_Ccode AS Program,

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
    AND issue_reason IS NOT NULL
    AND x.serial_nr <>''
    AND x.case_type_cd IN ('TECH', 'RETL','DENY', 'INFO', 'ADMIN', 'CUST', 'SALE') ; --These case types are the only ones we generally ever care about as these are the 'complete' cases


 COLLECT STATISTICS 
            COLUMN ( CASE_ID ) , 
            COLUMN ( case_opened_date ) , 
            COLUMN ( serial_nr ) 
                ON CCRA_BIZ_APP.jk_csx_serialized_2 ;
                
                
 select * from CCRA_BIZ_APP.jk_csx_serialized_2 sample 1000;
 select * from CCRA_BIZ_APP.jk_swrelease_true sample 1000;
 
 select distinct eligible_omni_group, SW_Version_Zinger from CCRA_BIZ_APP.jk_csx_serialized_2;
 
 create multiset volatile table temp003 as --this populates the 'Non-Tech Reason', which is the equivalent of the 'Issue_Desc' for TECH cases

