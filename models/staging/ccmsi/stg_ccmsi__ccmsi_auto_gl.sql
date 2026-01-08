WITH cte_ccmsi_auto_gl AS (
SELECT
*
FROM
{{ source('ccmsi', 'ccmsi_auto_gl') }}
)

SELECT
RTRIM(SUBSTR(raw_line, 1,12)) AS c_claim_number
,RTRIM(SUBSTR(raw_line, 3,3)) AS c_client_number
,RTRIM(SUBSTR(raw_line, 13,30)) AS c_member_name
,RTRIM(SUBSTR(raw_line, 43,15)) AS c_policy_number
,RTRIM(SUBSTR(raw_line, 58,8)) AS c_policy_eff
,RTRIM(SUBSTR(raw_line, 66,8)) AS c_policy_exp
,RTRIM(SUBSTR(raw_line, 74,4)) AS c_coverage_code
,RTRIM(SUBSTR(raw_line, 78,18)) AS c_clmt_last
,RTRIM(SUBSTR(raw_line, 96,12)) AS c_clmt_first
,RTRIM(SUBSTR(raw_line, 108,1)) AS c_clmt_mi
,RTRIM(SUBSTR(raw_line, 109,35)) AS c_clmt_address
,RTRIM(SUBSTR(raw_line, 144,15)) AS c_clmt_city
,RTRIM(SUBSTR(raw_line, 159,2)) AS c_clmt_state
,RTRIM(SUBSTR(raw_line, 161,10)) AS c_clmt_zip
,RTRIM(SUBSTR(raw_line, 171,1)) AS c_marital_stat
,RTRIM(SUBSTR(raw_line, 172,4)) AS c_num_depend
,RTRIM(SUBSTR(raw_line, 176,11)) AS c_soc_sec_num
,RTRIM(SUBSTR(raw_line, 187,2)) AS c_age
,RTRIM(SUBSTR(raw_line, 189,1)) AS c_gender
,RTRIM(SUBSTR(raw_line, 190,8)) AS c_date_of_hire
,RTRIM(SUBSTR(raw_line, 198,8)) AS c_date_curr_job
,RTRIM(SUBSTR(raw_line, 206,4)) AS c_days_work_wk
,RTRIM(SUBSTR(raw_line, 210,9)) AS c_type_employ
,RTRIM(SUBSTR(raw_line, 219,13)) AS c_avg_wkly_wage
,RTRIM(SUBSTR(raw_line, 232,13)) AS c_ttd_rate
,RTRIM(SUBSTR(raw_line, 245,13)) AS c_ppd_rate
,RTRIM(SUBSTR(raw_line, 258,8)) AS c_date_of_loss
,RTRIM(SUBSTR(raw_line, 266,8)) AS c_claim_status
,RTRIM(SUBSTR(raw_line, 274,1)) AS c_clm_denied
,RTRIM(SUBSTR(raw_line, 275,1)) AS c_investigation
,RTRIM(SUBSTR(raw_line, 276,8)) AS c_date_closed
,RTRIM(SUBSTR(raw_line, 284,8)) AS c_clmt_birth
,RTRIM(SUBSTR(raw_line, 292,8)) AS c_clmt_report
,RTRIM(SUBSTR(raw_line, 300,8)) AS c_clm_received
,RTRIM(SUBSTR(raw_line, 308,8)) AS c_clm_input
,RTRIM(SUBSTR(raw_line, 316,10)) AS c_st_clm_num
,RTRIM(SUBSTR(raw_line, 326,4)) AS c_job_class
,RTRIM(SUBSTR(raw_line, 330,4)) AS c_cause
,RTRIM(SUBSTR(raw_line, 334,4)) AS c_loss_type
,RTRIM(SUBSTR(raw_line, 338,4)) AS c_body_part
,RTRIM(SUBSTR(raw_line, 342,50)) AS c_acc_descrip
,RTRIM(SUBSTR(raw_line, 392,13)) AS c_total_res
,RTRIM(SUBSTR(raw_line, 405,13)) AS c_total_paid
,RTRIM(SUBSTR(raw_line, 418,13)) AS c_total_recov
,RTRIM(SUBSTR(raw_line, 431,13)) AS c_total_ded
,RTRIM(SUBSTR(raw_line, 444,13)) AS c_med_paid
,RTRIM(SUBSTR(raw_line, 457,13)) AS c_ind_paid
,RTRIM(SUBSTR(raw_line, 470,13)) AS c_exp_paid
,RTRIM(SUBSTR(raw_line, 483,13)) AS c_daily_ttd
,RTRIM(SUBSTR(raw_line, 496,13)) AS c_os_med_res
,RTRIM(SUBSTR(raw_line, 509,13)) AS c_os_ind_res
,RTRIM(SUBSTR(raw_line, 522,13)) AS c_os_exp_res
,RTRIM(SUBSTR(raw_line, 535,13)) AS c_total_inc
,RTRIM(SUBSTR(raw_line, 548,1)) AS c_ncci_injury
,RTRIM(SUBSTR(raw_line, 549,2)) AS c_acc_state
,RTRIM(SUBSTR(raw_line, 551,30)) AS c_job_desc
,RTRIM(SUBSTR(raw_line, 581,2)) AS c_member_state
,RTRIM(SUBSTR(raw_line, 583,2)) AS c_jurisdiction
,RTRIM(SUBSTR(raw_line, 585,8)) AS c_date_of_death
,RTRIM(SUBSTR(raw_line, 593,8)) AS c_catastrophe
,RTRIM(SUBSTR(raw_line, 601,2)) AS c_loss_cov_code
,RTRIM(SUBSTR(raw_line, 603,15)) AS c_alt_claim_num
,RTRIM(SUBSTR(raw_line, 618,13)) AS c_total_reimb
,RTRIM(SUBSTR(raw_line, 631,13)) AS c_admin_exp_paid
,RTRIM(SUBSTR(raw_line, 644,13)) AS c_os_AdminExp_res
,RTRIM(SUBSTR(raw_line, 657,15)) AS Occurrence_Number
,RTRIM(SUBSTR(raw_line, 672,500)) AS Occurrence_Description
,RTRIM(SUBSTR(raw_line, 1172,25)) AS VIN
,RTRIM(SUBSTR(raw_line, 1197,20)) AS Vehicle_Make
,RTRIM(SUBSTR(raw_line, 1232,30)) AS Vehicle_Model
,RTRIM(SUBSTR(raw_line, 1262,4)) AS Vehicle_Year
,RTRIM(SUBSTR(raw_line, 1266,1)) AS Fraudulent_Claim_YN
,file_name
,etl_date
FROM
cte_ccmsi_auto_gl
