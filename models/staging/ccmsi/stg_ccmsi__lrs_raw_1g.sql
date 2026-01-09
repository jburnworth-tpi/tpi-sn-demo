WITH cte_ccmsi_lrs_raw_1g AS (
SELECT
*, current_date() as etl_date
FROM
{{ source('lrs_sedgewick', 'lrs_raw_1g') }}
)

SELECT
RTRIM(SUBSTR(raw_line, 4, 4)) AS sedgwick_cms_client_id /* Sedgwick CMS Client ID */
,RTRIM(SUBSTR(raw_line, 8, 8)) AS sedgwick_cms_client_account /* Sedgwick CMS Client Account  */
,RTRIM(SUBSTR(raw_line, 16, 6)) AS sedgwick_cms_client_location /* Sedgwick CMS Client Location */
,RTRIM(SUBSTR(raw_line, 22, 18)) AS claim_number /* "Claim Number  */
,RTRIM(SUBSTR(raw_line, 40, 3)) AS claim_vendor_id /* Claim Vendor ID (originator of claim number) */
,RTRIM(SUBSTR(raw_line, 43, 3)) AS processing_office /* Processing Office */
,RTRIM(SUBSTR(raw_line, 46, 2)) AS line_type /* "Line Type: */
,RTRIM(SUBSTR(raw_line, 48, 8)) AS date_of_loss /* Date of Loss */
,RTRIM(SUBSTR(raw_line, 56, 4)) AS time_of_loss /* Time of Loss */
,RTRIM(SUBSTR(raw_line, 60, 50)) AS description_of_event /* Description of Event */
,RTRIM(SUBSTR(raw_line, 110, 80)) AS description_of_loss /* Description of Loss */
,RTRIM(SUBSTR(raw_line, 190, 2)) AS claim_type /* "Claim Type: */
,RTRIM(SUBSTR(raw_line, 192, 2)) AS claim_subtype /* Claim Subtype  (Appendix T) */
,RTRIM(SUBSTR(raw_line, 194, 1)) AS claim_status /* "Claim Status: */
,RTRIM(SUBSTR(raw_line, 195, 1)) AS claim_substatus /* "Claim Sub status: */
,RTRIM(SUBSTR(raw_line, 196, 8)) AS date_reported_to_client /* Date Reported to Client */
,RTRIM(SUBSTR(raw_line, 212, 8)) AS date_opened /* Date Opened */
,RTRIM(SUBSTR(raw_line, 220, 8)) AS date_closed /* Date Closed */
,RTRIM(SUBSTR(raw_line, 228, 8)) AS date_reopened /* Date Reopened */
,RTRIM(SUBSTR(raw_line, 236, 2)) AS state_of_employment_premium /* State of Employment/Premium   (Appendix N) */
,RTRIM(SUBSTR(raw_line, 240, 2)) AS state_of_jurisdiction /* State of Jurisdiction (Appendix N) */
,RTRIM(SUBSTR(raw_line, 242, 4)) AS source_code /* Source Code (Appendix A) */
,RTRIM(SUBSTR(raw_line, 246, 4)) AS cause_code /* Cause Code  (Appendix B) */
,RTRIM(SUBSTR(raw_line, 250, 4)) AS nature_result_code /* Nature/Result Code (Appendix C) */
,RTRIM(SUBSTR(raw_line, 254, 4)) AS part_target_code /* Part/Target Code (Appendix D) */
,RTRIM(SUBSTR(raw_line, 258, 4)) AS site_code /* Site Code (Client Defined) */
,RTRIM(SUBSTR(raw_line, 262, 30)) AS "Lo_ Location Street" /* Loss Location Street */
,RTRIM(SUBSTR(raw_line, 292, 25)) AS "Lo_ Location City" /* Loss Location City */
,RTRIM(SUBSTR(raw_line, 317, 15)) AS "Lo_ Location Po_tal Code" /* Loss Location Postal Code */
,RTRIM(SUBSTR(raw_line, 332, 1)) AS " Claimant In_ured Name Type" /* "Claimant/Insured Name Type:  */
,RTRIM(SUBSTR(raw_line, 333, 25)) AS "Claimant In_ured La_t Name" /* Claimant/Insured Last Name */
,RTRIM(SUBSTR(raw_line, 358, 20)) AS "Claimant In_ured Fir_t Name" /* Claimant/Insured First Name */
,RTRIM(SUBSTR(raw_line, 378, 1)) AS "Claimant In_ured Middle Initial" /* Claimant/Insured Middle Initial */
,RTRIM(SUBSTR(raw_line, 379, 30)) AS "Claimant In_ured Addre_ Line 1" /* Claimant/Insured Address Line 1 */
,RTRIM(SUBSTR(raw_line, 409, 30)) AS "Claimant In_ured Addre_ Line 2" /* Claimant/Insured Address Line 2 */
,RTRIM(SUBSTR(raw_line, 439, 30)) AS "Claimant In_ured City" /* Claimant/Insured City */
,RTRIM(SUBSTR(raw_line, 469, 2)) AS "Claimant In_ured State" /* Claimant/Insured State */
,RTRIM(SUBSTR(raw_line, 471, 15)) AS "Claimant In_ured Po_tal Code" /* Claimant/Insured Postal Code */
,RTRIM(SUBSTR(raw_line, 486, 20)) AS "Claimant Country" /* Claimant Country  */
,RTRIM(SUBSTR(raw_line, 506, 9)) AS "Claimant S_n" /* Claimant SSN */
,RTRIM(SUBSTR(raw_line, 515, 3)) AS "Claimant Age" /* Claimant Age */
,RTRIM(SUBSTR(raw_line, 518, 8)) AS "Date Of Birth" /* Date of Birth */
,RTRIM(SUBSTR(raw_line, 526, 8)) AS "Date Of Death" /* Date of Death */
,RTRIM(SUBSTR(raw_line, 534, 2)) AS "Number Of Dependent_" /* Number of Dependents */
,RTRIM(SUBSTR(raw_line, 536, 1)) AS " Marital Statu_" /* "Marital Status: */
,RTRIM(SUBSTR(raw_line, 537, 1)) AS " Gender" /* "Gender:  */
,RTRIM(SUBSTR(raw_line, 538, 8)) AS "Date Of Hire" /* Date of Hire */
,RTRIM(SUBSTR(raw_line, 546, 6)) AS "Length Of Service" /* Length of Service */
,RTRIM(SUBSTR(raw_line, 552, 2)) AS "Filler_1" /* Filler */
,RTRIM(SUBSTR(raw_line, 554, 15)) AS "State A_igned Claim Number" /* State Assigned Claim Number */
,RTRIM(SUBSTR(raw_line, 569, 4)) AS "Job Cla_ification Code" /* Job Classification Code */
,RTRIM(SUBSTR(raw_line, 573, 15)) AS "Occupation" /* Occupation */
,RTRIM(SUBSTR(raw_line, 588, 15)) AS "Department" /* Department */
,RTRIM(SUBSTR(raw_line, 603, 25)) AS "Supervi_or" /* Supervisor */
,RTRIM(SUBSTR(raw_line, 628, 4)) AS "Union Id" /* Union ID */
,RTRIM(SUBSTR(raw_line, 632, 7)) AS "Average Weekly Wage" /* Average Weekly Wage */
,RTRIM(SUBSTR(raw_line, 639, 4)) AS "Filler_2" /* Filler */
,RTRIM(SUBSTR(raw_line, 643, 4)) AS "Number Of Lo_t Day_" /* Number of Lost Days */
,RTRIM(SUBSTR(raw_line, 647, 8)) AS "La_t Day Worked" /* Last Day Worked (Latest) */
,RTRIM(SUBSTR(raw_line, 655, 8)) AS "Date Return To Work Re_tricted Duty" /* Date Return to Work-Restricted Duty (Latest) */
,RTRIM(SUBSTR(raw_line, 663, 8)) AS "Date Return To Work Full Duty" /* Date Return to Work-Full Duty (Latest) */
,RTRIM(SUBSTR(raw_line, 671, 2)) AS "But Sedgwick Default_ Are Defined In Appendix W _1" /* "Line of Coverage (Can be defined by Client */
,RTRIM(SUBSTR(raw_line, 673, 2)) AS "But Sedgwick Default_ Are Defined In Appendix W _2" /* "Coverage (Can be defined by Client */
,RTRIM(SUBSTR(raw_line, 675, 1)) AS " Program Type" /* "Program Type: */
,RTRIM(SUBSTR(raw_line, 676, 1)) AS " Policy Type" /* "Policy Type: */
,RTRIM(SUBSTR(raw_line, 677, 6)) AS "Carrier Id" /* Carrier ID (Appendix M) */
,RTRIM(SUBSTR(raw_line, 683, 18)) AS "Filler_3" /* Filler */
,RTRIM(SUBSTR(raw_line, 701, 8)) AS "Policy Effective Date" /* Policy Effective Date */
,RTRIM(SUBSTR(raw_line, 709, 8)) AS "Policy Expiration Date" /* Policy Expiration Date */
,RTRIM(SUBSTR(raw_line, 717, 1)) AS " Claim Made Indicator" /* "Claim Made Indicator: */
,RTRIM(SUBSTR(raw_line, 718, 8)) AS "Claim Made Date" /* Claim Made Date */
,RTRIM(SUBSTR(raw_line, 726, 8)) AS "Claim Made Retroactive Date" /* Claim Made Retroactive Date */
,RTRIM(SUBSTR(raw_line, 734, 8)) AS "Claim Made Tail Date" /* Claim Made Tail Date */
,RTRIM(SUBSTR(raw_line, 742, 8)) AS "Sir Deductible" /* SIR/Deductible  */
,RTRIM(SUBSTR(raw_line, 750, 10)) AS "Deductible Applied" /* Deductible Applied */
,RTRIM(SUBSTR(raw_line, 760, 2)) AS " Filler Field No Longer Updateable In The Claim Sy_tem" /* "Filler - FIELD NO LONGER UPDATEABLE IN THE CLAIM SYSTEM */
,RTRIM(SUBSTR(raw_line, 762, 1)) AS " Exce_ Indicator" /* "Excess Indicator: */
,RTRIM(SUBSTR(raw_line, 763, 1)) AS " Subrogation Indicator" /* "Subrogation Indicator: */
,RTRIM(SUBSTR(raw_line, 764, 1)) AS " Rehabilitation Indicator" /* "Rehabilitation Indicator: */
,RTRIM(SUBSTR(raw_line, 765, 1)) AS " Apportionment Indicator" /* "Apportionment Indicator: */
,RTRIM(SUBSTR(raw_line, 766, 3)) AS "Percent Of Apportionment" /* Percent of Apportionment */
,RTRIM(SUBSTR(raw_line, 769, 1)) AS " Ho_pitalization Indicator" /* "Hospitalization Indicator: */
,RTRIM(SUBSTR(raw_line, 770, 1)) AS " Suit Legal Indicator" /* "Suit/Legal Indicator */
,RTRIM(SUBSTR(raw_line, 771, 8)) AS "Date Suit Filed" /* Date Suit Filed */
,RTRIM(SUBSTR(raw_line, 779, 1)) AS " O_ha Reportable Indicator" /* "OSHA Reportable Indicator: */
,RTRIM(SUBSTR(raw_line, 780, 4)) AS " Filler Formerly Safety Reportable Day_ " /* "Filler - formerly ""Safety Reportable Days""" */
,RTRIM(SUBSTR(raw_line, 784, 4)) AS " Filler Formerly Safety Re_tricted Day_ " /* "Filler - formerly ""Safety Restricted Days""" */
,RTRIM(SUBSTR(raw_line, 788, 1)) AS " O_ha Accident Illne_ Code" /* "OSHA Accident/Illness Code (WC Only): */
,RTRIM(SUBSTR(raw_line, 789, 17)) AS "Vehicle Identification Number" /* Vehicle Identification Number */
,RTRIM(SUBSTR(raw_line, 806, 12)) AS "Vehicle Make" /* Vehicle Make */
,RTRIM(SUBSTR(raw_line, 818, 12)) AS "Vehicle Model" /* Vehicle Model */
,RTRIM(SUBSTR(raw_line, 830, 4)) AS "Vehicle Year" /* Vehicle Year */
,RTRIM(SUBSTR(raw_line, 834, 30)) AS "Lo_ Payee" /* Loss Payee */
,RTRIM(SUBSTR(raw_line, 864, 30)) AS "Driver Name" /* Driver Name */
,RTRIM(SUBSTR(raw_line, 894, 3)) AS "Driver Age" /* Driver Age (calculated from Date of Birth) */
,RTRIM(SUBSTR(raw_line, 897, 1)) AS " Driver Gender" /* "Driver Gender: */
,RTRIM(SUBSTR(raw_line, 898, 1)) AS " Driver Chargeable" /* "Driver Chargeable */
,RTRIM(SUBSTR(raw_line, 899, 30)) AS "Product Name" /* Product Name */
,RTRIM(SUBSTR(raw_line, 929, 8)) AS "Date Of La_t Action" /* Date of Last Action (Other than inquiry) */
,RTRIM(SUBSTR(raw_line, 937, 8)) AS "Examiner" /* Examiner */
,RTRIM(SUBSTR(raw_line, 945, 1)) AS " Converted Claim Indicator" /* "Converted Claim Indicator (from other TPA) */
,RTRIM(SUBSTR(raw_line, 946, 2)) AS "Pr Appendix E " /* "Paid/Incurred Category-AU */
,RTRIM(SUBSTR(raw_line, 948, 12)) AS "Paid To Date Indemnity Lo_" /* Paid to Date-Indemnity/Loss  */
,RTRIM(SUBSTR(raw_line, 960, 12)) AS "Paid To Date Medical" /* Paid to Date-Medical  (WC/DS Only) */
,RTRIM(SUBSTR(raw_line, 972, 12)) AS "Paid To Date Other Expen_e" /* Paid to Date-Other Expense  (WC Only) */
,RTRIM(SUBSTR(raw_line, 984, 12)) AS "Paid To Date Expen_e" /* Paid to Date-Expense   */
,RTRIM(SUBSTR(raw_line, 996, 12)) AS "Paid Current Period Indemnity Lo_" /* Paid Current Period-Indemnity/Loss  */
,RTRIM(SUBSTR(raw_line, 1008, 12)) AS "Paid Current Period Medical" /* Paid Current Period-Medical  (WC/DS Only) */
,RTRIM(SUBSTR(raw_line, 1020, 12)) AS "Paid Current Period Other Expen_e" /* Paid Current Period-Other Expense  (WC Only) */
,RTRIM(SUBSTR(raw_line, 1032, 12)) AS "Paid Current Period Expen_e" /* Paid Current Period-Expense  */
,RTRIM(SUBSTR(raw_line, 1044, 12)) AS "Incurred Indemnity Lo_" /* Incurred-Indemnity/Loss  */
,RTRIM(SUBSTR(raw_line, 1056, 12)) AS "Incurred Medical" /* Incurred-Medical  (WC/DS Only) */
,RTRIM(SUBSTR(raw_line, 1068, 12)) AS "Incurred Other Expen_e" /* Incurred-Other Expense  (WC Only) */
,RTRIM(SUBSTR(raw_line, 1080, 12)) AS "Incurred Expen_e" /* Incurred-Expense */
,RTRIM(SUBSTR(raw_line, 1092, 12)) AS "Change In Re_erve Indemnity Lo_" /* Change in Reserve-Indemnity/Loss */
,RTRIM(SUBSTR(raw_line, 1104, 12)) AS "Change In Re_erve Medical" /* Change in Reserve-Medical  (WC/DS Only) */
,RTRIM(SUBSTR(raw_line, 1116, 12)) AS "Change In Re_erve Other Expen_e" /* Change in Reserve-Other Expense  (WC Only) */
,RTRIM(SUBSTR(raw_line, 1128, 12)) AS "Change In Re_erve Expen_e" /* Change in Reserve-Expense */
,RTRIM(SUBSTR(raw_line, 1140, 12)) AS "Recovered To Date Indemnity Lo_" /* Recovered to Date-Indemnity/Loss  */
,RTRIM(SUBSTR(raw_line, 1152, 12)) AS "Recovered To Date Medical" /* Recovered to Date-Medical  (WC/DS Only) */
,RTRIM(SUBSTR(raw_line, 1164, 12)) AS "Recovered To Date Other Expen_e" /* Recovered to Date-Other Expense  (WC Only) */
,RTRIM(SUBSTR(raw_line, 1176, 12)) AS "Recovered To Date Expen_e" /* Recovered to Date-Expense */
,RTRIM(SUBSTR(raw_line, 1188, 10)) AS "Event Number" /* Event Number */
,RTRIM(SUBSTR(raw_line, 1198, 5)) AS "Suffix Number" /* Suffix Number */
,RTRIM(SUBSTR(raw_line, 1203, 1)) AS "Carrier Change Statu_" /* Carrier Change Status */
,RTRIM(SUBSTR(raw_line, 1204, 8)) AS "Date Created" /* Date Created */
,RTRIM(SUBSTR(raw_line, 1212, 3)) AS "Examiner Office" /* Examiner Office  (Appendix R) */
,RTRIM(SUBSTR(raw_line, 1215, 1)) AS " Part Of Body Side" /* "Part of Body Side */
,RTRIM(SUBSTR(raw_line, 1216, 18)) AS "Claimant Telephone Number" /* Claimant Telephone Number */
,RTRIM(SUBSTR(raw_line, 1234, 8)) AS "Date Of Claimant Contact" /* Date of Claimant Contact */
,RTRIM(SUBSTR(raw_line, 1242, 16)) AS "Claim Internal Unique Id" /* Claim Internal Unique ID  */
,RTRIM(SUBSTR(raw_line, 1258, 15)) AS "We Have The Ability To Send The Client S Employee Id" /* "Employee Unique ID (This is the Employee Unique Number of the claimant.  While sometimes true */
,RTRIM(SUBSTR(raw_line, 1273, 1)) AS " Claimant Medicare Beneficiary Statu_" /* "Claimant Medicare Beneficiary Status */
,RTRIM(SUBSTR(raw_line, 1274, 12)) AS "Claimant Health In_urance Claim Number" /* Claimant Health Insurance Claim Number (HICN) */
,RTRIM(SUBSTR(raw_line, 1286, 4)) AS "Plan Number" /* Plan Number */
,RTRIM(SUBSTR(raw_line, 1290, 4)) AS "Line Number" /* Line Number */
,RTRIM(SUBSTR(raw_line, 1294, 50)) AS "Location Node" /* Location-Node (Utilized by SRS as Billing Code) */
,RTRIM(SUBSTR(raw_line, 1344, 22)) AS "Policy Number" /* Policy Number */
,RTRIM(SUBSTR(raw_line, 1366, 320)) AS "Examiner S Email Addre_" /* Examiner's Email Address */
,RTRIM(SUBSTR(raw_line, 1686, 1)) AS " Tender Flag" /* "Tender Flag  */
,RTRIM(SUBSTR(raw_line, 1687, 18)) AS "Claimant Work Phone" /* Claimant Work Phone */
,RTRIM(SUBSTR(raw_line, 1705, 4)) AS "Claimant Work Exten_ion" /* Claimant Work Extension */
,RTRIM(SUBSTR(raw_line, 1709, 40)) AS "Lo_ Organization" /* Loss Organization */
,RTRIM(SUBSTR(raw_line, 1749, 20)) AS "Lo_ County" /* Loss County */
,RTRIM(SUBSTR(raw_line, 1769, 2)) AS " Claimant Medical Accommodation Code" /* "Claimant Medical Accommodation Code (Canadian Claims) */
,RTRIM(SUBSTR(raw_line, 1771, 2)) AS " Claimant Occupation Code" /* "Claimant Occupation Code */
,RTRIM(SUBSTR(raw_line, 1773, 1)) AS " Coin_urance Flag" /* "Coinsurance Flag */
,RTRIM(SUBSTR(raw_line, 1774, 9)) AS "Coin_urance Limit" /* Coinsurance Limit */
,RTRIM(SUBSTR(raw_line, 1783, 2)) AS "Coin_urance Percent" /* Coinsurance Percent */
,RTRIM(SUBSTR(raw_line, 1785, 8)) AS "Date Policy Limit Demand Received" /* Date Policy Limit Demand Received */
,RTRIM(SUBSTR(raw_line, 1793, 1)) AS " Latent Group Indicator" /* "Latent Group Indicator */
,RTRIM(SUBSTR(raw_line, 1794, 3)) AS "Union Code" /* Union Code (This data element only applies to workers comp and disability claims.  It is only available if sent to Sedgwick in a feed from the client.) */
,RTRIM(SUBSTR(raw_line, 1797, 4)) AS "Union Local" /* Union Local (This data element only applies to workers comp and disability claims.  It is only available if sent to Sedgwick in a feed from the client.) */
,RTRIM(SUBSTR(raw_line, 1801, 2)) AS "Sub Cla_ Code" /* Sub Class Code (WC WA Jurisdiction Only) */
,file_name
,etl_date
FROM
cte_ccmsi_lrs_raw_1g
