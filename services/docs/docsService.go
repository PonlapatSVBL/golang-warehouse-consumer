package docsService

import (
	"fmt"
	"humansoft/warehouse-consumer/connections/mysql"
)

type SlipReportStruct struct {
	MasterSalaryReportId string `json:"master_salary_report_id" db:"master_salary_report_id"`
	MasterSalaryMonth    string `json:"master_salary_month" db:"master_salary_month"`
}

type DocsStruct struct {
	BranchId     string `json:"branch_id" db:"branch_id"`
	BranchCode   string `json:"branch_code" db:"branch_code"`
	BranchName   string `json:"branch_name" db:"branch_name"`
	BranchNameEn string `json:"branch_name_en" db:"branch_name_en"`

	HolidayChangeCount                     int16 `json:"holiday_change_count" db:"holiday_change_count"`
	HolidayChangeDisapproveCount           int16 `json:"holiday_change_disapprove_count" db:"holiday_change_disapprove_count"`
	OtWorkCount                            int16 `json:"ot_work_count" db:"ot_work_count"`
	OtWorkDisapproveCount                  int16 `json:"ot_work_disapprove_count" db:"ot_work_disapprove_count"`
	TimeAdjustCount                        int16 `json:"time_adjust_count" db:"time_adjust_count"`
	TimeAdjustDisapproveCount              int16 `json:"time_adjust_disapprove_count" db:"time_adjust_disapprove_count"`
	TimeLeaveCount                         int16 `json:"time_leave_count" db:"time_leave_count"`
	TimeLeaveDisapproveCount               int16 `json:"time_leave_disapprove_count" db:"time_leave_disapprove_count"`
	TimeLeave01Count                       int16 `json:"time_leave_01_count" db:"time_leave_01_count"`
	TimeLeave02Count                       int16 `json:"time_leave_02_count" db:"time_leave_02_count"`
	TimeLeave03Count                       int16 `json:"time_leave_03_count" db:"time_leave_03_count"`
	TimeLeave04Count                       int16 `json:"time_leave_04_count" db:"time_leave_04_count"`
	TimeLeave05Count                       int16 `json:"time_leave_05_count" db:"time_leave_05_count"`
	TimeLeave06Count                       int16 `json:"time_leave_06_count" db:"time_leave_06_count"`
	TimeLeave07Count                       int16 `json:"time_leave_07_count" db:"time_leave_07_count"`
	TimeLeave08Count                       int16 `json:"time_leave_08_count" db:"time_leave_08_count"`
	TimeLeave09Count                       int16 `json:"time_leave_09_count" db:"time_leave_09_count"`
	TimeLeave10Count                       int16 `json:"time_leave_10_count" db:"time_leave_10_count"`
	TimeLeave11Count                       int16 `json:"time_leave_11_count" db:"time_leave_11_count"`
	TimeLeave12Count                       int16 `json:"time_leave_12_count" db:"time_leave_12_count"`
	TimeLeave13Count                       int16 `json:"time_leave_13_count" db:"time_leave_13_count"`
	TimeLeave14Count                       int16 `json:"time_leave_14_count" db:"time_leave_14_count"`
	TimeLeave15Count                       int16 `json:"time_leave_15_count" db:"time_leave_15_count"`
	TimeLeave16Count                       int16 `json:"time_leave_16_count" db:"time_leave_16_count"`
	TimeLeave17Count                       int16 `json:"time_leave_17_count" db:"time_leave_17_count"`
	TimeLeave18Count                       int16 `json:"time_leave_18_count" db:"time_leave_18_count"`
	TimeLeave19Count                       int16 `json:"time_leave_19_count" db:"time_leave_19_count"`
	TimeLeave20Count                       int16 `json:"time_leave_20_count" db:"time_leave_20_count"`
	WorkCycleChangeCount                   int16 `json:"work_cycle_change_count" db:"work_cycle_change_count"`
	WorkCycleChangeDisapproveCount         int16 `json:"work_cycle_change_disapprove_count" db:"work_cycle_change_disapprove_count"`
	WithdrawDocCount                       int16 `json:"withdraw_doc_count" db:"withdraw_doc_count"`
	WithdrawDocDisapproveCount             int16 `json:"withdraw_doc_disapprove_count" db:"withdraw_doc_disapprove_count"`
	PettyCashCount                         int16 `json:"petty_cash_count" db:"petty_cash_count"`
	PettyCashDisapproveCount               int16 `json:"petty_cash_disapprove_count" db:"petty_cash_disapprove_count"`
	SalaryCertificateLetterCount           int16 `json:"salary_certificate_letter_count" db:"salary_certificate_letter_count"`
	SalaryCertificateLetterDisapproveCount int16 `json:"salary_certificate_letter_disapprove_count" db:"salary_certificate_letter_disapprove_count"`
	WorkCertificateLetterCount             int16 `json:"work_certificate_letter_count" db:"work_certificate_letter_count"`
	WorkCertificateLetterDisapproveCount   int16 `json:"work_certificate_letter_disapprove_count" db:"work_certificate_letter_disapprove_count"`
	WelfareCount                           int16 `json:"welfare_count" db:"welfare_count"`
	WelfareDisapproveCount                 int16 `json:"welfare_disapprove_count" db:"welfare_disapprove_count"`
	ComplaintCount                         int16 `json:"complaint_count" db:"complaint_count"`
	ComplaintDisapproveCount               int16 `json:"complaint_disapprove_count" db:"complaint_disapprove_count"`
}

func GetSummaryDocs(param map[string]string) ([]DocsStruct, error) {
	var slipReport SlipReportStruct
	var docsSummary []DocsStruct

	mysqlInstance := mysql.NewMysql()

	query := fmt.Sprintf(`SELECT master_salary_report_id
	, master_salary_month
    FROM %s.payroll_master_salary_report
    WHERE server_id = '%s'
    AND instance_server_id = '%s'
    AND instance_server_channel_id = '%s'
    AND master_salary_month = '%s' `, param["dbn"], param["server_id"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"])
	slipReport = SlipReportStruct{}
	mysqlInstance.SqlGet(&slipReport, query)

	query = fmt.Sprintf(`SELECT _branch.branch_id
    , _branch.branch_code
    , IFNULL(_branch.branch_name, '') AS branch_name
    , IFNULL(_branch.branch_name_en, '') AS branch_name_en
    , IFNULL(_summary.holiday_change_count, 0) AS holiday_change_count
    , IFNULL(_summary.holiday_change_disapprove_count, 0) AS holiday_change_disapprove_count
    , IFNULL(_summary.ot_work_count, 0) AS ot_work_count
    , IFNULL(_summary.ot_work_disapprove_count, 0) AS ot_work_disapprove_count
    , IFNULL(_summary.time_adjust_count, 0) AS time_adjust_count
    , IFNULL(_summary.time_adjust_disapprove_count, 0) AS time_adjust_disapprove_count
    , IFNULL(_summary.time_leave_count, 0) AS time_leave_count
    , IFNULL(_summary.time_leave_disapprove_count, 0) AS time_leave_disapprove_count
    , IFNULL(_summary.time_leave_01_count, 0) AS time_leave_01_count
    , IFNULL(_summary.time_leave_02_count, 0) AS time_leave_02_count
    , IFNULL(_summary.time_leave_03_count, 0) AS time_leave_03_count
    , IFNULL(_summary.time_leave_04_count, 0) AS time_leave_04_count
    , IFNULL(_summary.time_leave_05_count, 0) AS time_leave_05_count
    , IFNULL(_summary.time_leave_06_count, 0) AS time_leave_06_count
    , IFNULL(_summary.time_leave_07_count, 0) AS time_leave_07_count
    , IFNULL(_summary.time_leave_08_count, 0) AS time_leave_08_count
    , IFNULL(_summary.time_leave_09_count, 0) AS time_leave_09_count
    , IFNULL(_summary.time_leave_10_count, 0) AS time_leave_10_count
    , IFNULL(_summary.time_leave_11_count, 0) AS time_leave_11_count
    , IFNULL(_summary.time_leave_12_count, 0) AS time_leave_12_count
    , IFNULL(_summary.time_leave_13_count, 0) AS time_leave_13_count
    , IFNULL(_summary.time_leave_14_count, 0) AS time_leave_14_count
    , IFNULL(_summary.time_leave_15_count, 0) AS time_leave_15_count
    , IFNULL(_summary.time_leave_16_count, 0) AS time_leave_16_count
    , IFNULL(_summary.time_leave_17_count, 0) AS time_leave_17_count
    , IFNULL(_summary.time_leave_18_count, 0) AS time_leave_18_count
    , IFNULL(_summary.time_leave_19_count, 0) AS time_leave_19_count
    , IFNULL(_summary.time_leave_20_count, 0) AS time_leave_20_count
    , IFNULL(_summary.work_cycle_change_count, 0) AS work_cycle_change_count
    , IFNULL(_summary.work_cycle_change_disapprove_count, 0) AS work_cycle_change_disapprove_count
    , IFNULL(_summary.withdraw_doc_count, 0) AS withdraw_doc_count
    , IFNULL(_summary.withdraw_doc_disapprove_count, 0) AS withdraw_doc_disapprove_count
    , IFNULL(_summary.petty_cash_count, 0) AS petty_cash_count
    , IFNULL(_summary.petty_cash_disapprove_count, 0) AS petty_cash_disapprove_count
    , IFNULL(_summary.salary_certificate_letter_count, 0) AS salary_certificate_letter_count
    , IFNULL(_summary.salary_certificate_letter_disapprove_count, 0) AS salary_certificate_letter_disapprove_count
    , IFNULL(_summary.work_certificate_letter_count, 0) AS work_certificate_letter_count
    , IFNULL(_summary.work_certificate_letter_disapprove_count, 0) AS work_certificate_letter_disapprove_count
    , IFNULL(_summary.welfare_count, 0) AS welfare_count
    , IFNULL(_summary.welfare_disapprove_count, 0) AS welfare_disapprove_count
    , IFNULL(_summary.complaint_count, 0) AS complaint_count
    , IFNULL(_summary.complaint_disapprove_count, 0) AS complaint_disapprove_count
    FROM hms_api.comp_branch _branch
    LEFT JOIN (
        SELECT _emp.branch_id
        , SUM(IFNULL(_holiday_change.holiday_change_count, 0)) AS holiday_change_count
        , SUM(IFNULL(_holiday_change.holiday_change_disapprove_count, 0)) AS holiday_change_disapprove_count
        , SUM(IFNULL(_ot_work.ot_work_count, 0)) AS ot_work_count
        , SUM(IFNULL(_ot_work.ot_work_disapprove_count, 0)) AS ot_work_disapprove_count
        , SUM(IFNULL(_time_adjust.time_adjust_count, 0)) AS time_adjust_count
        , SUM(IFNULL(_time_adjust.time_adjust_disapprove_count, 0)) AS time_adjust_disapprove_count
        , SUM(IFNULL(_time_leave.time_leave_count, 0)) AS time_leave_count
        , SUM(IFNULL(_time_leave.time_leave_disapprove_count, 0)) AS time_leave_disapprove_count
        , SUM(IFNULL(_time_leave.time_leave_01_count, 0)) AS time_leave_01_count
        , SUM(IFNULL(_time_leave.time_leave_02_count, 0)) AS time_leave_02_count
        , SUM(IFNULL(_time_leave.time_leave_03_count, 0)) AS time_leave_03_count
        , SUM(IFNULL(_time_leave.time_leave_04_count, 0)) AS time_leave_04_count
        , SUM(IFNULL(_time_leave.time_leave_05_count, 0)) AS time_leave_05_count
        , SUM(IFNULL(_time_leave.time_leave_06_count, 0)) AS time_leave_06_count
        , SUM(IFNULL(_time_leave.time_leave_07_count, 0)) AS time_leave_07_count
        , SUM(IFNULL(_time_leave.time_leave_08_count, 0)) AS time_leave_08_count
        , SUM(IFNULL(_time_leave.time_leave_09_count, 0)) AS time_leave_09_count
        , SUM(IFNULL(_time_leave.time_leave_10_count, 0)) AS time_leave_10_count
        , SUM(IFNULL(_time_leave.time_leave_11_count, 0)) AS time_leave_11_count
        , SUM(IFNULL(_time_leave.time_leave_12_count, 0)) AS time_leave_12_count
        , SUM(IFNULL(_time_leave.time_leave_13_count, 0)) AS time_leave_13_count
        , SUM(IFNULL(_time_leave.time_leave_14_count, 0)) AS time_leave_14_count
        , SUM(IFNULL(_time_leave.time_leave_15_count, 0)) AS time_leave_15_count
        , SUM(IFNULL(_time_leave.time_leave_16_count, 0)) AS time_leave_16_count
        , SUM(IFNULL(_time_leave.time_leave_17_count, 0)) AS time_leave_17_count
        , SUM(IFNULL(_time_leave.time_leave_18_count, 0)) AS time_leave_18_count
        , SUM(IFNULL(_time_leave.time_leave_19_count, 0)) AS time_leave_19_count
        , SUM(IFNULL(_time_leave.time_leave_20_count, 0)) AS time_leave_20_count
        , SUM(IFNULL(_work_cycle_change.work_cycle_change_count, 0)) AS work_cycle_change_count
        , SUM(IFNULL(_work_cycle_change.work_cycle_change_disapprove_count, 0)) AS work_cycle_change_disapprove_count
        , SUM(IFNULL(_withdraw_doc.withdraw_doc_count, 0)) AS withdraw_doc_count
        , SUM(IFNULL(_withdraw_doc.withdraw_doc_disapprove_count, 0)) AS withdraw_doc_disapprove_count
        , SUM(IFNULL(_petty_cash.petty_cash_count, 0)) AS petty_cash_count
        , SUM(IFNULL(_petty_cash.petty_cash_disapprove_count, 0)) AS petty_cash_disapprove_count
        , SUM(IFNULL(_salary_certificate_letter.salary_certificate_letter_count, 0)) AS salary_certificate_letter_count
        , SUM(IFNULL(_salary_certificate_letter.salary_certificate_letter_disapprove_count, 0)) AS salary_certificate_letter_disapprove_count
        , SUM(IFNULL(_work_certificate_letter.work_certificate_letter_count, 0)) AS work_certificate_letter_count
        , SUM(IFNULL(_work_certificate_letter.work_certificate_letter_disapprove_count, 0)) AS work_certificate_letter_disapprove_count
        , SUM(IFNULL(_welfare.welfare_count, 0)) AS welfare_count
        , SUM(IFNULL(_welfare.welfare_disapprove_count, 0)) AS welfare_disapprove_count
        , SUM(IFNULL(_complaint.complaint_count, 0)) AS complaint_count
        , SUM(IFNULL(_complaint.complaint_disapprove_count, 0)) AS complaint_disapprove_count
        FROM hms_api.comp_employee _emp
        LEFT JOIN (
            SELECT employee_id
            , COUNT(*) AS holiday_change_count
            , SUM(CASE WHEN approve_flag != '02' THEN 1 ELSE 0 END) AS holiday_change_disapprove_count
            FROM %s.payroll_holiday_change
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id 
        ) _holiday_change ON (_emp.employee_id = _holiday_change.employee_id) 
        LEFT JOIN (
            SELECT employee_id
            , COUNT(*) AS ot_work_count
            , SUM(CASE WHEN approve_flag != '02' THEN 1 ELSE 0 END) AS ot_work_disapprove_count
            FROM %s.payroll_ot_work
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _ot_work ON (_emp.employee_id = _ot_work.employee_id) 
        LEFT JOIN (
            SELECT employee_id
            , COUNT(*) AS time_adjust_count
            , SUM(CASE WHEN approve_flag != '02' THEN 1 ELSE 0 END) AS time_adjust_disapprove_count
            FROM %s.payroll_time_adjust
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _time_adjust ON (_emp.employee_id = _time_adjust.employee_id) 
        LEFT JOIN (
            SELECT employee_id
            , COUNT(*) AS time_leave_count
            , SUM(CASE WHEN approve_flag != '02' THEN 1 ELSE 0 END) AS time_leave_disapprove_count
            , SUM(CASE WHEN absence_flag_lv = '01' THEN 1 ELSE 0 END) AS time_leave_01_count
            , SUM(CASE WHEN absence_flag_lv = '02' THEN 1 ELSE 0 END) AS time_leave_02_count
            , SUM(CASE WHEN absence_flag_lv = '03' THEN 1 ELSE 0 END) AS time_leave_03_count
            , SUM(CASE WHEN absence_flag_lv = '04' THEN 1 ELSE 0 END) AS time_leave_04_count
            , SUM(CASE WHEN absence_flag_lv = '05' THEN 1 ELSE 0 END) AS time_leave_05_count
            , SUM(CASE WHEN absence_flag_lv = '06' THEN 1 ELSE 0 END) AS time_leave_06_count
            , SUM(CASE WHEN absence_flag_lv = '07' THEN 1 ELSE 0 END) AS time_leave_07_count
            , SUM(CASE WHEN absence_flag_lv = '08' THEN 1 ELSE 0 END) AS time_leave_08_count
            , SUM(CASE WHEN absence_flag_lv = '09' THEN 1 ELSE 0 END) AS time_leave_09_count
            , SUM(CASE WHEN absence_flag_lv = '10' THEN 1 ELSE 0 END) AS time_leave_10_count
            , SUM(CASE WHEN absence_flag_lv = '11' THEN 1 ELSE 0 END) AS time_leave_11_count
            , SUM(CASE WHEN absence_flag_lv = '12' THEN 1 ELSE 0 END) AS time_leave_12_count
            , SUM(CASE WHEN absence_flag_lv = '13' THEN 1 ELSE 0 END) AS time_leave_13_count
            , SUM(CASE WHEN absence_flag_lv = '14' THEN 1 ELSE 0 END) AS time_leave_14_count
            , SUM(CASE WHEN absence_flag_lv = '15' THEN 1 ELSE 0 END) AS time_leave_15_count
            , SUM(CASE WHEN absence_flag_lv = '16' THEN 1 ELSE 0 END) AS time_leave_16_count
            , SUM(CASE WHEN absence_flag_lv = '17' THEN 1 ELSE 0 END) AS time_leave_17_count
            , SUM(CASE WHEN absence_flag_lv = '18' THEN 1 ELSE 0 END) AS time_leave_18_count
            , SUM(CASE WHEN absence_flag_lv = '19' THEN 1 ELSE 0 END) AS time_leave_19_count
            , SUM(CASE WHEN absence_flag_lv = '20' THEN 1 ELSE 0 END) AS time_leave_20_count
            FROM %s.payroll_time_leave
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _time_leave ON (_emp.employee_id = _time_leave.employee_id) 
        LEFT JOIN (
            SELECT employee_id
            , COUNT(*) AS work_cycle_change_count
            , SUM(CASE WHEN approve_flag != '02' THEN 1 ELSE 0 END) AS work_cycle_change_disapprove_count
            FROM %s.payroll_work_cycle_change
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _work_cycle_change ON (_emp.employee_id = _work_cycle_change.employee_id) 
        LEFT JOIN (
            SELECT employee_id
            , COUNT(*) AS withdraw_doc_count
            , SUM(CASE WHEN approve_flag != '02' THEN 1 ELSE 0 END) AS withdraw_doc_disapprove_count
            FROM %s.payroll_employee_withdraw_doc
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _withdraw_doc ON (_emp.employee_id = _withdraw_doc.employee_id) 
        LEFT JOIN (
            SELECT employee_id
            , COUNT(*) AS petty_cash_count
            , SUM(CASE WHEN approve_dt IS NULL THEN 1 ELSE 0 END) AS petty_cash_disapprove_count
            FROM %s.payroll_petty_cash
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _petty_cash ON (_emp.employee_id = _petty_cash.employee_id) 
        LEFT JOIN (
            SELECT employee_id
            , COUNT(*) AS salary_certificate_letter_count
            , SUM(CASE WHEN approve_dt IS NULL THEN 1 ELSE 0 END) AS salary_certificate_letter_disapprove_count
            FROM %s.payroll_salary_certificate_letter
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _salary_certificate_letter ON (_emp.employee_id = _salary_certificate_letter.employee_id) 
        LEFT JOIN (
            SELECT employee_id
            , COUNT(*) AS work_certificate_letter_count
            , SUM(CASE WHEN approve_dt IS NULL THEN 1 ELSE 0 END) AS work_certificate_letter_disapprove_count
            FROM %s.payroll_work_certificate_letter
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _work_certificate_letter ON (_emp.employee_id = _work_certificate_letter.employee_id) 
        LEFT JOIN (
            SELECT employee_id
            , COUNT(*) AS welfare_count
            , SUM(CASE WHEN approve_flag != '02' THEN 1 ELSE 0 END) AS welfare_disapprove_count
            FROM %s.payroll_employee_welfare
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _welfare ON (_emp.employee_id = _welfare.employee_id) 
        LEFT JOIN (
            SELECT informer_employee_id AS employee_id
            , COUNT(*) AS complaint_count
            , 0 AS complaint_disapprove_count
            FROM %s.payroll_employee_complaint
            WHERE instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(created,'%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _complaint ON (_emp.employee_id = _complaint.employee_id)
        WHERE _emp.server_id = '%s'
        AND _emp.instance_server_id = '%s'
        AND _emp.instance_server_channel_id = '%s'
        GROUP BY _emp.branch_id
    ) _summary ON (_branch.branch_id = _summary.branch_id)
    WHERE _branch.server_id = '%s'
    AND _branch.instance_server_id = '%s'
    AND _branch.instance_server_channel_id = '%s'`,
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["server_id"], param["instance_server_id"], param["instance_server_channel_id"],
		param["server_id"], param["instance_server_id"], param["instance_server_channel_id"])

	docsSummary = []DocsStruct{}
	mysqlInstance.SqlList(&docsSummary, query)

	return docsSummary, nil
}
