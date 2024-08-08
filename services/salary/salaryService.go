package salaryService

import (
	"fmt"
	"humansoft/warehouse-consumer/connections/mysql"
)

type SlipReportStruct struct {
	MasterSalaryReportId string `json:"master_salary_report_id" db:"master_salary_report_id"`
	MasterSalaryMonth    string `json:"master_salary_month" db:"master_salary_month"`
}

type SalaryStruct struct {
	BranchId     string  `json:"branch_id" db:"branch_id"`
	BranchCode   string  `json:"branch_code" db:"branch_code"`
	BranchName   string  `json:"branch_name" db:"branch_name"`
	BranchNameEn string  `json:"branch_name_en" db:"branch_name_en"`
	Salary       float64 `json:"salary" db:"salary"`
	NetSalary    float64 `json:"net_salary" db:"net_salary"`
	TotalIncome  float64 `json:"total_income" db:"total_income"`
	TotalExpense float64 `json:"total_expense" db:"total_expense"`
	WithdrawAmt  float64 `json:"withdraw_amt" db:"withdraw_amt"`
}

func GetSummarySalary(param map[string]string) ([]SalaryStruct, error) {
	var slipReport SlipReportStruct
	var salarySummary []SalaryStruct

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
    , IFNULL(_summary.salary, 0) AS salary
    , IFNULL(_summary.net_salary, 0) AS net_salary
    , IFNULL(_summary.total_income, 0) AS total_income
    , IFNULL(_summary.total_expense, 0) AS total_expense
    , IFNULL(_summary.withdraw_amt, 0) AS withdraw_amt
    FROM hms_api.comp_branch _branch
    LEFT JOIN (
        SELECT _slip.branch_id
        , SUM(_slip.salary) AS salary
        , SUM(_slip.total_salary) AS net_salary
        , SUM(_slip.total_income) AS total_income
        , SUM(_slip.total_expense) AS total_expense
        , SUM(IFNULL(_wd.withdraw_amt, 0)) AS withdraw_amt
        FROM %s.payroll_master_salary_slip _slip
        LEFT JOIN %s.payroll_employee_withdraw _wd ON (_slip.master_salary_report_id = _wd.master_salary_report_id AND _slip.employee_id = _wd.employee_id)
        WHERE _slip.server_id = '%s'
        AND _slip.instance_server_id = '%s'
        AND _slip.instance_server_channel_id = '%s'
        AND _slip.master_salary_report_id = '%s'
        GROUP BY _slip.branch_id
    ) _summary ON (_branch.branch_id = _summary.branch_id)
    WHERE _branch.server_id = '%s'
    AND _branch.instance_server_id = '%s'
    AND _branch.instance_server_channel_id = '%s'`, param["dbn"], param["dbn"], param["server_id"], param["instance_server_id"], param["instance_server_channel_id"], slipReport.MasterSalaryReportId, param["server_id"], param["instance_server_id"], param["instance_server_channel_id"])
	salarySummary = []SalaryStruct{}
	mysqlInstance.SqlList(&salarySummary, query)

	return salarySummary, nil
}
