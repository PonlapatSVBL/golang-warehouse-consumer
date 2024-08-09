package timeattendanceService

import (
	"fmt"
	"humansoft/warehouse-consumer/connections/mysql"
)

type SlipReportStruct struct {
	MasterSalaryReportId string `json:"master_salary_report_id" db:"master_salary_report_id"`
	MasterSalaryMonth    string `json:"master_salary_month" db:"master_salary_month"`
	SalaryReportStartDt  string `json:"salary_report_start_dt" db:"salary_report_start_dt"`
	SalaryReportEndDt    string `json:"salary_report_end_dt" db:"salary_report_end_dt"`
}

type TimeattendanceStruct struct {
	BranchId           string `json:"branch_id" db:"branch_id"`
	BranchCode         string `json:"branch_code" db:"branch_code"`
	BranchName         string `json:"branch_name" db:"branch_name"`
	BranchNameEn       string `json:"branch_name_en" db:"branch_name_en"`
	TaFingerprintCount uint32 `json:"ta_fingerprint_count" db:"ta_fingerprint_count"`
	TaManualCount      uint32 `json:"ta_manual_count" db:"ta_manual_count"`
	TaManagerCount     uint32 `json:"ta_manager_count" db:"ta_manager_count"`
	TaOfflineCount     uint32 `json:"ta_offline_count" db:"ta_offline_count"`
	TaBeaconCount      uint32 `json:"ta_beacon_count" db:"ta_beacon_count"`
	TaTimeAdjustCount  uint32 `json:"ta_time_adjust_count" db:"ta_time_adjust_count"`
	TaImportCount      uint32 `json:"ta_import_count" db:"ta_import_count"`
	TaLineCheckinCount uint32 `json:"ta_line_checkin_count" db:"ta_line_checkin_count"`
	TaCheckinCount     uint32 `json:"ta_checkin_count" db:"ta_checkin_count"`
	TaFacialCount      uint32 `json:"ta_facial_count" db:"ta_facial_count"`
	TaWifiCount        uint32 `json:"ta_wifi_count" db:"ta_wifi_count"`
	TaQrCount          uint32 `json:"ta_qr_count" db:"ta_qr_count"`
	TaTimeappCount     uint32 `json:"ta_timeapp_count" db:"ta_timeapp_count"`
	DayTotal           uint32 `json:"day_total" db:"day_total"`
	IncompleteTimes    uint32 `json:"incomplete_times" db:"incomplete_times"`
	MorningTimes       uint32 `json:"morning_times" db:"morning_times"`
	LateTimes          uint32 `json:"late_times" db:"late_times"`
	LunchOverTimes     uint32 `json:"lunch_over_times" db:"lunch_over_times"`
	LunchUnderTimes    uint32 `json:"lunch_under_times" db:"lunch_under_times"`
	EarlyTimes         uint32 `json:"early_times" db:"early_times"`
	AfterTimes         uint32 `json:"after_times" db:"after_times"`
	AbsenceTimes       uint32 `json:"absence_times" db:"absence_times"`
	Absence2Times      uint32 `json:"absence_2_times" db:"absence_2_times"`
	LostTimes          uint32 `json:"lost_times" db:"lost_times"`
	TaTotal            uint32 `json:"ta_total" db:"ta_total"`
}

func GetSummaryTimeattendance(param map[string]interface{}) ([]TimeattendanceStruct, error) {
	var slipReport SlipReportStruct
	var timeattendanceSummary []TimeattendanceStruct

	mysqlInstance := mysql.NewMysql()

	query := fmt.Sprintf(`SELECT master_salary_report_id
	, master_salary_month
    , salary_report_start_dt
    , salary_report_end_dt
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
    , IFNULL(_summary.ta_total, 0) AS ta_total
    , IFNULL(_summary.ta_fingerprint_count, 0) AS ta_fingerprint_count
    , IFNULL(_summary.ta_manual_count, 0) AS ta_manual_count
    , IFNULL(_summary.ta_manager_count, 0) AS ta_manager_count
    , IFNULL(_summary.ta_offline_count, 0) AS ta_offline_count
    , IFNULL(_summary.ta_beacon_count, 0) AS ta_beacon_count
    , IFNULL(_summary.ta_time_adjust_count, 0) AS ta_time_adjust_count
    , IFNULL(_summary.ta_import_count, 0) AS ta_import_count
    , IFNULL(_summary.ta_line_checkin_count, 0) AS ta_line_checkin_count
    , IFNULL(_summary.ta_checkin_count, 0) AS ta_checkin_count
    , IFNULL(_summary.ta_facial_count, 0) AS ta_facial_count
    , IFNULL(_summary.ta_wifi_count, 0) AS ta_wifi_count
    , IFNULL(_summary.ta_qr_count, 0) AS ta_qr_count
    , IFNULL(_summary.ta_timeapp_count, 0) AS ta_timeapp_count
    , IFNULL(_summary.day_total, 0) AS day_total
    , IFNULL(_summary.incomplete_times, 0) AS incomplete_times
    , IFNULL(_summary.morning_times, 0) AS morning_times
    , IFNULL(_summary.late_times, 0) AS late_times
    , IFNULL(_summary.lunch_over_times, 0) AS lunch_over_times
    , IFNULL(_summary.lunch_under_times, 0) AS lunch_under_times
    , IFNULL(_summary.early_times, 0) AS early_times
    , IFNULL(_summary.after_times, 0) AS after_times
    , IFNULL(_summary.absence_times, 0) AS absence_times
    , IFNULL(_summary.absence_2_times, 0) AS absence_2_times
    , IFNULL(_summary.lost_times, 0) AS lost_times
    FROM hms_api.comp_branch _branch
    LEFT JOIN (
        SELECT _emp.branch_id
        , SUM(_ta.ta_total) AS ta_total
        , SUM(_ta.ta_fingerprint_count) AS ta_fingerprint_count
        , SUM(_ta.ta_manual_count) AS ta_manual_count
        , SUM(_ta.ta_manager_count) AS ta_manager_count
        , SUM(_ta.ta_offline_count) AS ta_offline_count
        , SUM(_ta.ta_beacon_count) AS ta_beacon_count
        , SUM(_ta.ta_time_adjust_count) AS ta_time_adjust_count
        , SUM(_ta.ta_import_count) AS ta_import_count
        , SUM(_ta.ta_line_checkin_count) AS ta_line_checkin_count
        , SUM(_ta.ta_checkin_count) AS ta_checkin_count
        , SUM(_ta.ta_facial_count) AS ta_facial_count
        , SUM(_ta.ta_wifi_count) AS ta_wifi_count
        , SUM(_ta.ta_qr_count) AS ta_qr_count
        , SUM(_ta.ta_timeapp_count) AS ta_timeapp_count
        , SUM(_gt.day_total) AS day_total
        , SUM(_gt.incomplete_times) AS incomplete_times
        , SUM(_gt.morning_times) AS morning_times
        , SUM(_gt.late_times) AS late_times
        , SUM(_gt.lunch_over_times) AS lunch_over_times
        , SUM(_gt.lunch_under_times) AS lunch_under_times
        , SUM(_gt.early_times) AS early_times
        , SUM(_gt.after_times) AS after_times
        , SUM(_gt.absence_times) AS absence_times
        , SUM(_gt.absence_2_times) AS absence_2_times
        , SUM(_gt.lost_times) AS lost_times
        FROM hms_api.comp_employee _emp
        LEFT JOIN (
            SELECT employee_id
            , COUNT(time_attendance_transac_id) AS ta_total
            , SUM(CASE WHEN time_attendance_type_lv='Fingerprint' THEN 1 ELSE 0 END) AS ta_fingerprint_count
            , SUM(CASE WHEN time_attendance_type_lv='Manual' THEN 1 ELSE 0 END) AS ta_manual_count
            , SUM(CASE WHEN time_attendance_type_lv='Manager' THEN 1 ELSE 0 END) AS ta_manager_count
            , SUM(CASE WHEN time_attendance_type_lv='Offline' THEN 1 ELSE 0 END) AS ta_offline_count
            , SUM(CASE WHEN time_attendance_type_lv='Beacon' THEN 1 ELSE 0 END) AS ta_beacon_count
            , SUM(CASE WHEN time_attendance_type_lv='Time Adjust' THEN 1 ELSE 0 END) AS ta_time_adjust_count
            , SUM(CASE WHEN time_attendance_type_lv='Import' THEN 1 ELSE 0 END) AS ta_import_count
            , SUM(CASE WHEN time_attendance_type_lv='LINE-Checkin' THEN 1 ELSE 0 END) AS ta_line_checkin_count
            , SUM(CASE WHEN time_attendance_type_lv='Checkin' THEN 1 ELSE 0 END) AS ta_checkin_count
            , SUM(CASE WHEN time_attendance_type_lv='Facial' THEN 1 ELSE 0 END) AS ta_facial_count
            , SUM(CASE WHEN time_attendance_type_lv='Wifi' THEN 1 ELSE 0 END) AS ta_wifi_count
            , SUM(CASE WHEN time_attendance_type_lv='QR' THEN 1 ELSE 0 END) AS ta_qr_count
            , SUM(CASE WHEN time_attendance_type_lv='TimeApp' THEN 1 ELSE 0 END) AS ta_timeapp_count
            FROM %s.payroll_time_attendance_transac
            WHERE server_id = '%s'
            AND instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(attendance_date, '%%Y-%%m') = '%s'
            GROUP BY employee_id
        ) _ta ON (_emp.employee_id = _ta.employee_id)
        LEFT JOIN (
            SELECT employee_id
            , COUNT(time_attendance_group_transac_id) AS day_total
            , SUM(CASE WHEN work_min_time = work_max_time AND work_time_count > 0 THEN 1 ELSE 0 END) AS incomplete_times
            , SUM(CASE WHEN morning_flag_lv != '00' THEN 1 ELSE 0 END) AS morning_times
            , SUM(CASE WHEN late_flag_lv != '00' THEN 1 ELSE 0 END) AS late_times
            , SUM(CASE WHEN lunch_over_flag_lv != '00' THEN 1 ELSE 0 END) AS lunch_over_times
            , SUM(CASE WHEN lunch_under_flag_lv != '00' THEN 1 ELSE 0 END) AS lunch_under_times
            , SUM(CASE WHEN early_flag_lv != '00' THEN 1 ELSE 0 END) AS early_times
            , SUM(CASE WHEN after_flag_lv != '00' THEN 1 ELSE 0 END) AS after_times
            , SUM(CASE WHEN absence_flag_lv != '00' THEN 1 ELSE 0 END) AS absence_times
            , SUM(CASE WHEN absence_2_flag_lv != '00' THEN 1 ELSE 0 END) AS absence_2_times
            , SUM(CASE WHEN lost_flag_lv != '00' THEN 1 ELSE 0 END) AS lost_times
            FROM %s.payroll_time_attendance_group_transac
            WHERE server_id = '%s'
            AND instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND master_salary_report_id = '%s'
            GROUP BY employee_id
        ) _gt ON (_emp.employee_id = _gt.employee_id)
        WHERE _emp.server_id = '%s'
        AND _emp.instance_server_id = '%s'
        AND _emp.instance_server_channel_id = '%s'
        GROUP BY _emp.branch_id
    ) _summary ON (_branch.branch_id = _summary.branch_id)
    WHERE _branch.server_id = '%s'
    AND _branch.instance_server_id = '%s'
    AND _branch.instance_server_channel_id = '%s'`,
		param["dbn"], param["server_id"], param["instance_server_id"], param["instance_server_channel_id"], param["year_month"],
		param["dbn"], param["server_id"], param["instance_server_id"], param["instance_server_channel_id"], slipReport.MasterSalaryReportId,
		param["server_id"], param["instance_server_id"], param["instance_server_channel_id"],
		param["server_id"], param["instance_server_id"], param["instance_server_channel_id"])

	timeattendanceSummary = []TimeattendanceStruct{}
	mysqlInstance.SqlList(&timeattendanceSummary, query)

	return timeattendanceSummary, nil
}
