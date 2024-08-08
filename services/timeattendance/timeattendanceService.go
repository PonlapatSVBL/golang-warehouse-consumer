package timeattendanceService

import (
	"fmt"
	"humansoft/warehouse-consumer/connections/mysql"
)

type SlipReportStruct struct {
	MasterSalaryReportId string `json:"master_salary_report_id" db:"master_salary_report_id"`
	MasterSalaryMonth    string `json:"master_salary_month" db:"master_salary_month"`
}

type TimeattendanceStruct struct {
	BranchId           string `json:"branch_id" db:"branch_id"`
	BranchCode         string `json:"branch_code" db:"branch_code"`
	BranchName         string `json:"branch_name" db:"branch_name"`
	BranchNameEn       string `json:"branch_name_en" db:"branch_name_en"`
	TaFingerprintCount int16  `json:"ta_fingerprint_count" db:"ta_fingerprint_count"`
	TaManualCount      int16  `json:"ta_manual_count" db:"ta_manual_count"`
	TaManagerCount     int16  `json:"ta_manager_count" db:"ta_manager_count"`
	TaOfflineCount     int16  `json:"ta_offline_count" db:"ta_offline_count"`
	TaBeaconCount      int16  `json:"ta_beacon_count" db:"ta_beacon_count"`
	TaTimeAdjustCount  int16  `json:"ta_time_adjust_count" db:"ta_time_adjust_count"`
	TaImportCount      int16  `json:"ta_import_count" db:"ta_import_count"`
	TaLineCheckinCount int16  `json:"ta_line_checkin_count" db:"ta_line_checkin_count"`
	TaCheckinCount     int16  `json:"ta_checkin_count" db:"ta_checkin_count"`
	TaFacialCount      int16  `json:"ta_facial_count" db:"ta_facial_count"`
	TaWifiCount        int16  `json:"ta_wifi_count" db:"ta_wifi_count"`
	TaQrCount          int16  `json:"ta_qr_count" db:"ta_qr_count"`
	TaTimeappCount     int16  `json:"ta_timeapp_count" db:"ta_timeapp_count"`
	TotalTimes         int16  `json:"total_times" db:"total_times"`
	IncompleteTimes    int16  `json:"incomplete_times" db:"incomplete_times"`
	MorningTimes       int16  `json:"morning_times" db:"morning_times"`
	LateTimes          int16  `json:"late_times" db:"late_times"`
	LunchOverTimes     int16  `json:"lunch_over_times" db:"lunch_over_times"`
	LunchUnderTimes    int16  `json:"lunch_under_times" db:"lunch_under_times"`
	EarlyTimes         int16  `json:"early_times" db:"early_times"`
	AfterTimes         int16  `json:"after_times" db:"after_times"`
	AbsenceTimes       int16  `json:"absence_times" db:"absence_times"`
	Absence2Times      int16  `json:"absence_2_times" db:"absence_2_times"`
	LostTimes          int16  `json:"lost_times" db:"lost_times"`
}

func GetSummaryTimeattendance(param map[string]interface{}) ([]TimeattendanceStruct, error) {
	var slipReport SlipReportStruct
	var timeattendanceSummary []TimeattendanceStruct

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
    , IFNULL(_summary.total_times, 0) AS total_times
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
        , SUM(CASE WHEN _ta.time_attendance_type_lv='Fingerprint' THEN 1 ELSE 0 END) AS ta_fingerprint_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='Manual' THEN 1 ELSE 0 END) AS ta_manual_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='Manager' THEN 1 ELSE 0 END) AS ta_manager_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='Offline' THEN 1 ELSE 0 END) AS ta_offline_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='Beacon' THEN 1 ELSE 0 END) AS ta_beacon_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='Time Adjust' THEN 1 ELSE 0 END) AS ta_time_adjust_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='Import' THEN 1 ELSE 0 END) AS ta_import_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='LINE-Checkin' THEN 1 ELSE 0 END) AS ta_line_checkin_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='Checkin' THEN 1 ELSE 0 END) AS ta_checkin_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='Facial' THEN 1 ELSE 0 END) AS ta_facial_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='Wifi' THEN 1 ELSE 0 END) AS ta_wifi_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='QR' THEN 1 ELSE 0 END) AS ta_qr_count 
        , SUM(CASE WHEN _ta.time_attendance_type_lv='TimeApp' THEN 1 ELSE 0 END) AS ta_timeapp_count
        , SUM(_gt.total_times) AS total_times
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
        INNER JOIN (
            SELECT employee_id
            , attendance_date
            , time_attendance_type_lv
            FROM %s.payroll_time_attendance_transac
            WHERE server_id = '%s'
            AND instance_server_id = '%s'
            AND instance_server_channel_id = '%s'
            AND DATE_FORMAT(attendance_date, '%%Y-%%m') = '%s'
        ) _ta ON (_emp.employee_id = _ta.employee_id)
        INNER JOIN (
            SELECT employee_id
            , COUNT(time_attendance_group_transac_id) AS total_times
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
