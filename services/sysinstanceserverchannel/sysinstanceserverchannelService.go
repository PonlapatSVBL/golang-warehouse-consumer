package sysinstanceserverchannelService

import (
	"humansoft/warehouse-consumer/connections/mysql"
)

type InstanceServerChannelStruct struct {
	ServerId                  string `json:"server_id" db:"server_id"`
	InstanceServerDbn         string `json:"instance_server_dbn" db:"instance_server_dbn"`
	InstanceServerId          string `json:"instance_server_id" db:"instance_server_id"`
	InstanceServerCode        string `json:"instance_server_code" db:"instance_server_code"`
	InstanceServerChannelId   string `json:"instance_server_channel_id" db:"instance_server_channel_id"`
	InstanceServerChannelCode string `json:"instance_server_channel_code" db:"instance_server_channel_code"`
}

func GetActiveChannel() ([]InstanceServerChannelStruct, error) {
	var channels []InstanceServerChannelStruct

	mysqlInstance := mysql.NewMysql()

	query := `SELECT _sv.server_id
	, _sv.instance_server_dbn
	, _sv.instance_server_id
	, _sv.instance_server_code
	, _ch.instance_server_channel_id
	, _ch.instance_server_channel_code
	FROM hms_api.sys_instance_server_channel _ch
	INNER JOIN hms_api.sys_instance_server _sv ON (_ch.instance_server_id = _sv.instance_server_id)
	WHERE _sv.active_status = 'Active'
	AND _ch.sys_del_flag = 'N'
	AND _sv.instance_server_code = 'hms'
	ORDER BY _sv.instance_server_dbn`
	channels = []InstanceServerChannelStruct{}
	mysqlInstance.SqlList(&channels, query)

	return channels, nil
}
