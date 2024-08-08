package main

import (
	"encoding/json"
	"fmt"
	"humansoft/warehouse-consumer/connections/storageaccount"
	docsService "humansoft/warehouse-consumer/services/docs"
	salaryService "humansoft/warehouse-consumer/services/salary"
	timeattendanceService "humansoft/warehouse-consumer/services/timeattendance"
	"humansoft/warehouse-consumer/utils"
)

func main() {
	utils.LoadEnv()

	request := map[string]string{
		"dbn":                          "hms_inst8sp1",
		"server_id":                    "100002",
		"instance_server_id":           "2022040158D8F957BD29",
		"instance_server_channel_id":   "202205072D33E7BA048F",
		"year_month":                   "2024-07",
		"instance_server_code":         "hms",
		"instance_server_channel_code": "hms4",
		"year":                         "2024",
		"month":                        "07",
	}

	runPipeline(request)
}

func runPipeline(request map[string]string) {
	containerName := "hrs-dashboard"
	pathBlob := fmt.Sprintf("%s/%s/%s/%s/", request["instance_server_code"], request["instance_server_channel_code"], request["year"], request["month"])

	blobClient := storageaccount.NewBlob()

	result, _ := salaryService.GetSummarySalary(request)
	// utils.PrintJsonIndent(result, true)
	jsonData, _ := json.MarshalIndent(result, "", "  ")
	blobClient.UploadFile(containerName, pathBlob, "salary.json", jsonData)
	utils.AppendLog("salary", request)

	result2, _ := timeattendanceService.GetSummaryTimeattendance(request)
	// utils.PrintJsonIndent(result2, false)
	jsonData, _ = json.MarshalIndent(result2, "", "  ")
	blobClient.UploadFile(containerName, pathBlob, "timeattendance.json", jsonData)
	utils.AppendLog("timeattendance", request)

	result3, _ := docsService.GetSummaryDocs(request)
	// utils.PrintJsonIndent(result3, false)
	jsonData, _ = json.MarshalIndent(result3, "", "  ")
	blobClient.UploadFile(containerName, pathBlob, "docs.json", jsonData)
	utils.AppendLog("docs", request)
}
