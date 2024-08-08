package main

import (
	"encoding/json"
	"fmt"
	"humansoft/warehouse-consumer/connections/mysql"
	"humansoft/warehouse-consumer/connections/storageaccount"
	docsService "humansoft/warehouse-consumer/services/docs"
	salaryService "humansoft/warehouse-consumer/services/salary"
	sysinstanceserverchannelService "humansoft/warehouse-consumer/services/sysinstanceserverchannel"
	timeattendanceService "humansoft/warehouse-consumer/services/timeattendance"
	"humansoft/warehouse-consumer/utils"
	"sync"
)

func main() {
	utils.LoadEnv()

	mysql.NewMysql()
	storageaccount.NewBlob()

	years := []string{"2024"}
	months := []string{"07", "08"}

	channels, _ := sysinstanceserverchannelService.GetActiveChannel()
	for _, ch := range channels {
		for _, year := range years {
			for _, month := range months {
				request2 := map[string]interface{}{
					"dbn":                          ch.InstanceServerDbn,
					"server_id":                    ch.ServerId,
					"instance_server_id":           ch.InstanceServerId,
					"instance_server_channel_id":   ch.InstanceServerChannelId,
					"year_month":                   year + "-" + month,
					"instance_server_code":         ch.InstanceServerCode,
					"instance_server_channel_code": ch.InstanceServerChannelCode,
					"year":                         year,
					"month":                        month,
				}
				utils.PrintJsonIndent(request2, false)
				runPipeline(request2)
			}
		}
	}
}

func runPipeline(request map[string]interface{}) {
	containerName := "hrs-dashboard"
	pathBlob := fmt.Sprintf("%s/%s/%s/%s/", request["instance_server_code"], request["instance_server_channel_code"], request["year"], request["month"])

	blobClient := storageaccount.NewBlob()

	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		defer wg.Done()

		result, _ := salaryService.GetSummarySalary(request)
		// utils.PrintJsonIndent(result, true)
		jsonData, _ := json.MarshalIndent(result, "", "  ")
		blobClient.UploadFile(containerName, pathBlob, "salary.json", jsonData)
		utils.AppendLog("salary", request)
	}()

	go func() {
		defer wg.Done()

		result2, _ := timeattendanceService.GetSummaryTimeattendance(request)
		// utils.PrintJsonIndent(result2, false)
		jsonData, _ := json.MarshalIndent(result2, "", "  ")
		blobClient.UploadFile(containerName, pathBlob, "timeattendance.json", jsonData)
		utils.AppendLog("timeattendance", request)
	}()

	go func() {
		defer wg.Done()

		result3, _ := docsService.GetSummaryDocs(request)
		// utils.PrintJsonIndent(result3, false)
		jsonData, _ := json.MarshalIndent(result3, "", "  ")
		blobClient.UploadFile(containerName, pathBlob, "docs.json", jsonData)
		utils.AppendLog("docs", request)
	}()

	wg.Wait()
}
