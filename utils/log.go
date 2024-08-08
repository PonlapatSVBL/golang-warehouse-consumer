package utils

import (
	"fmt"
	"log"
	"os"
)

func AppendLog(fileName string, param map[string]string) {
	// สร้างชื่อไฟล์ที่มีวันที่และเดือน
	fileName = fmt.Sprintf("%s-%s.log", fileName, param["year_month"])

	// ระบุพาธของโฟลเดอร์ log/
	logDir := "log/"

	// สร้างโฟลเดอร์ log ถ้ายังไม่มีอยู่
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("error creating directory: %v", err)
	}

	// รวมโฟลเดอร์กับชื่อไฟล์
	filePath := logDir + fileName

	// สร้างข้อความ log
	logDetail := fmt.Sprintf("%s %s (%s, %s)", param["instance_server_code"], param["instance_server_channel_code"], param["instance_server_id"], param["instance_server_channel_id"])

	// เปิดไฟล์ log เพื่อเพิ่มข้อมูลใหม่
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer file.Close()

	// สร้าง logger ที่เขียนไปยังไฟล์
	logger := log.New(file, "LOGGING: ", log.Ldate|log.Ltime|log.Lshortfile)

	// เขียนข้อความ log
	logger.Println(logDetail)
}
