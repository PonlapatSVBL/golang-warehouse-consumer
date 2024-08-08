package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
)

func PrintJson(data interface{}, openLog bool) {
	// แปลง slice ของ employees เป็น JSON
	dataJson, err := json.Marshal(data)
	if err != nil {
		log.Println("Error marshalling employees to JSON:", err)
	}

	// พิมพ์ JSON ออกมา
	if openLog {
		log.Println(string(dataJson))
	} else {
		fmt.Println(string(dataJson))
	}
}

func PrintJsonIndent(data interface{}, openLog bool) {
	// แปลง slice ของ employees เป็น JSON
	dataJson, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Println("Error marshalling employees to JSON:", err)
	}

	// พิมพ์ JSON ออกมา
	if openLog {
		log.Println(">>>")
	}
	fmt.Println(string(dataJson))
}

func PrintExistJson(data interface{}, openLog bool) {
	// ใช้ reflection เพื่อตรวจสอบ key ที่มีค่าว่างและลบออก
	value := reflect.ValueOf(data)
	if value.Kind() != reflect.Slice {
		log.Println("Input data must be a slice")
		return
	}

	// สร้าง slice ใหม่เพื่อเก็บข้อมูลที่มีค่าเท่ากับศูนย์
	var filteredSlice []interface{}
	for i := 0; i < value.Len(); i++ {
		itemValue := value.Index(i)
		if itemValue.Kind() == reflect.Struct {
			newItem := make(map[string]interface{})
			for j := 0; j < itemValue.NumField(); j++ {
				field := itemValue.Type().Field(j).Name
				fieldValue := itemValue.Field(j)
				if fieldValue.Kind() == reflect.String && fieldValue.String() == "" {
					continue
				}
				newItem[field] = fieldValue.Interface()
			}
			filteredSlice = append(filteredSlice, newItem)
		} else {
			log.Println("Non-struct item found in slice")
			continue
		}
	}

	// แปลง slice ใหม่เป็น JSON
	dataJson, err := json.Marshal(filteredSlice)
	if err != nil {
		log.Println("Error marshalling data to JSON:", err)
		return
	}

	// พิมพ์ JSON ออกมา
	if openLog {
		log.Println(string(dataJson))
	} else {
		fmt.Println(string(dataJson))
	}
}

func PrintExistJsonIndent(data interface{}, openLog bool) {
	// ใช้ reflection เพื่อตรวจสอบ key ที่มีค่าว่างและลบออก
	value := reflect.ValueOf(data)
	if value.Kind() != reflect.Slice {
		log.Println("Input data must be a slice")
		return
	}

	// สร้าง slice ใหม่เพื่อเก็บข้อมูลที่มีค่าเท่ากับศูนย์
	var filteredSlice []interface{}
	for i := 0; i < value.Len(); i++ {
		itemValue := value.Index(i)
		if itemValue.Kind() == reflect.Struct {
			newItem := make(map[string]interface{})
			for j := 0; j < itemValue.NumField(); j++ {
				field := itemValue.Type().Field(j).Name
				fieldValue := itemValue.Field(j)
				if fieldValue.Kind() == reflect.String && fieldValue.String() == "" {
					continue
				}
				newItem[field] = fieldValue.Interface()
			}
			filteredSlice = append(filteredSlice, newItem)
		} else {
			log.Println("Non-struct item found in slice")
			continue
		}
	}

	// แปลง slice ใหม่เป็น JSON
	dataJson, err := json.MarshalIndent(filteredSlice, "", "  ")
	if err != nil {
		log.Println("Error marshalling data to JSON:", err)
		return
	}

	// พิมพ์ JSON ออกมา
	if openLog {
		log.Println(">>>")
	}
	fmt.Println(string(dataJson))
}
