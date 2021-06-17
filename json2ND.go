package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

func processJSONFile(inputFile string, writerChannel chan<- map[string]interface{}, isJSONArray bool) {
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("error opening inputfile, err: %s", err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	if isJSONArray {
		var input []map[string]interface{}
		err := dec.Decode(&input)
		if err != nil {
			log.Fatalf("error decoding json array, %s", err)
		}
		for _, val := range input {
			writerChannel <- val
		}
		close(writerChannel)
	}
	for {
		var val map[string]interface{}
		if err := dec.Decode(&val); err == io.EOF {
			close(writerChannel)
			break
		} else if err != nil {
			log.Fatalf("error decoding json, %s", err)
		}

		writerChannel <- val
	}
}

func writeNDJSONFile(inputFile string, writerChannel <-chan map[string]interface{}, done chan<- bool) {
	writeString := createStringWriter(inputFile)
	jsonFunc := func(record map[string]interface{}) string {
		jsonData, _ := json.Marshal(record)
		return string(jsonData)
	}

	fmt.Println("Writing ND JSON file...")
	for {
		record, more := <-writerChannel
		if more {
			jsonData := jsonFunc(record)
			writeString(jsonData+"\n", false)
		} else {
			fmt.Println("Completed!")
			writeString("", true)
			done <- true
			break
		}
	}

}

func createStringWriter(outputPath string) func(string, bool) {
	jsonDir := filepath.Dir(outputPath)
	jsonName := fmt.Sprintf("nd_%s", filepath.Base(outputPath))
	finalLocation := fmt.Sprintf("%s/%s", jsonDir, jsonName)

	f, err := os.Create(finalLocation)
	if err != nil {
		log.Fatalf("error creating new nd json file, err: %s", err)
	}

	return func(data string, close bool) {
		if close {
			f.Close()
			return
		}

		_, err := f.WriteString(data)
		if err != nil {
			log.Fatalf("error writing string, err: %s", err)
		}
	}
}

func main() {
	filePath := flag.String("file", "", "File location or name")
	isJSONArray := flag.Bool("array", true, "If the input file is an array of json objects")
	flag.Parse() // This will parse all the arguments from the terminal

	if fileExt := filepath.Ext(*filePath); fileExt != ".json" {
		log.Fatalf("input file needs to be of extension json, err: %s", fileExt)
	}

	if _, err := os.Stat(*filePath); err != nil && os.IsNotExist(err) {
		log.Fatalf("error opening file, err: %s", err)
	}

	writerChannel := make(chan map[string]interface{})
	done := make(chan bool)

	go processJSONFile(*filePath, writerChannel, *isJSONArray)
	go writeNDJSONFile(*filePath, writerChannel, done)

	<-done
}
