package main

import (
	"io"
	"os"
	"testing"
)

func TestParse(t *testing.T) {
	stations := make(map[string]WeatherData)

	file, err := os.Open("measurements_100.txt")
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()

	file_bytes, err := io.ReadAll(file)
	if err != nil {
		panic(err.Error())
	}

	ParseFile(stations, file_bytes)
}
