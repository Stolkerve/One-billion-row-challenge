package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

/*
float stof(const char* s){
  float rez = 0, fact = 1;
  if (*s == '-'){
    s++;
    fact = -1;
  };
  for (int point_seen = 0; *s; s++){
    if (*s == '.'){
      point_seen = 1;
      continue;
    };
    int d = *s - '0';
    if (d >= 0 && d <= 9){
      if (point_seen) fact /= 10.0f;
      rez = rez * 10.0f + (float)d;
    };
  };
  return rez * fact;
};
*/

type WeatherData struct {
	Min   float32
	Mean  float32
	Max   float32
	Count uint64
}

const FileBufferSize = 1024 * 1024 * 10

func Calculate() {
	file, err := os.Open("measurements.txt")
	if err != nil {
		log.Panic(err.Error())
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		log.Panic(err.Error())
	}

	fileSize := stat.Size()
	offset := 0
	stations := make(map[string]WeatherData)
	file_bytes := make([]byte, FileBufferSize)

	xd := math.Round(float64(fileSize) / float64(FileBufferSize))
	for x := 0; x < int(xd); x++ {
		startWord := 0
		startNumber := 0
		lastEndLinePos := 0
		word := ""
		n, err := file.ReadAt(file_bytes, int64(offset))
		// fmt.Printf("Total bytes read: %d, Bytes readed: %d\n", n+offset, n)
		if err != nil {
			if err != io.EOF {
				log.Panic(err.Error())
			}
		}

		e := n - 1
		for {
			b := file_bytes[e]
			if b == '\n' {
				lastEndLinePos = e + 1
				break
			}
			e--
		}

		for i := 0; i < lastEndLinePos; i++ {
			b := file_bytes[i]
			if b == ';' {
				word = string(file_bytes[startWord:i])
				startNumber = i + 1
			}
			if b == '\n' {
				startWord = i + 1
				number_str := string(file_bytes[startNumber:i])
				number64, err := strconv.ParseFloat(number_str, 32)
				number32 := float32(number64)
				if err != nil {
					fmt.Println(number_str, i, startNumber)
					log.Panic(err.Error())
				}

				if data, ok := stations[word]; ok {
					if data.Max < number32 {
						data.Max = number32
					}
					if data.Min > number32 {
						data.Min = number32
					}
					data.Count += 1
					data.Mean += number32 / float32(data.Count)
					stations[word] = data
					continue
				}
				stations[word] = WeatherData{
					Min:   number32,
					Mean:  number32,
					Max:   number32,
					Count: 1,
				}
			}
		}
		offset += lastEndLinePos
	}

	fmt.Println(fileSize)

	stationsNum := len(stations) - 1
	i := 0
	var outBuf bytes.Buffer
	// outBuf.Grow(2 + (4 * 3 * stations_num) + (6 * stations_num))

	outBuf.WriteString("{")
	for k, v := range stations {
		if stationsNum == i {
			outBuf.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", k, v.Min, v.Mean, v.Max))
			continue
		}
		outBuf.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f,", k, v.Min, v.Mean, v.Max))
		i += 1
	}
	outBuf.WriteString("}")
	fmt.Println(outBuf.String())
}

func main() {
	start := time.Now()
	Calculate()
	fmt.Printf("Took: %v", time.Since(start))
}
