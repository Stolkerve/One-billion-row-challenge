package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"time"
	"unsafe"
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

const FileBufferSize = 1024 * 1024 * 10

type WeatherData struct {
	Min   float32
	Mean  float32
	Max   float32
	Count uint64
}

func BytesToString(b []byte) string {
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}

func ReadFileByChuncks(file *os.File, fileChuncksChannel chan []byte, done chan struct{}) {
	stat, err := file.Stat()
	if err != nil {
		log.Panic(err.Error())
	}
	fileSize := stat.Size()
	offset := 0
	fileChunk := make([]byte, FileBufferSize)

	xd := math.Round(float64(fileSize) / float64(FileBufferSize))

	for x := 0; x < int(xd); x++ {
		lastEndLinePos := 0
		n, err := file.ReadAt(fileChunk, int64(offset))
		// fmt.Printf("Total bytes read: %d, Bytes readed: %d\n", n+offset, n)
		if err != nil {
			if err != io.EOF {
				log.Panic(err.Error())
			}
		}

		e := n - 1
		for {
			b := fileChunk[e]
			if b == '\n' {
				lastEndLinePos = e + 1
				break
			}
			e--
		}

		b := make([]byte, lastEndLinePos)
		copy(b, fileChunk)
		fileChuncksChannel <- b

		offset += lastEndLinePos
	}
	done <- struct{}{}
}

func ParseFile(stations map[string]WeatherData, fileChunk []byte) {
	startWord := 0
	startNumber := 0
	word := ""
	for i := 0; i < len(fileChunk); i++ {
		b := fileChunk[i]
		if b == ';' {
			word = BytesToString(fileChunk[startWord:i])
			startNumber = i + 1
		}
		if b == '\n' {
			startWord = i + 1
			number_str := BytesToString(fileChunk[startNumber:i])
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
}

func Calculate() {
	file, err := os.Open("measurements.txt")
	if err != nil {
		log.Panic(err.Error())
	}
	defer file.Close()

	stations := make(map[string]WeatherData)
	fileChuncksChannel := make(chan []byte, 10)
	fileChuncksDoneChannel := make(chan struct{})

	go ReadFileByChuncks(file, fileChuncksChannel, fileChuncksDoneChannel)

L:
	for {
		select {
		case chunk := <-fileChuncksChannel:
			ParseFile(stations, chunk)
		case <-fileChuncksDoneChannel:
			break L
		}
	}

	stationsNum := len(stations) - 1
	i := 0
	var outBuf bytes.Buffer
	// outBuf.Grow(2 + (4 * 3 * stations_num) + (6 * stations_num))

	keys := make([]string, 0, len(stations))
	for k := range stations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	outBuf.WriteString("{")
	for _, k := range keys {
		v := stations[k]
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
	if len(os.Args) > 1 {
		if os.Args[1] == "--debug" {
			fmt.Println("Iniciando debug...")
			f, err := os.Create("profile.prof")
			if err != nil {
				panic(err)
			}
			defer f.Close()

			if err := pprof.StartCPUProfile(f); err != nil {
				panic(err)
			}
			defer pprof.StopCPUProfile()
		}
	}
	start := time.Now()
	Calculate()
	fmt.Printf("Took: %v", time.Since(start))
}
