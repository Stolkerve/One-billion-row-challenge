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
https://stackoverflow.com/questions/7021725/how-to-convert-a-string-to-integer-in-c
int atoi(const char* str){
    int num = 0;
    int i = 0;
    bool isNegetive = false;
    if(str[i] == '-'){
        isNegetive = true;
        i++;
    }
    while (str[i] && (str[i] >= '0' && str[i] <= '9')){
        num = num * 10 + (str[i] - '0');
        i++;
    }
    if(isNegetive) num = -1 * num;
    return num;
}
*/

const FileBufferSize = 1024 * 1024 * 10

type WeatherData struct {
	Min   int64
	Sum   int64
	Max   int64
	Count uint64
}

func BytesToString(b []byte) string {
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}

func Atoi(str []byte) int {
	num := 0
	i := 0
	isNegetive := false
	if str[i] == '-' {
		isNegetive = true
		i++
	}

	for str[i] >= '0' && str[i] <= '9' && i < len(str) {
		num = num*10 + int(str[i]-byte('0'))
		i++
	}
	if isNegetive {
		num = -1 * num
	}
	return num
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
				panic(err.Error())
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
	number_buf := make([]byte, 6)
	for i := 0; i < len(fileChunk); i++ {
		b := fileChunk[i]
		if b == ';' {
			word = BytesToString(fileChunk[startWord:i])
			startNumber = i + 1
		}
		if b == '\n' {
			startWord = i + 1
			number_str := fileChunk[startNumber:i]
			cursor := 0
			count := 0
			if number_str[cursor] == '-' {
				number_buf[0] = '-'
				cursor += 1
				count += 1
			}
			for cursor < len(number_str) {
				if number_str[cursor] != '.' {
					number_buf[count] = number_str[cursor]
					count += 1
				}
				cursor++
			}
			number, err := strconv.ParseInt(BytesToString(number_buf[:count]), 10, 32)
			if err != nil {
				log.Panicln(err)
			}

			if data, ok := stations[word]; ok {
				if data.Max < number {
					data.Max = number
				}
				if data.Min > number {
					data.Min = number
				}
				data.Count += 1
				data.Sum += number
				stations[word] = data
				continue
			}
			stations[word] = WeatherData{
				Min:   number,
				Sum:   number,
				Max:   number,
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
			outBuf.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", k, float32(v.Min)/10.0, (float32(v.Sum)/10)/float32(v.Count), float32(v.Max)/10))
			continue
		}
		outBuf.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f,", k, float32(v.Min)/10.0, (float32(v.Sum)/10)/float32(v.Count), float32(v.Max)/10))
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
				log.Panic(err)
			}
			defer f.Close()

			if err := pprof.StartCPUProfile(f); err != nil {
				log.Panic(err)
			}
			defer pprof.StopCPUProfile()
		}
	}
	start := time.Now()
	Calculate()
	fmt.Printf("Took: %v", time.Since(start))
}
