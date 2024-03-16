package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const FileBufferSize = 1024 * 1024 * 10
const NumBuckets = 1 << 17 // number of hash buckets (power of 2)
const (
	// FNV-1 64-bit constants from hash/fnv.
	Offset64 = 14695981039346656037
	Prime64  = 1099511628211
)

type WeatherData struct {
	Min   int64
	Sum   int64
	Max   int64
	Count uint64
}

type Weather struct {
	WeatherData
	Name string
	Set  bool
}

// idea sacada de https://github.com/benhoyt/go-1brc/blob/master/r7.go
type HashMap struct {
	Buckets []Weather
	Size    atomic.Int64
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

func ParseFile(stations *HashMap, fileChunk []byte) {
	startWord := 0
	word := ""
	number := int64(0)
	for i := 0; i < len(fileChunk); i++ {
		hash := uint64(Offset64)
		for {
			if fileChunk[i] == ';' {
				word = BytesToString(fileChunk[startWord:i])
				i++
				break
			}
			hash ^= uint64(fileChunk[i]) // FNV-1a is XOR then *
			hash *= Prime64
			i++
		}
		var n uint64
		neg := false
		for {
			if fileChunk[i] == '\n' {
				startWord = i + 1
				break
			}
			if fileChunk[i] == '-' {
				neg = true
			}
			if fileChunk[i] != '.' {
				c := fileChunk[i]
				var d byte
				switch {
				case '0' <= c && c <= '9':
					d = c - '0'
				}

				n *= uint64(10)
				n1 := n + uint64(d)
				n = n1
			}
			i++
		}
		number = int64(n)
		if neg {
			number = -number
		}
		hashIndex := int(hash & uint64(NumBuckets-1))
		for {
			if !stations.Buckets[hashIndex].Set {
				stations.Buckets[hashIndex].Set = true
				stations.Buckets[hashIndex].Name = word
				stations.Buckets[hashIndex].WeatherData = WeatherData{
					Min:   number,
					Sum:   number,
					Max:   number,
					Count: 1,
				}
				stations.Size.Add(1)
				break
			}
			if stations.Buckets[hashIndex].Name == word {
				data := &stations.Buckets[hashIndex]
				data.Min = min(data.Min, number)
				data.Max = max(data.Max, number)
				data.Count += 1
				data.Sum += number
				break
			}

			hashIndex++
			if hashIndex >= NumBuckets {
				hashIndex = 0
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

	weatherStations := HashMap{
		Buckets: make([]Weather, NumBuckets),
	}

	fileChuncksChannel := make(chan []byte, 10)
	fileChuncksDoneChannel := make(chan struct{})

	go ReadFileByChuncks(file, fileChuncksChannel, fileChuncksDoneChannel)

	var numWorkers = runtime.NumCPU()
	jobs := make(chan []byte, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		go (func() {
			for {
				ParseFile(&weatherStations, <-jobs)
				wg.Done()
			}
		})()
	}
L:
	for {
		select {
		case chunk := <-fileChuncksChannel:
			jobs <- chunk
			wg.Add(1)
		case <-fileChuncksDoneChannel:
			break L
		}
	}
	wg.Wait()

	i := 0
	var outBuf bytes.Buffer
	outBuf.Grow(1 << 10)

	weatherRecords := make([]Weather, 0, weatherStations.Size.Load())
	for _, record := range weatherStations.Buckets {
		if !record.Set {
			continue
		}
		weatherRecords = append(weatherRecords, record)
	}
	stationsNum := len(weatherRecords) - 1
	sort.Slice(weatherRecords, func(i, j int) bool {
		// fmt.Println()
		return weatherRecords[i].Name < weatherRecords[j].Name
	})

	outBuf.WriteString("{")
	for _, record := range weatherRecords {
		outBuf.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", record.Name, float64(record.Min)/10, float64(record.Sum)/float64(record.Count)/10, float64(record.Max)/10))
		if stationsNum != i {
			outBuf.WriteString(",")
		}
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
