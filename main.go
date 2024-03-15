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
	"sync"
	"time"
	"unsafe"

	"github.com/alphadose/haxmap"
)

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

func ParseUint(s []byte) uint64 {
	// Cutoff is the smallest number such that cutoff*base > maxUint64.
	// Use compile-time constants for common cases.

	var n uint64
	for _, c := range []byte(s) {
		var d byte
		switch {
		case '0' <= c && c <= '9':
			d = c - '0'
		}

		n *= uint64(10)

		n1 := n + uint64(d)
		n = n1
	}

	return n
}

func Atoi(s []byte) int64 {
	neg := false
	if s[0] == '+' {
		s = s[1:]
	} else if s[0] == '-' {
		neg = true
		s = s[1:]
	}

	// Convert unsigned and check range.
	un := ParseUint(s)

	n := int64(un)
	if neg {
		n = -n
	}
	return n
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

func ParseFile(stations *haxmap.Map[string, WeatherData], fileChunk []byte) {
	startWord := 0
	word := ""
	number := int64(0)
	for i := 0; i < len(fileChunk); i++ {
		for {
			if fileChunk[i] == ';' {
				word = BytesToString(fileChunk[startWord:i])
				i++
				break
			}
			i++
		}
		for {
			if fileChunk[i] == '\n' {
				startWord = i + 1
				break
			}
			var n uint64
			neg := false
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

			number = int64(n)
			if neg {
				number = -number
			}
			i++
		}
		if data, ok := stations.GetOrSet(word, WeatherData{
			Min:   number,
			Sum:   number,
			Max:   number,
			Count: 1,
		}); ok {
			if data.Max < number {
				data.Max = number
			}
			if data.Min > number {
				data.Min = number
			}
			data.Count += 1
			data.Sum += number
			stations.Set(word, data)
			continue
		}
	}
}

func Calculate() {
	file, err := os.Open("measurements.txt")
	if err != nil {
		log.Panic(err.Error())
	}
	defer file.Close()

	// stations := make(map[string]WeatherData)
	stations := haxmap.New[string, WeatherData](1 << 10)
	fileChuncksChannel := make(chan []byte, 10)
	fileChuncksDoneChannel := make(chan struct{})

	go ReadFileByChuncks(file, fileChuncksChannel, fileChuncksDoneChannel)

	const numWorkers = 40
	jobs := make(chan []byte, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		go (func() {
			for {
				ParseFile(stations, <-jobs)
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

	keys := make([]string, 0, stations.Len())
	stations.ForEach(func(k string, _ WeatherData) bool {
		keys = append(keys, k)
		return true
	})
	stationsNum := len(keys) - 1
	sort.Strings(keys)

	outBuf.WriteString("{")
	for _, k := range keys {
		v, _ := stations.Get(k)
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
