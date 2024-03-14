package main

import (
	"hash"
	"math"
)

// https://github.com/chris-tomich/go-fast-hashmap/blob/master/go_fast_hashmap.go
// This appears to be a pretty popular load factor figure (taken from a mixture of the Go built-in map, Mono, and .NET Core)
const loadFactor float64 = 0.7

var primeBasedSizes = []uint64{
	11, 101, 211, 503, 1009, 1511, 2003, 3511, 5003, 7507, 10007, 15013, 20011,
	25013, 50021, 75011, 100003, 125003, 150001, 175003, 200003, 350003, 500009,
	750019, 1000003, 1250003, 1500007, 1750009, 2000003, 3500017, 5000011, 7500013,
	10000019,
}

func isPrime(n uint64) bool {
	for i := uint64(3); i <= (n+3)/3; i++ {
		if n%i == 0 {
			return false
		}
	}

	return true
}

func nextPrime(start uint64) uint64 {
	num := start

	if num%2 == 0 {
		num++
	}

	for ; !isPrime(num); num += 2 {
		if num > math.MaxUint64 {
			panic("Requested size is too large.")
		}
	}

	return num
}

func findHashmapPrimeSize(size uint64) uint64 {
	maxSize := uint64(float64(size) / loadFactor)

	for _, prime := range primeBasedSizes {
		if prime > maxSize {
			return prime
		}
	}

	// The dataset is clearly huge so we'll just calculate the next prime beyond the maxSize we were given.
	// This obviously could take a while but clearly the user is expecting this as their size is beyond 10,000,000.
	// The following prime finding algorithm is hugely inadequate but it'll have to do for now.

	largePrime := nextPrime(maxSize)

	return largePrime
}

type keyValuePair struct {
	Key   string
	Value WeatherData
}

const BucketSize = 50

type bucket struct {
	pairs [BucketSize]keyValuePair
	count int
}

type Hashmap struct {
	buckets []bucket
	bSize   uint64
}

func New(size uint64) *Hashmap {
	bSize := findHashmapPrimeSize(size)

	m := &Hashmap{
		buckets: make([]bucket, bSize),
		bSize:   bSize,
	}

	return m
}

func (m *Hashmap) findMatchingKeyOrNextKeyValuePair(key string) (*keyValuePair, bool) {
	fnv := New64()
	fnv.Write([]byte(key))

	h := fnv.Sum64()

	i := h % m.bSize

	b := &(m.buckets[i])

	for {
		for j := 0; j < b.count; j++ {
			if b.pairs[j].Key == key {
				return &(b.pairs[j]), true
			}
		}

		switch {
		case b.count < BucketSize:
			j := b.count
			b.count++
			return &(b.pairs[j]), false

		default:
			r := m.bSize - i

			if r > (m.bSize / 2) {
				step := h % r
				i += step
			} else {
				step := h % i
				i -= step
			}
			b = &(m.buckets[i])
		}
	}
}

func (m *Hashmap) Get(key string) (WeatherData, bool) {
	keyValuePair, isMatching := m.findMatchingKeyOrNextKeyValuePair(key)

	if isMatching {
		return keyValuePair.Value, true
	} else {
		return WeatherData{}, false
	}
}

func (m *Hashmap) Set(key string, value WeatherData) {
	keyValuePair, isMatching := m.findMatchingKeyOrNextKeyValuePair(key)

	if isMatching {
		keyValuePair.Value = value
	} else {
		keyValuePair.Key = key
		keyValuePair.Value = value
	}
}

// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fnv implements FNV-1 and FNV-1a, non-cryptographic hash functions
// created by Glenn Fowler, Landon Curt Noll, and Phong Vo.
// See
// https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function.
//
// All the hash.Hash implementations returned by this package also
// implement encoding.BinaryMarshaler and encoding.BinaryUnmarshaler to
// marshal and unmarshal the internal state of the hash.

type (
	sum64  uint64
	sum64a uint64
)

const (
	offset64 = 14695981039346656037
	prime32  = 16777619
	prime64  = 1099511628211
)

// New64 returns a new 64-bit FNV-1 [hash.Hash].
// Its Sum method will lay the value out in big-endian byte order.
func New64() hash.Hash64 {
	var s sum64 = offset64
	return &s
}

// New64a returns a new 64-bit FNV-1a [hash.Hash].
// Its Sum method will lay the value out in big-endian byte order.
func New64a() hash.Hash64 {
	var s sum64a = offset64
	return &s
}

func (s *sum64) Reset()  { *s = offset64 }
func (s *sum64a) Reset() { *s = offset64 }

func (s *sum64) Sum64() uint64  { return uint64(*s) }
func (s *sum64a) Sum64() uint64 { return uint64(*s) }

func (s *sum64) Write(data []byte) (int, error) {
	hash := *s
	for _, c := range data {
		hash *= prime64
		hash ^= sum64(c)
	}
	*s = hash
	return len(data), nil
}

func (s *sum64a) Write(data []byte) (int, error) {
	hash := *s
	for _, c := range data {
		hash ^= sum64a(c)
		hash *= prime64
	}
	*s = hash
	return len(data), nil
}

func (s *sum64) Size() int  { return 8 }
func (s *sum64a) Size() int { return 8 }

func (s *sum64) BlockSize() int  { return 1 }
func (s *sum64a) BlockSize() int { return 1 }

func (s *sum64) Sum(in []byte) []byte {
	v := uint64(*s)
	return append(in, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (s *sum64a) Sum(in []byte) []byte {
	v := uint64(*s)
	return append(in, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
