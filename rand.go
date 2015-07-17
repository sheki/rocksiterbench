package rocksiterbench

import "math/rand"

var nums = []byte("0123456789")

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int, src []byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = src[rand.Intn(len(src))]
	}
	return b
}

// RandKey numeric key of given length
func randKey(n int) []byte {
	return randSeq(n, nums)
}

// RandBlob returns a byte blob of approximate lenght
func randBlob(n int) []byte {
	return randSeq(n, letters)
}
