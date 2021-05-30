package common

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

type HashArray []string

func (h *HashArray) GetHashes() {
	gets256 := func(s string) string {
		dig := sha256.Sum256([]byte(s))
		return hex.EncodeToString(dig[:])
	}
	ha := *h
	for i := 1; i < len(ha); i++ {
		ha[i] = gets256(ha[i-1])
	}
}
func (h *HashArray) TransformBase64() {
	h2b := func(s string) string {
		b, _ := hex.DecodeString(s)
		return base64.RawStdEncoding.EncodeToString(b)
	}
	for i, hstr := range *h {
		if len(hstr) == 64 {
			(*h)[i] = h2b(hstr)
		}
	}
}
