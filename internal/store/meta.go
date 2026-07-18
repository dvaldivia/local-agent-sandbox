// Copyright 2026 Daniel Valdivia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	mathrand "math/rand/v2"
)

// newUUID returns a random RFC 4122 version-4 UUID string.
func newUUID() string {
	var b [16]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		// crypto/rand should never fail; fall back to math/rand to stay non-fatal.
		for i := range b {
			b[i] = byte(mathrand.UintN(256))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]),
	)
}

// nameSuffixAlphabet mirrors Kubernetes' generateName charset (consonants and
// digits, avoiding vowels and ambiguous characters).
const nameSuffixAlphabet = "bcdfghjklmnpqrstvwxz2456789"

// randSuffix returns a random string of n characters from nameSuffixAlphabet.
func randSuffix(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = nameSuffixAlphabet[mathrand.IntN(len(nameSuffixAlphabet))]
	}
	return string(b)
}
