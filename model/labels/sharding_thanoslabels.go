// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build thanoslabels

package labels

import (
	"github.com/cespare/xxhash/v2"
)

// StableHash is a labels hashing implementation which is guaranteed to not change over time.
// This function should be used whenever labels hashing backward compatibility must be guaranteed.
func StableHash(ls Labels) uint64 {
	b := make([]byte, 0, 1024)
	var h *xxhash.Digest
	for _, l := range ls.data {
		if h == nil && len(b)+len(l.Name)+len(l.Value)+2 >= cap(b) {
			h = xxhash.New()
			_, _ = h.Write(b)
		}
		if h != nil {
			_, _ = h.WriteString(l.Name)
			_, _ = h.Write(seps)
			_, _ = h.WriteString(l.Value)
			_, _ = h.Write(seps)
			continue
		}
		b = append(b, l.Name...)
		b = append(b, sep)
		b = append(b, l.Value...)
		b = append(b, sep)
	}
	if h != nil {
		return h.Sum64()
	}
	return xxhash.Sum64(b)
}
