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
	"bytes"
	"slices"
	"strings"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// ImplementationName is the name of the labels implementation.
const ImplementationName = "thanoslabels"

// Labels is a sorted set of labels backed by labelpb.Label pointers.
// This allows direct use of Thanos protobuf labels without conversion overhead.
type Labels struct {
	data []*labelpb.Label
}

// Bytes returns an opaque encoding of ls, usable as a map key.
func (ls Labels) Bytes(buf []byte) []byte {
	b := bytes.NewBuffer(buf[:0])
	b.WriteByte(labelSep)
	for i, l := range ls.data {
		if i > 0 {
			b.WriteByte(sep)
		}
		b.WriteString(l.Name)
		b.WriteByte(sep)
		b.WriteString(l.Value)
	}
	return b.Bytes()
}

// IsZero implements yaml.IsZeroer.
func (ls Labels) IsZero() bool {
	return len(ls.data) == 0
}

// MatchLabels returns a subset of Labels that matches/does not match with the provided label names.
func (ls Labels) MatchLabels(on bool, names ...string) Labels {
	b := NewBuilder(ls)
	if on {
		b.Keep(names...)
	} else {
		b.Del(MetricName)
		b.Del(names...)
	}
	return b.Labels()
}

// Hash returns a hash value for the label set.
func (ls Labels) Hash() uint64 {
	b := make([]byte, 0, 1024)
	for i, l := range ls.data {
		if len(b)+len(l.Name)+len(l.Value)+2 >= cap(b) {
			h := xxhash.New()
			_, _ = h.Write(b)
			for _, l := range ls.data[i:] {
				_, _ = h.WriteString(l.Name)
				_, _ = h.Write(seps)
				_, _ = h.WriteString(l.Value)
				_, _ = h.Write(seps)
			}
			return h.Sum64()
		}
		b = append(b, l.Name...)
		b = append(b, sep)
		b = append(b, l.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b)
}

// HashForLabels returns a hash value for the labels matching the provided names.
// 'names' have to be sorted in ascending order.
func (ls Labels) HashForLabels(b []byte, names ...string) (uint64, []byte) {
	b = b[:0]
	i, j := 0, 0
	for i < len(ls.data) && j < len(names) {
		switch {
		case names[j] < ls.data[i].Name:
			j++
		case ls.data[i].Name < names[j]:
			i++
		default:
			b = append(b, ls.data[i].Name...)
			b = append(b, sep)
			b = append(b, ls.data[i].Value...)
			b = append(b, sep)
			i++
			j++
		}
	}
	return xxhash.Sum64(b), b
}

// HashWithoutLabels returns a hash value for all labels except those matching the provided names.
// 'names' have to be sorted in ascending order.
func (ls Labels) HashWithoutLabels(b []byte, names ...string) (uint64, []byte) {
	b = b[:0]
	j := 0
	for _, l := range ls.data {
		for j < len(names) && names[j] < l.Name {
			j++
		}
		if l.Name == MetricName || (j < len(names) && l.Name == names[j]) {
			continue
		}
		b = append(b, l.Name...)
		b = append(b, sep)
		b = append(b, l.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b), b
}

// BytesWithLabels is just as Bytes(), but only for labels matching names.
// 'names' have to be sorted in ascending order.
func (ls Labels) BytesWithLabels(buf []byte, names ...string) []byte {
	b := bytes.NewBuffer(buf[:0])
	b.WriteByte(labelSep)
	i, j := 0, 0
	for i < len(ls.data) && j < len(names) {
		switch {
		case names[j] < ls.data[i].Name:
			j++
		case ls.data[i].Name < names[j]:
			i++
		default:
			if b.Len() > 1 {
				b.WriteByte(sep)
			}
			b.WriteString(ls.data[i].Name)
			b.WriteByte(sep)
			b.WriteString(ls.data[i].Value)
			i++
			j++
		}
	}
	return b.Bytes()
}

// BytesWithoutLabels is just as Bytes(), but only for labels not matching names.
// 'names' have to be sorted in ascending order.
func (ls Labels) BytesWithoutLabels(buf []byte, names ...string) []byte {
	b := bytes.NewBuffer(buf[:0])
	b.WriteByte(labelSep)
	j := 0
	for _, l := range ls.data {
		for j < len(names) && names[j] < l.Name {
			j++
		}
		if j < len(names) && l.Name == names[j] {
			continue
		}
		if b.Len() > 1 {
			b.WriteByte(sep)
		}
		b.WriteString(l.Name)
		b.WriteByte(sep)
		b.WriteString(l.Value)
	}
	return b.Bytes()
}

// Copy returns a copy of the labels.
func (ls Labels) Copy() Labels {
	result := make([]*labelpb.Label, len(ls.data))
	for i, l := range ls.data {
		result[i] = &labelpb.Label{Name: l.Name, Value: l.Value}
	}
	return Labels{data: result}
}

// Get returns the value for the label with the given name.
func (ls Labels) Get(name string) string {
	for _, l := range ls.data {
		if l.Name == name {
			return l.Value
		}
		if l.Name > name {
			break
		}
	}
	return ""
}

// Has returns true if the label with the given name is present.
func (ls Labels) Has(name string) bool {
	for _, l := range ls.data {
		if l.Name == name {
			return true
		}
		if l.Name > name {
			break
		}
	}
	return false
}

// HasDuplicateLabelNames returns whether ls has duplicate label names.
func (ls Labels) HasDuplicateLabelNames() (string, bool) {
	for i := 1; i < len(ls.data); i++ {
		if ls.data[i].Name == ls.data[i-1].Name {
			return ls.data[i].Name, true
		}
	}
	return "", false
}

// WithoutEmpty returns the labelset without empty labels.
func (ls Labels) WithoutEmpty() Labels {
	for i, l := range ls.data {
		if l.Value == "" {
			result := make([]*labelpb.Label, 0, len(ls.data)-1)
			result = append(result, ls.data[:i]...)
			for _, l := range ls.data[i+1:] {
				if l.Value != "" {
					result = append(result, l)
				}
			}
			return Labels{data: result}
		}
	}
	return ls
}

// ByteSize returns the approximate size of the labels in bytes.
func (ls Labels) ByteSize() uint64 {
	var size uint64
	for _, l := range ls.data {
		size += uint64(len(l.Name)+len(l.Value)) + 2*uint64(unsafe.Sizeof(""))
	}
	return size
}

// Equal returns whether the two label sets are equal.
func Equal(ls, o Labels) bool {
	if len(ls.data) != len(o.data) {
		return false
	}
	for i, l := range ls.data {
		if l.Name != o.data[i].Name || l.Value != o.data[i].Value {
			return false
		}
	}
	return true
}

// EmptyLabels returns an empty Labels value.
func EmptyLabels() Labels {
	return Labels{}
}

// New returns a sorted Labels from the given labels.
func New(ls ...Label) Labels {
	slices.SortFunc(ls, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
	result := make([]*labelpb.Label, len(ls))
	for i, l := range ls {
		result[i] = &labelpb.Label{Name: l.Name, Value: l.Value}
	}
	return Labels{data: result}
}

// FromStrings creates new labels from pairs of strings.
func FromStrings(ss ...string) Labels {
	if len(ss)%2 != 0 {
		panic("invalid number of strings")
	}
	ls := make([]Label, 0, len(ss)/2)
	for i := 0; i < len(ss); i += 2 {
		ls = append(ls, Label{Name: ss[i], Value: ss[i+1]})
	}
	return New(ls...)
}

// Compare compares the two label sets.
func Compare(a, b Labels) int {
	l := min(len(a.data), len(b.data))
	for i := 0; i < l; i++ {
		if c := strings.Compare(a.data[i].Name, b.data[i].Name); c != 0 {
			return c
		}
		if c := strings.Compare(a.data[i].Value, b.data[i].Value); c != 0 {
			return c
		}
	}
	return len(a.data) - len(b.data)
}

// CopyFrom copies labels from b on top of whatever was in ls previously.
func (ls *Labels) CopyFrom(b Labels) {
	ls.data = append(ls.data[:0], b.data...)
}

// IsEmpty returns true if ls represents an empty set of labels.
func (ls Labels) IsEmpty() bool {
	return len(ls.data) == 0
}

// Len returns the number of labels.
func (ls Labels) Len() int {
	return len(ls.data)
}

// Range calls f on each label.
func (ls Labels) Range(f func(l Label)) {
	for _, l := range ls.data {
		f(Label{Name: l.Name, Value: l.Value})
	}
}

// Validate calls f on each label. If f returns a non-nil error, then it returns that error.
func (ls Labels) Validate(f func(l Label) error) error {
	for _, l := range ls.data {
		if err := f(Label{Name: l.Name, Value: l.Value}); err != nil {
			return err
		}
	}
	return nil
}

// DropMetricName returns Labels with the "__name__" removed.
//
// Deprecated: Use DropReserved instead.
func (ls Labels) DropMetricName() Labels {
	return ls.DropReserved(func(n string) bool { return n == MetricName })
}

// DropReserved returns Labels without the chosen reserved labels.
func (ls Labels) DropReserved(shouldDropFn func(name string) bool) Labels {
	rm := 0
	for i, l := range ls.data {
		if l.Name[0] > '_' {
			break
		}
		if shouldDropFn(l.Name) {
			i := i - rm
			if i == 0 {
				ls.data = ls.data[1:]
			} else {
				ls.data = append(ls.data[:i:i], ls.data[i+1:]...)
			}
			rm++
		}
	}
	return ls
}

// InternStrings calls intern on every string value inside ls.
func (ls *Labels) InternStrings(intern func(string) string) {
	for _, l := range ls.data {
		l.Name = intern(l.Name)
		l.Value = intern(l.Value)
	}
}

// ReleaseStrings calls release on every string value inside ls.
func (ls Labels) ReleaseStrings(release func(string)) {
	for _, l := range ls.data {
		release(l.Name)
		release(l.Value)
	}
}

// Builder allows modifying Labels.
type Builder struct {
	base Labels
	del  []string
	add  []Label
}

// Reset clears all current state for the builder.
func (b *Builder) Reset(base Labels) {
	b.base = base
	b.del = b.del[:0]
	b.add = b.add[:0]
	b.base.Range(func(l Label) {
		if l.Value == "" {
			b.del = append(b.del, l.Name)
		}
	})
}

// Labels returns the labels from the builder.
func (b *Builder) Labels() Labels {
	if len(b.del) == 0 && len(b.add) == 0 {
		return b.base
	}

	expectedSize := max(len(b.base.data)+len(b.add)-len(b.del), 1)
	result := make([]*labelpb.Label, 0, expectedSize)

	for _, l := range b.base.data {
		if slices.Contains(b.del, l.Name) || contains(b.add, l.Name) {
			continue
		}
		result = append(result, l)
	}
	if len(b.add) > 0 {
		for _, l := range b.add {
			result = append(result, &labelpb.Label{Name: l.Name, Value: l.Value})
		}
		slices.SortFunc(result, func(a, b *labelpb.Label) int {
			return strings.Compare(a.Name, b.Name)
		})
	}
	return Labels{data: result}
}

// ScratchBuilder allows efficient construction of a Labels from scratch.
type ScratchBuilder struct {
	add       []*labelpb.Label
	unsafeAdd bool
}

// SymbolTable is no-op, just for API parity with dedupelabels.
type SymbolTable struct{}

func NewSymbolTable() *SymbolTable { return nil }

func (*SymbolTable) Len() int { return 0 }

// NewScratchBuilder creates a ScratchBuilder initialized for Labels with n entries.
func NewScratchBuilder(n int) ScratchBuilder {
	return ScratchBuilder{add: make([]*labelpb.Label, 0, n)}
}

// NewBuilderWithSymbolTable creates a Builder, for API parity with dedupelabels.
func NewBuilderWithSymbolTable(*SymbolTable) *Builder {
	return NewBuilder(EmptyLabels())
}

// NewScratchBuilderWithSymbolTable creates a ScratchBuilder, for API parity with dedupelabels.
func NewScratchBuilderWithSymbolTable(_ *SymbolTable, n int) ScratchBuilder {
	return NewScratchBuilder(n)
}

func (*ScratchBuilder) SetSymbolTable(*SymbolTable) {}

// SetUnsafeAdd allows turning on/off the assumptions that added strings are unsafe
// for reuse. When true, strings will be cloned before adding.
func (b *ScratchBuilder) SetUnsafeAdd(unsafeAdd bool) {
	b.unsafeAdd = unsafeAdd
}

func (b *ScratchBuilder) Reset() {
	b.add = b.add[:0]
}

// Add a name/value pair.
func (b *ScratchBuilder) Add(name, value string) {
	if b.unsafeAdd {
		name = strings.Clone(name)
		value = strings.Clone(value)
	}
	b.add = append(b.add, &labelpb.Label{Name: name, Value: value})
}

// Sort the labels added so far by name.
func (b *ScratchBuilder) Sort() {
	slices.SortFunc(b.add, func(a, c *labelpb.Label) int {
		return strings.Compare(a.Name, c.Name)
	})
}

// Assign is for when you already have a Labels which you want this ScratchBuilder to return.
func (b *ScratchBuilder) Assign(ls Labels) {
	b.add = append(b.add[:0], ls.data...)
}

// Labels returns the name/value pairs added so far as a Labels object.
func (b *ScratchBuilder) Labels() Labels {
	return Labels{data: append([]*labelpb.Label{}, b.add...)}
}

// Overwrite the newly-built Labels out to ls.
func (b *ScratchBuilder) Overwrite(ls *Labels) {
	ls.data = append(ls.data[:0], b.add...)
}

// SizeOfLabels returns the approximate space required for n copies of a label.
func SizeOfLabels(name, value string, n uint64) uint64 {
	return (uint64(len(name)) + uint64(unsafe.Sizeof(name)) + uint64(len(value)) + uint64(unsafe.Sizeof(value))) * n
}

// ---- Thanos-specific methods for zero-copy conversion ----

// FromLabelpbLabels creates Labels directly from labelpb.Label pointers.
// Zero-copy if already sorted.
func FromLabelpbLabels(lbls []*labelpb.Label) Labels {
	return Labels{data: lbls}
}

// ToLabelpbLabels returns the underlying labelpb.Label pointers.
// Zero-copy.
func (ls Labels) ToLabelpbLabels() []*labelpb.Label {
	return ls.data
}
