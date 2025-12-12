/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bean_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/th2-net/th2-listener-mysql-binlog-go/component/bean"
)

const (
	minLen    = 5
	maxLen    = 50
	minWidth  = 5
	maxWidth  = 15
	minHeight = 1
	maxHeight = 10
	letters   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var (
	seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	randTypes  []randAny
)

type randAny func() any

func init() {
	randTypes = []randAny{
		func() any { return randString() },
		func() any { return randInt() },
		func() any { return randFloat() },
		func() any { return randBytes() },
		func() any { return nil },
	}
}

func TestSizeBytesVsSerialized(t *testing.T) {
	tests := []struct {
		name    string
		newBean func() bean.Bean
	}{
		{
			name: "insert",
			newBean: func() bean.Bean {
				schema := randString()
				table := randString()
				fields, rows := randRows()
				return bean.NewInsert(schema, table, fields, rows)
			},
		},
		{
			name: "delete",
			newBean: func() bean.Bean {
				schema := randString()
				table := randString()
				fields, rows := randRows()
				return bean.NewDelete(schema, table, fields, rows)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bean := tc.newBean()
			size := bean.SizeBytes()
			data, err := bean.Serialize()
			if err != nil {
				t.Fatal(err)
			}
			if int(size) < len(data) {
				t.Fatalf("size calculated: %d, serialized: %d, data: %s", size, len(data), string(data))
			}
		})
	}
}

func TestSplittable(t *testing.T) {
	tests := []struct {
		name       string
		newBean    func() bean.Bean
		splittable bool
	}{
		{
			name: "splittable insert",
			newBean: func() bean.Bean {
				schema := randString()
				table := randString()
				fields, rows := randRowsM(randIntM(minWidth, maxWidth), randIntM(2, maxHeight))
				return bean.NewInsert(schema, table, fields, rows)
			},
			splittable: true,
		},
		{
			name: "not splittable insert",
			newBean: func() bean.Bean {
				schema := randString()
				table := randString()
				fields, rows := randRowsM(randIntM(minWidth, maxWidth), 1)
				return bean.NewInsert(schema, table, fields, rows)
			},
			splittable: false,
		},
		{
			name: "splittable delete",
			newBean: func() bean.Bean {
				schema := randString()
				table := randString()
				fields, rows := randRowsM(randIntM(minWidth, maxWidth), randIntM(2, maxHeight))
				return bean.NewDelete(schema, table, fields, rows)
			},
			splittable: true,
		},
		{
			name: "not splittable delete",
			newBean: func() bean.Bean {
				schema := randString()
				table := randString()
				fields, rows := randRowsM(randIntM(minWidth, maxWidth), 1)
				return bean.NewDelete(schema, table, fields, rows)
			},
			splittable: false,
		},
		{
			name: "not splittable update",
			newBean: func() bean.Bean {
				schema := randString()
				table := randString()
				fields, rows := randRows()
				return bean.NewUpdate(schema, table, fields, rows)
			},
			splittable: false,
		},
		{
			name: "not splittable query",
			newBean: func() bean.Bean {
				schema := randString()
				table := randString()
				query := randStringM(10, 1_000)
				operation := bean.Operation(randString())
				return bean.NewQuery(schema, table, query, operation)
			},
			splittable: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bean := tc.newBean()
			splittable := bean.Splittable()
			if tc.splittable != splittable {
				t.Fatalf("splittable expected: %v, got: %v, bean: %v", tc.splittable, splittable, bean)
			}
			size := bean.SizeBytes()
			if tc.splittable && size <= 0 {
				t.Fatalf("size expected > 0, got: %d, bean: %v", size, bean)
			}
			if !tc.splittable && size != 0 {
				t.Fatalf("size expected = 0, got: %d, bean: %v", size, bean)
			}
		})
	}
}

func TestSplit(t *testing.T) {
	tests := []struct {
		name       string
		newBean    func() bean.Bean
		splittable bool
	}{
		{
			name: "splittable",
			newBean: func() bean.Bean {
				schema := randString()
				table := randString()
				fields, rows := randRowsM(randIntM(minWidth, maxWidth), randIntM(1, maxHeight))
				return bean.NewInsert(schema, table, fields, rows)
			},
			splittable: true,
		},
		{
			name: "splittable",
			newBean: func() bean.Bean {
				schema := randString()
				table := randString()
				fields, rows := randRowsM(randIntM(minWidth, maxWidth), randIntM(1, maxHeight))
				return bean.NewDelete(schema, table, fields, rows)
			},
			splittable: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bean := tc.newBean()
			splittable := bean.Splittable()
			if tc.splittable != splittable {
				t.Fatalf("splittable expected: %v, got: %v, bean: %v", tc.splittable, splittable, bean)
			}
		})
	}
}

func randIntM(min, max int) int {
	return seededRand.Intn(max-min+1) + min
}

func randStringM(min, max int) string {
	n := randIntM(min, max)
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[seededRand.Intn(len(letters))]
	}
	return string(b)
}

func randInt() int {
	return seededRand.Int()
}

func randFloat() float64 {
	return seededRand.Float64()
}

func randBytes() []byte {
	return []byte(randString())
}

func randString() string {
	return randStringM(minLen, maxLen)
}

func randRowsM(width, height int) ([]string, [][]any) {
	fields := make([]string, width)
	rows := make([][]any, height)

	for i := range fields {
		fields[i] = randString()
	}
	for i := range rows {
		row := make([]any, len(fields))
		for j := range fields {
			row[j] = randTypes[j%len(randTypes)]()
		}
		rows[i] = row
	}
	return fields, rows
}

func randRows() ([]string, [][]any) {
	width := randIntM(minWidth, maxWidth)
	height := randIntM(minHeight, maxHeight)
	return randRowsM(width, height)
}
