/*
 * Copyright 2024-2025 Exactpro (Exactpro Systems Limited)
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

package bean

import (
	"fmt"

	"github.com/th2-net/th2-listener-mysql-binlog-go/component/database"
)

type Operation string
type Bean interface {
	// Returns approximate size (bigger or equal than Serialize) for Splittable instance
	SizeBytes() int
	Serialize() ([]byte, error)
	// Returns true if instance content allow to execute split
	Splittable() bool
	// Returns parts as close as possible in size.
	Split(size int) []Bean
}

type Values map[string]interface{}

type Record struct {
	Schema    string
	Table     string
	Operation Operation
}

func (r Record) sizeBytes() int {
	size := 2                         // {...}
	size += 10 + len(r.Schema) + 2    // "Schema":"...",
	size += 9 + len(r.Table) + 2      // "Table":"...",
	size += 13 + len(r.Operation) + 2 // "Operation":"...",
	return size
}

func (val Values) sizeBytes() int {
	size := 2            // {...}
	size += len(val) - 1 // ...,...
	for k, v := range val {
		size += 1 + len(k) + 3 + len(fmt.Sprintf("%v", v)) + 1 // "<k>":"<v>"
	}
	return size
}

func sliceValuesSizeBytes(slice []Values) int {
	size := len(slice) - 1 // ...,...
	for _, val := range slice {
		size += val.sizeBytes()
	}
	return size
}

func sliceValuesSplit(slice []Values, baseSize int, maxSize int) [][]Values {
	var res [][]Values
	var partSize int
	var part []Values
	for i, val := range slice {
		valSize := val.sizeBytes()
		if i == 0 {
			partSize = baseSize + valSize
			part = []Values{val}
		} else {
			if partSize+valSize+1 > maxSize {
				res = append(res, part)
				partSize = baseSize + valSize
				part = []Values{val}
			} else {
				partSize += valSize + 1 // ...,...
				part = append(part, val)
			}
		}
	}
	return append(res, part)
}

func createValues(tableMetadata database.TableMetadata, rows [][]any) []Values {
	result := make([]Values, len(rows))
	for index, row := range rows {
		values := Values{}
		result[index] = values
		for columnIndex, columnValue := range row {
			values[tableMetadata[columnIndex]] = columnValue
		}
	}
	return result
}

func createUpdatePairs(tableMetadata database.TableMetadata, rows [][]interface{}) []UpdatePair {
	result := make([]UpdatePair, len(rows)/2)
	var pair UpdatePair = UpdatePair{}
	for index, row := range rows {
		values := Values{}
		for columnIndex, columnValue := range row {
			values[tableMetadata[columnIndex]] = columnValue
		}
		if index%2 == 0 {
			pair.Before = values
		} else {
			pair.After = values
			result[(index-1)/2] = pair
			pair = UpdatePair{}
		}
	}
	return result
}
