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
	"encoding/json"
	"strconv"

	"github.com/th2-net/th2-listener-mysql-binlog-go/component/database"
)

type Operation string
type Bean interface {
	// Returns approximate size (bigger or equal than Serialize method return) for instances where Splittable method returns true.
	// Returns 0 where Splittable method returns false.
	SizeBytes() int
	// Returns serialized representation of instance.
	Serialize() ([]byte, error)
	// Returns true if the instance can be split.
	Splittable() bool
	// Returns parts as close as possible to passed size.
	Split(size int) []Bean
}

type DataMap map[string]any
type DataSlice []DataMap

type Record struct {
	Schema    string
	Table     string
	Operation Operation
}

func (r Record) sizeBytes() int {
	size := 2                              // {...}
	size += 9 + jsonSize(r.Schema) + 1     // "Schema":"...",
	size += 8 + jsonSize(r.Table) + 1      // "Table":"...",
	size += 12 + jsonSize(r.Operation) + 1 // "Operation":"...",
	return size
}

func (val DataMap) sizeBytes() int {
	size := 2            // {...}
	size += len(val) - 1 // ...,...
	for k, v := range val {
		size += jsonSize(k) + 1 + jsonSize(v) // "<k>":"<v>"
	}
	return size
}

func jsonSize(value interface{}) int {
	switch val := value.(type) {
	case nil:
		return 4 // null
	case int, int8, int16, int32, int64:
		return len(strconv.FormatInt(toInt64(val), 10))
	case uint, uint8, uint16, uint32, uint64:
		return len(strconv.FormatUint(toUint64(val), 10))
	case float32:
		return len(strconv.FormatFloat(float64(val), 'g', -1, 32))
	case float64:
		return len(strconv.FormatFloat(val, 'g', -1, 64))
	case string:
		return len(strconv.Quote(val))
	case Operation:
		return len(strconv.Quote(string(val)))
	case []byte:
		return ((len(val)+2)/3)*4 + 2
	default:
		b, _ := json.Marshal(val)
		return len(b)
	}
}

func toInt64(v any) int64 {
	switch vv := v.(type) {
	case int:
		return int64(vv)
	case int8:
		return int64(vv)
	case int16:
		return int64(vv)
	case int32:
		return int64(vv)
	case int64:
		return vv
	}
	return 0
}

func toUint64(v any) uint64 {
	switch vv := v.(type) {
	case uint:
		return uint64(vv)
	case uint8:
		return uint64(vv)
	case uint16:
		return uint64(vv)
	case uint32:
		return uint64(vv)
	case uint64:
		return vv
	}
	return 0
}

func (ds DataSlice) sizeBytes() int {
	size := len(ds) - 1 // ...,...
	for _, val := range ds {
		size += val.sizeBytes()
	}
	return size
}

func (ds DataSlice) split(baseSize int, maxSize int) []DataSlice {
	var res []DataSlice
	var partSize int
	var part DataSlice
	for i, val := range ds {
		valSize := val.sizeBytes()
		if i == 0 {
			partSize = baseSize + valSize
			part = DataSlice{val}
		} else {
			if partSize+valSize+1 > maxSize {
				res = append(res, part)
				partSize = baseSize + valSize
				part = DataSlice{val}
			} else {
				partSize += valSize + 1 // ...,...
				part = append(part, val)
			}
		}
	}
	return append(res, part)
}

func createValues(tableMetadata database.TableMetadata, rows [][]any) DataSlice {
	result := make(DataSlice, len(rows))
	for index, row := range rows {
		values := DataMap{}
		result[index] = values
		for columnIndex, columnValue := range row {
			values[tableMetadata[columnIndex]] = columnValue
		}
	}
	return result
}

func createUpdatePairs(tableMetadata database.TableMetadata, rows [][]any) []UpdatePair {
	result := make([]UpdatePair, len(rows)/2)
	var pair UpdatePair = UpdatePair{}
	for index, row := range rows {
		values := DataMap{}
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
