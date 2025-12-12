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
)

const (
	insertOperation Operation = "INSERT"
)

type Insert struct {
	Record
	Inserted []Values
}

func NewInsert(schema string, table string, fields []string, rows [][]any) Insert {
	return Insert{Record: Record{Schema: schema, Table: table, Operation: insertOperation}, Inserted: createValues(fields, rows)}
}

func (b Insert) SizeBytes() int {
	if !b.Splittable() {
		return 0
	}
	return b.baseSize() + sliceValuesSizeBytes(b.Inserted)
}

func (b Insert) Serialize() ([]byte, error) {
	return json.Marshal(b)
}

func (b Insert) Splittable() bool {
	return len(b.Inserted) > 1
}

func (b Insert) Split(size int) []Bean {
	if !b.Splittable() {
		return []Bean{b}
	}
	
	parts := sliceValuesSplit(b.Inserted, b.baseSize(), size)
	res := make([]Bean, len(parts))
	for i, part := range parts {
		res[i] = Insert{Record: b.Record, Inserted: part}
	}

	return res
}

func (b Insert) baseSize() int {
	return b.Record.sizeBytes() + 13 // "Inserted":[...]
}
