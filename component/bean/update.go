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
	updateOperation Operation = "UPDATE"
)

type UpdatePair struct {
	Before Values
	After  Values
}

type Update struct {
	Record
	Updated []UpdatePair
}

func NewUpdate(schema string, table string, fields []string, rows [][]any) Update {
	return Update{Record: Record{Schema: schema, Table: table, Operation: updateOperation}, Updated: createUpdatePairs(fields, rows)}
}

func (b Update) SizeBytes() int {
	return 0
}

func (b Update) Serialize() ([]byte, error) {
	return json.Marshal(b)
}

func (b Update) Splittable() bool {
	return false
}

func (b Update) Split(size int) []Bean {
	return []Bean{b}
}
