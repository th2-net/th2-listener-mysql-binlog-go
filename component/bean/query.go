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

package bean

import (
	"encoding/json"
)

const (
	truncateOperation    Operation = "TRUNCATE"
	createTableOperation Operation = "CREATE_TABLE"
	dropTableOperation   Operation = "DROP_TABLE"
	alterTableOperation  Operation = "ALTER_TABLE"
	unknownOperation     Operation = "UNKNOWN"
)

type Query struct {
	Record
	Query string
}

func NewQuery(schema string, table string, query string, operation Operation) Query {
	return Query{Record: Record{Schema: schema, Table: table, Operation: operation}, Query: query}
}

func (b Query) SizeBytes() int {
	return 0
}

func (b Query) Serialize() ([]byte, error) {
	return json.Marshal(b)
}

func (b Query) Splittable() bool {
	return false
}

func (b Query) Split(size int) []Bean {
	return []Bean{b}
}
