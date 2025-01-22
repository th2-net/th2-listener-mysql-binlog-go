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
	"github.com/th2-net/th2-read-mysql-binlog-go/component/database"
)

const (
	insertOperation = "INSERT"
	updateOperation = "UPDATE"
	deleteOperation = "DELETE"
)

type Values map[string]interface{}

type Insert struct {
	Operation string
	Inserted  []Values
}

type UpdatePair struct {
	Before Values
	After  Values
}

type Update struct {
	Operation string
	Updated   []UpdatePair
}

type Delete struct {
	Operation string
	Deleted   []Values
}

func NewInsert(fields []string, rows [][]interface{}) Insert {
	return Insert{Operation: insertOperation, Inserted: createValues(fields, rows)}
}

func NewUpdate(fields []string, rows [][]interface{}) Update {
	return Update{Operation: updateOperation, Updated: createUpdatePairs(fields, rows)}
}

func NewDelete(fields []string, rows [][]interface{}) Delete {
	return Delete{Operation: deleteOperation, Deleted: createValues(fields, rows)}
}

func createValues(tableMetadata database.TableMetadata, rows [][]interface{}) []Values {
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
