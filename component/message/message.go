/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

package message

import (
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/th2-net/th2-read-mysql-binlog-go/component/database"
)

const (
	insertOperation = "INSERT"
	updateOperation = "UPDATE"
	deleteOperation = "DELETE"
)

type Message interface {
}

type Header struct {
	LogName   string
	LogPos    uint32
	SeqNum    int64
	Timestamp time.Time
}

type Metadata struct {
	Schema    string
	Table     string
	Operation string
}

type Values map[string]interface{}

type Insert struct {
	Header
	Metadata
	Inserted []Values
}

type UpdatePair struct {
	Before Values
	After  Values
}

type Update struct {
	Header
	Metadata
	Updated []UpdatePair
}

type Delete struct {
	Header
	Metadata
	Deleted []Values
}

func CreateInsert(metadata *database.Metadata, header Header, event *replication.RowsEvent) (*Insert, error) {
	schema := string(event.Table.Schema)
	table := string(event.Table.Table)
	fields, err := metadata.GetFields(schema, table)
	if err != nil {
		return nil, fmt.Errorf("create %s struct failure: %w", insertOperation, err)
	}
	return &Insert{Header: header, Metadata: Metadata{Schema: schema, Table: table, Operation: insertOperation}, Inserted: createValues(fields, event.Rows)}, nil
}

func CreateUpdate(metadata *database.Metadata, header Header, event *replication.RowsEvent) (*Update, error) {
	schema := string(event.Table.Schema)
	table := string(event.Table.Table)
	fields, err := metadata.GetFields(schema, table)
	if err != nil {
		return nil, fmt.Errorf("create %s struct failure: %w", updateOperation, err)
	}
	return &Update{Header: header, Metadata: Metadata{Schema: schema, Table: table, Operation: updateOperation}, Updated: createUpdatePairs(fields, event.Rows)}, nil
}

func CreateDelete(metadata *database.Metadata, header Header, event *replication.RowsEvent) (*Delete, error) {
	schema := string(event.Table.Schema)
	table := string(event.Table.Table)
	fields, err := metadata.GetFields(schema, table)
	if err != nil {
		return nil, fmt.Errorf("create %s struct failure: %w", deleteOperation, err)
	}
	return &Delete{Header: header, Metadata: Metadata{Schema: schema, Table: table, Operation: deleteOperation}, Deleted: createValues(fields, event.Rows)}, nil
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
