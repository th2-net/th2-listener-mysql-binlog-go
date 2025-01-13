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

package database

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type TableMetadata []string

type SchemaMetadata map[string]TableMetadata

type Metadata struct {
	db      *sql.DB
	schemas map[string]SchemaMetadata
}

func CreateMetadata(host string, port uint16, username string, password string) (*Metadata, error) {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema", username, password, host, port)
	db, err := sql.Open("mysql", dataSourceName)

	if err != nil {
		return nil, err
	}

	return &Metadata{db: db, schemas: make(map[string]SchemaMetadata)}, nil
}

func (metadata *Metadata) loadFields(schema string, table string) ([]string, error) {
	rows, err := metadata.db.Query(
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
		schema,
		table,
	)

	if err != nil {
		return nil, fmt.Errorf("execute query for getting %s.%s table metadata failure: %w", schema, table, err)
	}

	defer rows.Close()

	var fields []string
	i := 0

	var columnName string
	for rows.Next() {
		err := rows.Scan(&columnName)

		if err != nil {
			return nil, fmt.Errorf("scan query result for getting %s.%s table metadata failure: %w", schema, table, err)
		}

		fields = append(fields, columnName)
		i++
	}

	return fields, nil
}

func (metadata *Metadata) GetFields(schema string, table string) ([]string, error) {
	schemaMetadata, exist := metadata.schemas[schema]
	var tableMetadata TableMetadata = nil
	var err error = nil

	if schemaMetadata == nil {
		tableMetadata, err = metadata.loadFields(schema, table)
		if err == nil {
			schemaMetadata = make(SchemaMetadata)
			metadata.schemas[schema] = schemaMetadata
			schemaMetadata[table] = tableMetadata
		}
	} else {
		tableMetadata = schemaMetadata[table]
		if tableMetadata == nil {
			tableMetadata, err = metadata.loadFields(schema, table)
			if err == nil {
				schemaMetadata[table] = tableMetadata
			}
		}
	}

	return tableMetadata, err
}

func (metadata *Metadata) Close() error {
	return metadata.db.Close()
}
