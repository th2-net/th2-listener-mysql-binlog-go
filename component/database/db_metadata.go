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

package database

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/th2-net/th2-common-go/pkg/log"
	conf "github.com/th2-net/th2-listener-mysql-binlog-go/component/configuration"
)

var (
	logger = log.ForComponent("db-meta-data")
)

type TableMetadata []string

type SchemaMetadata map[string]TableMetadata

type DbMetadata map[string]SchemaMetadata

func LoadMetadata(host string, port uint16, username string, password string, schemas conf.SchemasConf) (DbMetadata, error) {
	if len(schemas) == 0 {
		return nil, errors.New("no one schema isn't configured for loading db metadata")
	}
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema", username, password, host, port)
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("open mysql db for getting information_schema data failure: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Warn().Msg("Metadata db connection closed ungracefully")
		}
	}()

	dbMetadata := make(DbMetadata, len(schemas))
	for schema, tables := range schemas {
		schemaMetadata := make(SchemaMetadata, len(tables))
		for _, table := range tables {
			if _, ok := schemaMetadata[table]; ok {
				continue
			}
			tableMetadata, err := loadFields(db, schema, table)
			if err != nil {
				return nil, err
			}
			schemaMetadata[table] = tableMetadata
		}
		dbMetadata[schema] = schemaMetadata
	}

	return dbMetadata, nil
}

func (metadata DbMetadata) GetFields(schema string, table string) []string {
	schemaMetadata, ok := metadata[schema]
	if !ok {
		return nil
	}
	tableMetadata, ok := schemaMetadata[table]
	if !ok {
		return nil
	}
	return tableMetadata
}

func loadFields(db *sql.DB, schema string, table string) ([]string, error) {
	rows, err := db.Query(
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
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("scan query result for getting %s.%s table metadata failure: %w", schema, table, err)
		}

		fields = append(fields, columnName)
		i++
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("loaded field names for %s.%s table failure", schema, table)
	}

	logger.Info().Strs("fields", fields).Msgf("Loaded field names for %s.%s table", schema, table)

	return fields, nil
}
