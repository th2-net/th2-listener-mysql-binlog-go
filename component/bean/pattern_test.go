/*
 Copyright 2025 Exactpro (Exactpro Systems Limited)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package bean

import "testing"

func TestExtractOperation(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		operation Operation
	}{
		{
			name:      "unknown",
			query:     ``,
			operation: unknownOperation,
		},
		{
			name:      "truncate",
			query:     `TRUNCATE TABLE users;`,
			operation: truncateOperation,
		},
		{
			name:      "truncate",
			query:     "TRUNCATE TABLE `users`;",
			operation: truncateOperation,
		},
		{
			name:      "truncate",
			query:     "TRUNCATE TABLE db1.users;",
			operation: truncateOperation,
		},
		{
			name:      "truncate",
			query:     "TRUNCATE TABLE `db1`.`users`;",
			operation: truncateOperation,
		},
		{
			name:      "truncate",
			query:     "TRUNCATE TABLE  db1.`users`  ;",
			operation: truncateOperation,
		},
		{
			name: "truncate",
			query: `TRUNCATE TABLE
			` + "`my_db`.`tbl_user`;",
			operation: truncateOperation,
		},
		{
			name:      "create table 1",
			query:     "CREATE TABLE users (id INT);",
			operation: createTableOperation,
		},
		{
			name:      "create table 2",
			query:     "CREATE TABLE `users` (id INT, name VARCHAR(50));",
			operation: createTableOperation,
		},
		{
			name:      "create table 3",
			query:     "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY);",
			operation: createTableOperation,
		},
		{
			name: "create table 4",
			query: "CREATE TABLE IF NOT EXISTS `mydb`.`users` (" + `
			    id INT AUTO_INCREMENT,
				name VARCHAR(255),
				PRIMARY KEY (id)
			);`,
			operation: createTableOperation,
		},
		{
			name:      "create table 5",
			query:     "CREATE TABLE test.users (col1 INT, col2 TEXT);",
			operation: createTableOperation,
		},
		{
			name: "create table 6",
			query: `CREATE TABLE
			` + "`schema1`.`table1`" + `
			(
				col1 INT,
				col2 VARCHAR(100)
			);`,
			operation: createTableOperation,
		},
		{
			name:      "drop table 1",
			query:     "DROP TABLE users;",
			operation: dropTableOperation,
		},
		{
			name:      "drop table 2",
			query:     "DROP TABLE IF EXISTS users;",
			operation: dropTableOperation,
		},
		{
			name:      "drop table 3",
			query:     "DROP TABLE IF EXISTS `users`;",
			operation: dropTableOperation,
		},
		{
			name:      "drop table 4",
			query:     "DROP TABLE `db1`.`users`;",
			operation: dropTableOperation,
		},
		{
			name:      "drop table 5",
			query:     "DROP TABLE db1.users;",
			operation: dropTableOperation,
		},
		{
			name: "drop table 6",
			query: `DROP TABLE
			IF EXISTS
			` + "`my_schema`.`tbl`;",
			operation: dropTableOperation,
		},
		{
			name:      "alter table 1",
			query:     "ALTER TABLE users ADD COLUMN age INT;",
			operation: alterTableOperation,
		},
		{
			name:      "alter table 2",
			query:     "ALTER TABLE `users` DROP COLUMN age;",
			operation: alterTableOperation,
		},
		{
			name:      "alter table 3",
			query:     "ALTER TABLE db1.users MODIFY COLUMN name VARCHAR(255);",
			operation: alterTableOperation,
		},
		{
			name:      "alter table 4",
			query:     "ALTER TABLE `db1`.`users` ADD INDEX idx_name (name);",
			operation: alterTableOperation,
		},
		{
			name: "alter table 5",
			query: `ALTER TABLE
    		` + "   `schema1`.`table1`" + `
			ADD
    			COLUMN col_new INT;`,
			operation: alterTableOperation,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			operation := ExtractOperation(tc.query)
			if tc.operation != operation {
				t.Fatalf("expected: %s, got: %s", tc.operation, operation)
			}
		})
	}
}
