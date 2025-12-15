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

package bean_test

import (
	"testing"

	"github.com/th2-net/th2-listener-mysql-binlog-go/component/bean"
)

func TestDeleteSplit(t *testing.T) {
	schema := randString()
	table := randString()
	fields, rows := randRowsM(randIntM(minWidth, maxWidth), randIntM(2, maxHeight))
	baseDelete := bean.NewDelete(schema, table, fields, rows)

	size := baseDelete.SizeBytes()
	testDelete := bean.Delete{Record: baseDelete.Record, Deleted: append(baseDelete.Deleted, baseDelete.Deleted...)}

	parts := testDelete.Split(size)
	if len(parts) != 2 {
		t.Fatalf("expected: 2, got: %d", len(parts))
	}
	parts = testDelete.Split(size / 2)
	if len(parts) < 4 {
		t.Fatalf("expected >= 4, got: %d", len(parts))
	}
}
