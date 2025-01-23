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

package read

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/rs/zerolog/log"
	b "github.com/th2-net/th2-common-mq-batcher-go/pkg/batcher"
	"github.com/th2-net/th2-read-mysql-binlog-go/component/bean"
	conf "github.com/th2-net/th2-read-mysql-binlog-go/component/configuration"
	"github.com/th2-net/th2-read-mysql-binlog-go/component/database"
)

const (
	logNameProp        = "name"
	logPosProp         = "pos"
	logSeqNumProp      = "seq"
	logTimestampProp   = "timestamp"
	logEventSchemaProp = "schema"
	logEventTableProp  = "table"

	msgProtocol = "json"
)

type newBean func(fields []string, rows [][]interface{}) interface{}

type Read struct {
	dbMetadata database.DbMetadata
	batcher    b.MqBatcher[b.MessageArguments]
	conf       conf.Connection
	alias      string
}

func NewRead(batcher b.MqBatcher[b.MessageArguments], conf conf.Connection, schemas conf.SchemasConf, alias string) (*Read, error) {
	dbMetadata, err := database.LoadMetadata(conf.Host, conf.Port, conf.Username, conf.Password, schemas)
	if err != nil {
		return nil, fmt.Errorf("loading schema metadata ta failure: %w", err)
	}
	return &Read{
		dbMetadata: *dbMetadata,
		conf:       conf,
		batcher:    batcher,
		alias:      alias,
	}, nil
}

func (r *Read) Read(ctx context.Context) error {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     r.conf.Host,
		Port:     r.conf.Port,
		User:     r.conf.Username,
		Password: r.conf.Password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	streamer, err := syncer.StartSync(mysql.Position{Name: "", Pos: uint32(0)})
	if err != nil {
		return fmt.Errorf("starting sync binlog failure: %w", err)
	}

	// or you can start a GTID replication like
	// gtidSet, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set is like this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2" and uses mysql.MySQLFlavor
	// the mariadb GTID set is like this "0-1-100" and uses mysql.MariaDBFlavor

	var logName string
	var logSeqNum int64
	var logTimestamp time.Time

	newInsert := func(fields []string, rows [][]interface{}) interface{} {
		return bean.NewInsert(fields, rows)
	}
	newUpdate := func(fields []string, rows [][]interface{}) interface{} {
		return bean.NewUpdate(fields, rows)
	}
	newDelete := func(fields []string, rows [][]interface{}) interface{} {
		return bean.NewDelete(fields, rows)
	}

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("checking context err failure: %w", err)
		}

		e, err := streamer.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("getting binlog event failure: %w", err)
		}

		logEvent(e)
		// Dump event
		eventType := e.Header.EventType
		switch eventType {
		// case replication.QUERY_EVENT:
		// 	queryEvent := e.Event.(*replication.QueryEvent)
		// 	fmt.Printf("type: %s, query: %s\n", eventType, queryEvent.Query)
		// 	break
		case replication.WRITE_ROWS_EVENTv1,
			replication.WRITE_ROWS_EVENTv2:
			if err := r.processEvent(e, logName, logSeqNum, logTimestamp, newInsert); err != nil {
				return fmt.Errorf("processing write event failure: %w", err)
			}
		case replication.UPDATE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv2:
			r.processEvent(e, logName, logSeqNum, logTimestamp, newUpdate)
			if err := r.processEvent(e, logName, logSeqNum, logTimestamp, newInsert); err != nil {
				return fmt.Errorf("processing update event failure: %w", err)
			}
		case replication.DELETE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv2:
			r.processEvent(e, logName, logSeqNum, logTimestamp, newDelete)
			if err := r.processEvent(e, logName, logSeqNum, logTimestamp, newInsert); err != nil {
				return fmt.Errorf("processing delete event failure: %w", err)
			}
		case replication.ANONYMOUS_GTID_EVENT:
			event := e.Event.(*replication.GTIDEvent)
			logSeqNum = event.SequenceNumber
			logTimestamp = event.ImmediateCommitTime()
		case replication.ROTATE_EVENT:
			event := e.Event.(*replication.RotateEvent)
			logName = string(event.NextLogName)
		}
	}
}

func (r *Read) Close() error {
	return nil
}

func (r *Read) processEvent(event *replication.BinlogEvent, logName string, logSeqNum int64, logTimestamp time.Time, createBean newBean) error {
	rowsEvent := event.Event.(*replication.RowsEvent)
	schema := string(rowsEvent.Table.Schema)
	table := string(rowsEvent.Table.Table)
	fields := r.dbMetadata.GetFields(schema, table)
	if len(fields) == 0 {
		log.Trace().Str("schema", schema).Str("table", table).Msg("Event skipped")
		return nil
	}
	bean := createBean(fields, rowsEvent.Rows)
	metadata := createMetadata(schema, table, logName, event.Header.LogPos, logSeqNum, logTimestamp)
	if err := r.batchMessage(bean, r.alias, metadata); err != nil {
		return fmt.Errorf("batching event failure: %w", err)
	}
	return nil
}

func (r *Read) batchMessage(bean any, alias string, metadata map[string]string) error {
	data, err := json.Marshal(bean)
	if err != nil {
		return fmt.Errorf("marshaling failure: %w", err)
	}
	if err := r.batcher.Send(data, b.MessageArguments{
		Metadata:  metadata,
		Alias:     alias,
		Direction: b.InDirection,
		Protocol:  msgProtocol,
	}); err != nil {
		return fmt.Errorf("batching failure: %w", err)
	}
	log.Trace().Msg("Message is sent to batcher")
	return nil
}

func logEvent(event *replication.BinlogEvent) {
	if log.Debug().Enabled() {
		buf := new(bytes.Buffer)
		event.Dump(buf)
		log.Debug().Str("event", buf.String()).Msg("read event")
	}
}

func createMetadata(schema string, table string, logName string, logPos uint32, logSeqNum int64, logTimestamp time.Time) map[string]string {
	return map[string]string{
		logNameProp:        logName,
		logPosProp:         fmt.Sprint(logPos),
		logSeqNumProp:      fmt.Sprint(logSeqNum),
		logTimestampProp:   fmt.Sprint(logTimestamp.UnixNano()),
		logEventSchemaProp: schema,
		logEventTableProp:  table,
	}
}
