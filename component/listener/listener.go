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

package listener

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/th2-net/th2-common-go/pkg/log"
	b "github.com/th2-net/th2-common-mq-batcher-go/pkg/batcher"
	proto "github.com/th2-net/th2-grpc-common-go"
	"github.com/th2-net/th2-listener-mysql-binlog-go/component/bean"
	conf "github.com/th2-net/th2-listener-mysql-binlog-go/component/configuration"
	"github.com/th2-net/th2-listener-mysql-binlog-go/component/database"
	"github.com/th2-net/th2-lwdp-grpc-fetcher-go/pkg/fetcher"
)

const (
	logNameProp      = "name"
	logPosProp       = "pos"
	logSeqNumProp    = "seq"
	logTimestampProp = "timestamp"

	msgProtocol = "json"

	// The 1236 error can occur due to incorrect or missing log files or positions in replication.
	mysql1236              = 1236
	mysqlIncorrectBinfile  = "Could not find first log file name in binary log index file"
	mysqlIncorrectPosition = "Client requested source to start replication from position > file size"
)

var (
	logger = log.ForComponent("listener")
)

type newBean func(schema string, table string, fields []string, rows [][]interface{}) interface{}

type Listener struct {
	dbMetadata database.DbMetadata
	batcher    b.MqBatcher[b.MessageArguments]
	conf       conf.Connection
	book       string
	group      string
	alias      string
}

func New(batcher b.MqBatcher[b.MessageArguments], conf conf.Connection, schemas conf.SchemasConf, book string, group string, alias string) (*Listener, error) {
	dbMetadata, err := database.LoadMetadata(conf.Host, conf.Port, conf.Username, conf.Password, schemas)
	if err != nil {
		return nil, fmt.Errorf("loading schema metadata ta failure: %w", err)
	}
	return &Listener{
		dbMetadata: dbMetadata,
		conf:       conf,
		batcher:    batcher,
		book:       book,
		group:      group,
		alias:      alias,
	}, nil
}

func (r *Listener) Listen(ctx context.Context, lwdp fetcher.LwdpFetcher) error {
	filename, pos, err := r.loadPreviousState(ctx, lwdp)
	if err != nil {
		return fmt.Errorf("getting the last grouped message failure: %w", err)
	}
	err = r.listen(ctx, filename, pos)
	var mysqlErr *mysql.MyError
	if errors.As(err, &mysqlErr) {
		logger.Error().Err(mysqlErr).Msg("Mysql error")
		if mysqlErr.Code == mysql1236 {
			switch mysqlErr.Message {
			case mysqlIncorrectBinfile:
				logger.Warn().Str("filename", filename).
					Msg("Replication binfile incorrect, try to use empty parameters")
				err = r.listen(ctx, "", 0)
			case mysqlIncorrectPosition:
				logger.Warn().Str("filename", filename).Uint32("position", pos).
					Msg("Replication binfile incorrect, try to use 0 position")
				err = r.listen(ctx, filename, 0)
			default:
				logger.Warn().Str("filename", filename).Uint32("position", pos).
					Msg("Unknown mysql error message, try to use empty parameters")
				err = r.listen(ctx, "", 0)
			}
		}
	}

	return err
}

func (r *Listener) listen(ctx context.Context, filename string, pos uint32) error {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     r.conf.Host,
		Port:     r.conf.Port,
		User:     r.conf.Username,
		Password: r.conf.Password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	streamer, err := syncer.StartSync(mysql.Position{Name: filename, Pos: pos})
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

	newInsert := func(schema string, table string, fields []string, rows [][]any) any {
		return bean.NewInsert(schema, table, fields, rows)
	}
	newUpdate := func(schema string, table string, fields []string, rows [][]any) any {
		return bean.NewUpdate(schema, table, fields, rows)
	}
	newDelete := func(schema string, table string, fields []string, rows [][]any) any {
		return bean.NewDelete(schema, table, fields, rows)
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
		case replication.QUERY_EVENT:
			if err := r.processQueryEvent(e, logName, logSeqNum, logTimestamp); err != nil {
				return fmt.Errorf("processing query event failure: %w", err)
			}
		case replication.WRITE_ROWS_EVENTv1,
			replication.WRITE_ROWS_EVENTv2:
			if err := r.processRowsEvent(e, logName, logSeqNum, logTimestamp, newInsert); err != nil {
				return fmt.Errorf("processing write event failure: %w", err)
			}
		case replication.UPDATE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv2:
			if err := r.processRowsEvent(e, logName, logSeqNum, logTimestamp, newUpdate); err != nil {
				return fmt.Errorf("processing update event failure: %w", err)
			}
		case replication.DELETE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv2:
			if err := r.processRowsEvent(e, logName, logSeqNum, logTimestamp, newDelete); err != nil {
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

func (r *Listener) Close() error {
	return nil
}

func (r *Listener) loadPreviousState(ctx context.Context, lwdp fetcher.LwdpFetcher) (string, uint32, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(1)*time.Minute)
	defer cancel()
	msg, err := lwdp.GetLastGroupedMessage(ctx, r.book, r.group, r.alias, proto.Direction_FIRST, fetcher.LwdpBase64Format)
	if err != nil {
		return "", 0, err
	}
	if msg == nil {
		logger.Info().Str("book", r.book).Str("alias", r.alias).Msg("no previous messages")
		return "", 0, nil
	}

	logName, ok := msg.MessageProperties[logNameProp]
	if !ok {
		logger.Warn().Any("message-id", msg.MessageId).Any("properties", msg.MessageProperties).Str("target", logNameProp).Msg("required property isn't found")
		return "", 0, nil
	}
	logPos, ok := msg.MessageProperties[logPosProp]
	if !ok {
		logger.Warn().Any("message-id", msg.MessageId).Any("properties", msg.MessageProperties).Str("target", logPosProp).Msg("required property isn't found")
		return "", 0, nil
	}
	num, err := strconv.ParseUint(logPos, 10, 32)
	if err != nil {
		logger.Warn().Any("message-id", msg.MessageId).Str("target", logPosProp).Str("value", logPos).Err(err).Msg("log position has incorrect format")
		return logName, 0, nil
	}
	logger.Info().Any("message-id", msg.MessageId).Str("log-name", logName).Uint64("log-pos", num).Msg("loaded previous state")
	return logName, uint32(num), nil
}

func (r *Listener) processRowsEvent(event *replication.BinlogEvent, logName string, logSeqNum int64, logTimestamp time.Time, createBean newBean) error {
	rowsEvent, ok := event.Event.(*replication.RowsEvent)
	if !ok {
		return fmt.Errorf("cast event failure")
	}
	schema := string(rowsEvent.Table.Schema)
	table := string(rowsEvent.Table.Table)
	fields := r.dbMetadata.GetFields(schema, table)
	if len(fields) == 0 {
		logger.Trace().Str("schema", schema).Str("table", table).Msg("Event skipped")
		return nil
	}
	bean := createBean(schema, table, fields, rowsEvent.Rows)
	metadata := createMetadata(schema, table, logName, event.Header.LogPos, logSeqNum, logTimestamp)
	if err := r.batchMessage(bean, r.alias, metadata); err != nil {
		return fmt.Errorf("batching event failure: %w", err)
	}
	return nil
}

func (r *Listener) processQueryEvent(event *replication.BinlogEvent, logName string, logSeqNum int64, logTimestamp time.Time) error {
	queryEvent, ok := event.Event.(*replication.QueryEvent)
	if !ok {
		return fmt.Errorf("cast event failure")
	}
	schema := string(queryEvent.Schema)
	query := string(queryEvent.Query)
	exSchema, exTable, operation, ok := bean.ExtractOperation(query)
	if !ok {
		return nil
	}
	if schema == "" && exSchema != "" {
		schema = exSchema
	}
	bean := bean.NewQuery(schema, exTable, query, operation)
	metadata := createMetadata(schema, "", logName, event.Header.LogPos, logSeqNum, logTimestamp)
	if err := r.batchMessage(bean, r.alias, metadata); err != nil {
		return fmt.Errorf("batching event failure: %w", err)
	}
	return nil
}

func (r *Listener) batchMessage(bean any, alias string, metadata map[string]string) error {
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
	logger.Trace().Msg("Message is sent to batcher")
	return nil
}

func logEvent(event *replication.BinlogEvent) {
	if logger.Debug().Enabled() {
		buf := new(bytes.Buffer)
		event.Dump(buf)
		logger.Debug().Str("event", buf.String()).Msg("read event")
	}
}

func createMetadata(schema string, table string, logName string, logPos uint32, logSeqNum int64, logTimestamp time.Time) map[string]string {
	return map[string]string{
		logNameProp:      logName,
		logPosProp:       fmt.Sprint(logPos),
		logSeqNumProp:    fmt.Sprint(logSeqNum),
		logTimestampProp: fmt.Sprint(logTimestamp.UnixNano()),
	}
}
