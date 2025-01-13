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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/th2-net/th2-common-go/pkg/common"

	"github.com/th2-net/th2-common-go/pkg/common/grpc/th2_grpc_common"

	"github.com/rs/zerolog/log"
	"github.com/th2-net/th2-common-go/pkg/factory"
	"github.com/th2-net/th2-common-go/pkg/modules/prometheus"
	"github.com/th2-net/th2-common-go/pkg/modules/queue"
	utils "github.com/th2-net/th2-common-utils-go/pkg/event"
	"github.com/th2-net/th2-read-mysql-binlog-go/component"
	"github.com/th2-net/th2-read-mysql-binlog-go/component/database"
	"github.com/th2-net/th2-read-mysql-binlog-go/component/message"
)

func main() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	newFactory := factory.New()
	defer func(newFactory common.Factory) {
		err := newFactory.Close()
		if err != nil {
			log.Error().Err(err).
				Msg("cannot close factory")
		}
	}(newFactory)
	if err := newFactory.Register(queue.NewRabbitMqModule); err != nil {
		panic(err)
	}

	var conf component.Configuration
	if err := newFactory.GetCustomConfiguration(&conf); err != nil {
		panic(err)
	}

	module, err := queue.ModuleID.GetModule(newFactory)
	if err != nil {
		panic(err)
	}

	// Create a root event
	rootEventID := utils.CreateEventID()
	err = module.GetEventRouter().SendAll(utils.CreateEventBatch(nil,
		&th2_grpc_common.Event{
			Id:                 rootEventID,
			ParentId:           nil,
			EndTimestamp:       nil,
			Status:             th2_grpc_common.EventStatus_SUCCESS,
			Name:               "Root Event",
			Type:               "Message",
			Body:               nil,
			AttachedMessageIds: nil,
		},
	))
	if err != nil {
		panic(err)
	}
	log.Info().
		Str("component", "read_mysql_binlog_main").
		Msg("Created root report event for read-mysql-binlog")

	promMod, err := prometheus.ModuleID.GetModule(newFactory)
	if err != nil {
		panic(err)
	}
	livenessMonitor := promMod.GetLivenessArbiter().RegisterMonitor("liveness_monitor")
	readinessMonitor := promMod.GetReadinessArbiter().RegisterMonitor("readiness_monitor")
	livenessMonitor.Enable()
	readinessMonitor.Enable()

	read(conf.Connection)

	// Start listening for shutdown signal
	s := <-sigCh
	log.Info().Interface("signal", s).Msg("shutdown component because of user signal")
}

func read(conf component.Connection) {
	metadata, err := database.CreateMetadata(conf.Host, conf.Port, conf.Username, conf.Password)
	if err != nil {
		log.Error().Err(err).Msg("Connect to database failure")
		panic(err)
	}
	defer metadata.Close()

	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     conf.Host,
		Port:     conf.Port,
		User:     conf.Username,
		Password: conf.Password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	streamer, err := syncer.StartSync(mysql.Position{Name: "", Pos: uint32(0)})
	if err != nil {
		log.Error().Err(err).Msg("Start sync binlog failure")
		panic(err)
	}

	// or you can start a GTID replication like
	// gtidSet, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set is like this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2" and uses mysql.MySQLFlavor
	// the mariadb GTID set is like this "0-1-100" and uses mysql.MariaDBFlavor

	var logName string
	var seqNum int64
	var timestamp time.Time

	for {
		e, _ := streamer.GetEvent(context.Background())
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
			event := e.Event.(*replication.RowsEvent)
			insert, err := message.CreateInsert(metadata, message.Header{LogName: logName, LogPos: e.Header.LogPos, SeqNum: seqNum, Timestamp: timestamp}, event)
			if err != nil {
				log.Error().Err(err)
				panic(err)
			}
			logMessage(insert)
		case replication.UPDATE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv2:
			event := e.Event.(*replication.RowsEvent)
			update, err := message.CreateUpdate(metadata, message.Header{LogName: logName, LogPos: e.Header.LogPos, SeqNum: seqNum, Timestamp: timestamp}, event)
			if err != nil {
				log.Error().Err(err)
				panic(err)
			}
			logMessage(update)
		case replication.DELETE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv2:
			event := e.Event.(*replication.RowsEvent)
			delete, err := message.CreateDelete(metadata, message.Header{LogName: logName, LogPos: e.Header.LogPos, SeqNum: seqNum, Timestamp: timestamp}, event)
			if err != nil {
				log.Error().Err(err)
				panic(err)
			}
			logMessage(delete)
		case replication.ANONYMOUS_GTID_EVENT:
			event := e.Event.(*replication.GTIDEvent)
			seqNum = event.SequenceNumber
			timestamp = event.ImmediateCommitTime()
		case replication.ROTATE_EVENT:
			event := e.Event.(*replication.RotateEvent)
			logName = string(event.NextLogName)
		}
	}
}

func logMessage(message interface{}) {
	log.Trace().Any("message", message).Msg("message received")
}

func logEvent(event *replication.BinlogEvent) {
	if log.Debug().Enabled() {
		buf := new(bytes.Buffer)
		event.Dump(buf)
		log.Debug().Str("event", buf.toString()).Msg("read event")
	}
}
