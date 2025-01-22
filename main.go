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

package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/signal"

	"github.com/google/uuid"
	"github.com/th2-net/th2-common-go/pkg/common"
	"github.com/th2-net/th2-common-mq-batcher-go/pkg/batcher"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/th2-net/th2-common-go/pkg/common/grpc/th2_grpc_common"

	"github.com/rs/zerolog/log"
	"github.com/th2-net/th2-common-go/pkg/factory"
	"github.com/th2-net/th2-common-go/pkg/modules/prometheus"
	"github.com/th2-net/th2-common-go/pkg/modules/queue"
	utils "github.com/th2-net/th2-common-utils-go/pkg/event"
	"github.com/th2-net/th2-read-mysql-binlog-go/component"
)

const (
	PROTOCOL string = "json"
)

func main() {
	newFactory := factory.New()
	defer func(newFactory common.Factory) {
		if err := newFactory.Close(); err != nil {
			log.Error().Err(err).Msg("cannot close factory")
		}
	}(newFactory)
	if err := newFactory.Register(queue.NewRabbitMqModule); err != nil {
		log.Panic().Err(err).Msg("'NewRabbitMqModule' can't be registered")
	}

	var conf component.Configuration
	if err := newFactory.GetCustomConfiguration(&conf); err != nil {
		log.Panic().Err(err).Msg("Getting custom config failure")
	}
	group, alias, err := getStreamParameters(conf)
	if err != nil {
		log.Panic().Err(err).Msg("Getting stream parameters from conf failure")
	}

	module, err := queue.ModuleID.GetModule(newFactory)
	if err != nil {
		log.Panic().Err(err).Msg("Getting 'NewRabbitMqModule' failure")
	}

	// Create a root event TODO: use utils.CreateEventID(book, scope) method
	componentConf := newFactory.GetBoxConfig()
	rootEventID := &th2_grpc_common.EventID{
		BookName:       componentConf.Book,
		Scope:          componentConf.Name,
		StartTimestamp: timestamppb.Now(),
		Id:             uuid.New().String(),
	}
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
		log.Panic().Err(err).Msg("Sending root event failure")
	}
	log.Info().
		Str("component", "read_mysql_binlog_main").
		Msg("Created root report event for read-mysql-binlog")

	batcher, err := batcher.NewMessageBatcher(module.GetMessageRouter(), batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book: rootEventID.BookName,
		},
		Group:    group,
		Protocol: PROTOCOL,
	})
	if err != nil {
		log.Panic().Err(err).Msg("Creating message batcher failure")
	}
	defer func(closer io.Closer) {
		if err := closer.Close(); err != nil {
			log.Error().Err(err).Msg("cannot close message batcher")
		}
	}(batcher)

	promMod, err := prometheus.ModuleID.GetModule(newFactory)
	if err != nil {
		log.Panic().Err(err).Msg("Getting 'PrometheusModule' failure")
	}
	livenessMonitor := promMod.GetLivenessArbiter().RegisterMonitor("liveness_monitor")
	readinessMonitor := promMod.GetReadinessArbiter().RegisterMonitor("readiness_monitor")
	livenessMonitor.Enable()
	defer livenessMonitor.Disable()
	readinessMonitor.Enable()
	defer readinessMonitor.Disable()

	read, err := component.NewRead(batcher, conf.Connection, conf.Schemas, alias)
	if err != nil {
		log.Panic().Err(err).Msg("Read creation failure")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := read.Read(ctx); err != nil {
		log.Panic().Err(err).Msg("Reading binlog events failure")
	}

	log.Info().Msg("shutdown component")
}

func getStreamParameters(conf component.Configuration) (string, string, error) {
	alias := conf.Alias
	if len(alias) == 0 {
		return "", "", errors.New("alias can't be empty")
	}
	group := component.OrDefaultIfEmpty(conf.Group, conf.Alias)
	return group, alias, nil
}
