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

	"github.com/th2-net/th2-common-go/pkg/common"
	"github.com/th2-net/th2-common-mq-batcher-go/pkg/batcher"
	"github.com/th2-net/th2-lwdp-grpc-fetcher-go/pkg/fetcher"

	proto "github.com/th2-net/th2-grpc-common-go"

	"github.com/th2-net/th2-common-go/pkg/factory"
	"github.com/th2-net/th2-common-go/pkg/log"
	"github.com/th2-net/th2-common-go/pkg/modules/grpc"
	"github.com/th2-net/th2-common-go/pkg/modules/prometheus"
	"github.com/th2-net/th2-common-go/pkg/modules/queue"
	utils "github.com/th2-net/th2-common-utils-go/pkg/event"
	"github.com/th2-net/th2-listener-mysql-binlog-go/component"
	conf "github.com/th2-net/th2-listener-mysql-binlog-go/component/configuration"
	"github.com/th2-net/th2-listener-mysql-binlog-go/component/listener"
)

const (
	PROTOCOL string = "json"
)

var (
	logger = log.ForComponent("main")
)

func main() {
	newFactory := factory.New()
	defer func(newFactory common.Factory) {
		if err := newFactory.Close(); err != nil {
			logger.Error().Err(err).Msg("cannot close factory")
		}
	}(newFactory)
	if err := newFactory.Register(queue.NewRabbitMqModule); err != nil {
		logger.Panic().Err(err).Msg("'RabbitMq' module can't be registered")
	}
	if err := newFactory.Register(grpc.NewModule); err != nil {
		logger.Panic().Err(err).Msg("'gRPC' module can't be registered")
	}

	var conf conf.Configuration
	if err := newFactory.GetCustomConfiguration(&conf); err != nil {
		logger.Panic().Err(err).Msg("Getting custom config failure")
	}
	group, alias, err := getStreamParameters(conf)
	if err != nil {
		logger.Panic().Err(err).Msg("Getting stream parameters from conf failure")
	}

	mqMod, err := queue.ModuleID.GetModule(newFactory)
	if err != nil {
		logger.Panic().Err(err).Msg("Getting 'RabbitMq' module failure")
	}
	grpcMod, err := grpc.ModuleID.GetModule(newFactory)
	if err != nil {
		logger.Panic().Err(err).Msg("Getting 'gRPC' module failure")
	}

	componentConf := newFactory.GetBoxConfig()
	rootEventID := utils.CreateEventID(componentConf.Book, componentConf.Name)
	err = mqMod.GetEventRouter().SendAll(utils.CreateEventBatch(nil,
		&proto.Event{
			Id:                 rootEventID,
			ParentId:           nil,
			EndTimestamp:       nil,
			Status:             proto.EventStatus_SUCCESS,
			Name:               "Root Event",
			Type:               "Message",
			Body:               nil,
			AttachedMessageIds: nil,
		},
	))
	if err != nil {
		logger.Panic().Err(err).Msg("Sending root event failure")
	}
	logger.Info().
		Str("component", "listener_mysql_binlog_main").
		Msg("Created root report event for listener-mysql-binlog")

	maxSize := batcher.DefaultBatchSize
	batcher, err := batcher.NewMessageBatcher(mqMod.GetMessageRouter(), batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           componentConf.Book,
			BatchSizeBytes: maxSize,
		},
		Group:    group,
		Protocol: PROTOCOL,
	})
	if err != nil {
		logger.Panic().Err(err).Msg("Creating message batcher failure")
	}
	defer func(closer io.Closer) {
		if err := closer.Close(); err != nil {
			logger.Error().Err(err).Msg("cannot close message batcher")
		}
	}(batcher)

	promMod, err := prometheus.ModuleID.GetModule(newFactory)
	if err != nil {
		logger.Panic().Err(err).Msg("Getting 'PrometheusModule' failure")
	}
	livenessMonitor := promMod.GetLivenessArbiter().RegisterMonitor("liveness_monitor")
	readinessMonitor := promMod.GetReadinessArbiter().RegisterMonitor("readiness_monitor")
	livenessMonitor.Enable()
	defer livenessMonitor.Disable()
	readinessMonitor.Enable()
	defer readinessMonitor.Disable()

	listener, err := listener.New(batcher, conf.Connection, conf.Schemas, componentConf.Book, group, alias, int(maxSize))
	if err != nil {
		logger.Panic().Err(err).Msg("Listener creation failure")
	}

	lwdp, err := fetcher.NewLwdpFetcher(grpcMod.GetRouter())
	if err != nil {
		logger.Panic().Err(err).Msg("Creating lwdp fetcher failure")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := listener.Listen(ctx, lwdp); err != nil {
		logger.Panic().Err(err).Msg("Reading binlog events failure")
	}

	logger.Info().Msg("shutdown component")
}

func getStreamParameters(conf conf.Configuration) (string, string, error) {
	alias := conf.Alias
	if len(alias) == 0 {
		return "", "", errors.New("alias can't be empty")
	}
	group := component.OrDefaultIfEmpty(conf.Group, conf.Alias)
	return group, alias, nil
}
