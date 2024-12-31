/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/th2-net/th2-common-go/pkg/common"
	queueApi "github.com/th2-net/th2-common-go/pkg/queue"

	p_buff "th2-grpc/th2_grpc_common"

	"github.com/rs/zerolog/log"
	"github.com/th2-net/th2-common-go/pkg/factory"
	"github.com/th2-net/th2-common-go/pkg/modules/prometheus"
	"github.com/th2-net/th2-common-go/pkg/modules/queue"
	utils "github.com/th2-net/th2-common-utils-go/pkg/event"
	"github.com/th2-net/th2-read-mysql-binlog-go/component"
	timestamp "google.golang.org/protobuf/types/known/timestamppb"
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

	var boxConf component.Configuration
	if err := newFactory.GetCustomConfiguration(&boxConf); err != nil {
		panic(err)
	}

	module, err := queue.ModuleID.GetModule(newFactory)
	if err != nil {
		panic(err)
	}

	// Create a root event
	rootEventID := utils.CreateEventID()
	err = module.GetEventRouter().SendAll(utils.CreateEventBatch(nil,
		&p_buff.Event{
			Id:                 rootEventID,
			ParentId:           nil,
			StartTimestamp:     timestamp.Now(),
			EndTimestamp:       nil,
			Status:             0,
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
		Str("component", "box_template_main").
		Msg("Created root report event for box")

	// Start listening for messages
	log.Info().
		Str("component", "box_template_main").
		Msg(fmt.Sprintf("Start listening for %v messages\n", boxConf.MessageType))

	typeListener := component.NewListener(
		rootEventID,
		module.GetMessageRouter(),
		module.GetEventRouter(),
		&boxConf,
		func(args ...interface{}) {},
	)

	monitor1, err := module.GetMessageRouter().SubscribeAll(typeListener, "group", "one")
	if err != nil {
		panic(err)
	}
	defer func(monitor1 queueApi.Monitor) {
		err := monitor1.Unsubscribe()
		if err != nil {
			log.Error().Err(err).
				Msg("cannot unsubscribe from message 'one' pin")
		}
	}(monitor1)
	monitor2, err := module.GetMessageRouter().SubscribeAll(typeListener, "group", "two")
	if err != nil {
		panic(err)
	}
	defer func(monitor2 queueApi.Monitor) {
		err := monitor2.Unsubscribe()
		if err != nil {
			log.Error().Err(err).
				Msg("cannot unsubscribe from message 'two' pin")
		}
	}(monitor2)

	monitor3, err := module.GetMessageRouter().SubscribeAll(typeListener, "group", "three")
	if err != nil {
		panic(err)
	}
	defer func(monitor3 queueApi.Monitor) {
		err := monitor3.Unsubscribe()
		if err != nil {
			log.Error().Err(err).
				Msg("cannot unsubscribe from message 'three' pin")
		}
	}(monitor3)

	promMod, err := prometheus.ModuleID.GetModule(newFactory)
	if err != nil {
		panic(err)
	}
	livenessMonitor := promMod.GetLivenessArbiter().RegisterMonitor("liveness_monitor")
	readinessMonitor := promMod.GetReadinessArbiter().RegisterMonitor("readiness_monitor")
	livenessMonitor.Enable()
	readinessMonitor.Enable()

	// Start listening for shutdown signal
	select {
	case s := <-sigCh:
		log.Info().Interface("signal", s).Msg("shutdown component because of user signal")
		return
	}

}
