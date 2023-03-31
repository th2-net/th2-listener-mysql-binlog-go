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
	"sync"
	"syscall"

	p_buff "th2-grpc/th2_grpc_common"

	"github.com/rs/zerolog/log"
	component "github.com/th2-net/th2-box-template-go/component"
	"github.com/th2-net/th2-common-go/schema/factory"
	promModule "github.com/th2-net/th2-common-go/schema/modules/PrometheusModule"
	rabbitmq "github.com/th2-net/th2-common-go/schema/modules/mqModule"
	"github.com/th2-net/th2-common-go/schema/queue/message"
	utils "github.com/th2-net/th2-common-utils-go/th2_common_utils"
	timestamp "google.golang.org/protobuf/types/known/timestamppb"
)

func main() {

	var closingFunctions []func()
	ch := make(chan os.Signal, 1)
	wait := shutdown(&closingFunctions, ch)

	newFactory := factory.NewFactory()
	closingFunctions = append(closingFunctions, func() { newFactory.Close() })
	if err := newFactory.Register(rabbitmq.NewRabbitMQModule); err != nil {
		ch <- syscall.SIGINT
		<-wait
		log.Fatal().Err(err).Msg("Registering RabbitMQModule failed")
	}

	var boxConf component.BoxConfiguration
	newFactory.GetCustomConfiguration(&boxConf)

	module, err := rabbitmq.ModuleID.GetModule(newFactory)
	if err != nil {
		ch <- syscall.SIGINT
		<-wait
		log.Fatal().Err(err).Msg("Getting RabbitMQ module failed")
	}

	// Create a root event
	rootEventID := utils.CreateEventID()
	module.MqEventRouter.SendAll(utils.CreateEventBatch(nil,
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
	), "event")
	log.Info().Msg("Created root report event for box")

	// Start listening for messages
	log.Info().Msg(fmt.Sprintf("Start listening for %v messages\n", boxConf.MessageType))

	var TypeListener message.MessageListener = component.NewListener(
		rootEventID,
		module,
		&boxConf,
		func(args ...interface{}) { fmt.Println("Found Message with ID: ", args) },
	)

	monitor1, err1 := module.MqMessageRouter.SubscribeAll(&TypeListener, "group", "one")
	closingFunctions = append(closingFunctions, func() { monitor1.Unsubscribe() })
	if err1 != nil {
		ch <- syscall.SIGINT
		<-wait
		log.Fatal().Err(err).Msg("Subscribing listener to the module failed")
	}
	monitor2, err2 := module.MqMessageRouter.SubscribeAll(&TypeListener, "group", "two")
	closingFunctions = append(closingFunctions, func() { monitor2.Unsubscribe() })
	if err2 != nil {
		ch <- syscall.SIGINT
		<-wait
		log.Fatal().Err(err).Msg("Subscribing listener to the module failed")
	}

	monitor3, err3 := module.MqMessageRouter.SubscribeAll(&TypeListener, "group", "three")
	closingFunctions = append(closingFunctions, func() { monitor3.Unsubscribe() })
	if err3 != nil {
		ch <- syscall.SIGINT
		<-wait
		log.Fatal().Err(err).Msg("Subscribing listener to the module failed")
	}

	promMod, err := promModule.ModuleID.GetModule(newFactory)
	if err != nil {
		ch <- syscall.SIGINT
		<-wait
		log.Fatal().Err(err).Msg("Getting Prometheus module failed")
	}
	livenessMonitor := promMod.LivenessArbiter.RegisterMonitor("liveness_monitor")
	readinessMonitor := promMod.ReadinessArbiter.RegisterMonitor("readiness_monitor")
	livenessMonitor.Enable()
	readinessMonitor.Enable()
	closingFunctions = append(closingFunctions, func() { promMod.Close() })

	// Start listening for shutdown signal
	<-wait

}

func shutdown(closes *[]func(), ch chan os.Signal) <-chan bool {
	wait := make(chan bool)
	go func() {
		signal.Notify(ch, os.Interrupt)
		<-ch
		log.Info().Msg("Shutting Down")
		var wg sync.WaitGroup
		for _, closeFunc := range *closes {
			wg.Add(1)
			clsFunc := closeFunc
			go func() {
				defer wg.Done()
				clsFunc()
			}()
		}
		wg.Wait()
		close(wait)
	}()
	return wait
}
