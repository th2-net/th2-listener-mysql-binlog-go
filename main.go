package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rs/zerolog/log"
	component "github.com/th2-net/th2-box-template-go/component"
	"github.com/th2-net/th2-common-go/schema/factory"
	rabbitmq "github.com/th2-net/th2-common-go/schema/modules/mqModule"
	"github.com/th2-net/th2-common-go/schema/queue/message"
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
	rootEventID := component.CreateEventID()
	module.MqEventRouter.SendAll(component.CreateEventBatch(
		nil, component.CreateEvent(
			rootEventID, nil, timestamp.Now(), timestamp.Now(), 0, "Root Event", "message", nil, nil),
	), "publish")
	log.Info().Msg("Created root report event for box")

	// Start listening for messages
	log.Info().Msg(fmt.Sprintf("Start listening for %v messages\n", boxConf.MessageType))

	var TypeListener message.MessageListener = component.NewListener(
		rootEventID,
		module,
		&boxConf,
		wait,
		ch,
		func(args ...interface{}) { fmt.Println("Found Message") },
	)

	monitor, err := module.MqMessageRouter.SubscribeAll(&TypeListener, "group")
	closingFunctions = append(closingFunctions, func() { monitor.Unsubscribe() })
	if err != nil {
		ch <- syscall.SIGINT
		<-wait
		log.Fatal().Err(err).Msg("Subscribing listener to the module failed")
	}

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
