package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"

	"github.com/rs/zerolog/log"
	component "github.com/th2-net/th2-box-template-go/component"
	"github.com/th2-net/th2-common-go/schema/factory"
	rabbitmq "github.com/th2-net/th2-common-go/schema/modules/mqModule"
	"github.com/th2-net/th2-common-go/schema/queue/message"
)

func main() {

	var closingFunctions []func()
	wait := shutdown(&closingFunctions)

	newFactory := factory.NewFactory()
	if err := newFactory.Register(rabbitmq.NewRabbitMQModule); err != nil {
		log.Fatal().Err(err).Msg("Registering RabbitMQModule failed")
	}
	closingFunctions = append(closingFunctions, func() { newFactory.Close() })

	var customConfig map[string]string
	newFactory.GetCustomConfiguration(&customConfig)

	boxConf := component.BoxConfiguration{MessageType: customConfig["messageType"]}
	messageType := boxConf.MessageType

	module, err := rabbitmq.ModuleID.GetModule(newFactory)
	if err != nil {
		log.Fatal().Err(err).Msg("Getting RabbitMQ module failed")
	}

	log.Info().Msg(fmt.Sprintf("Start listening for %v messages\n", messageType))

	var TypeListener message.ConformationMessageListener = component.MessageTypeListener{MessageType: messageType, Function: func(args ...interface{}) { fmt.Println("Found Message") }}

	monitor, err := module.MqMessageRouter.SubscribeWithManualAck(&TypeListener, "group")
	if err != nil {
		log.Fatal().Err(err).Msg("Subscribing listener to the module failed")
	}
	closingFunctions = append(closingFunctions, func() { monitor.Unsubscribe() })

	// Start listening for shutdown signal
	<-wait
}

func shutdown(closes *[]func()) <-chan bool {
	wait := make(chan bool)
	go func() {
		ch := make(chan os.Signal, 1)
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
