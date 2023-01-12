package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

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
		panic(err)
	}
	closingFunctions = append(closingFunctions, func() { newFactory.Close() })

	var customConfig map[string]string
	newFactory.GetCustomConfiguration(&customConfig)

	boxConf := component.BoxConfiguration{MessageType: customConfig["messageType"]}
	messageType := boxConf.MessageType

	module, err := rabbitmq.ModuleID.GetModule(newFactory)
	if err != nil {
		panic("No module found")
	}

	log.Printf("Start listening for %v messages\n", messageType)

	var TypeListener message.ConformationMessageListener = component.MessageTypeListener{MessageType: messageType, Function: func(args ...interface{}) { fmt.Println("Found Message") }}

	monitor, err := module.MqMessageRouter.SubscribeWithManualAck(&TypeListener, "group")
	if err != nil {
		log.Fatalln("Error occured when subscribing")
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
		log.Println("Shutting Down")
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
