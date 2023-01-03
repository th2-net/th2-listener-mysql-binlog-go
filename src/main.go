package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/th2-net/th2-common-go/schema/factory"
	rabbitmq "github.com/th2-net/th2-common-go/schema/modules"
	"github.com/th2-net/th2-common-go/schema/queue/message"
)

func main() {
	newFactory := factory.NewFactory(os.Args)
	if err := newFactory.Register(rabbitmq.NewRabbitMQModule); err != nil {
		panic(err)
	}

	boxConf := BoxConfiguration{messageType: "Batch"}
	messageType := boxConf.messageType

	module, err := rabbitmq.ModuleID.GetModule(newFactory)
	if err != nil {
		panic("No module found")
	}

	log.Printf("Start listening for %v messages\n", messageType)

	var TypeListener message.ConformationMessageListener = MessageTypeListener{messageType: messageType, function: func(args ...interface{}) { fmt.Println("Found Message") }}

	monitor, err := module.MqMessageRouter.SubscribeWithManualAck(&TypeListener, "group")
	if err != nil {
		log.Fatalln("Error occured when subscribing")
	}
	var wg sync.WaitGroup
	wg.Add(1)

	wg.Wait()
	// Listen for messages for 1000 seconds
	// time.Sleep(1000 * time.Second)

	module.Close()
	monitor.Unsubscribe()
}
