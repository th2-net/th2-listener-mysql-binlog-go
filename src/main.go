package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	box "github.com/th2-net/th2-box-template-go/src/boxConfiguration"
	"github.com/th2-net/th2-common-go/schema/factory"
	rabbitmq "github.com/th2-net/th2-common-go/schema/modules"
	"github.com/th2-net/th2-common-go/schema/queue/MQcommon"
	"github.com/th2-net/th2-common-go/schema/queue/message"
)

func main() {
	newFactory := factory.NewFactory(os.Args)
	if err := newFactory.Register(rabbitmq.NewRabbitMQModule); err != nil {
		panic(err)
	}

	boxConf := box.BoxConfiguration{MessageType: "Batch"}
	messageType := boxConf.MessageType

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

	// Start listening for shutdown signal
	shutdown(&monitor, module)
}

func shutdown(monitor *MQcommon.Monitor, module *rabbitmq.RabbitMQModule) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	log.Println("Shutting Down")
	(*monitor).Unsubscribe()
	module.Close()
}
