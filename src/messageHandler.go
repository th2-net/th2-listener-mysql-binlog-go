package main

import (
	"log"

	"github.com/th2-net/th2-common-go/schema/queue/MQcommon"
	p_buff "github.com/th2-net/th2-common-go/th2_grpc/th2_grpc_common"
)

type MessageTypeListener struct {
	messageType string
	function    func(args ...interface{})
}

func (listener MessageTypeListener) Handle(delivery *MQcommon.Delivery, batch *p_buff.MessageGroupBatch,
	confirm *MQcommon.Confirmation) error {

	defer func() {
		if r := recover(); r != nil {
			log.Println("Error occurred while processing the received message.")
		}
	}()

	if err := (*confirm).Confirm(); err != nil {
		log.Println("Error in message confirmation")
		return err
	}

	for _, group := range batch.Groups {
		for _, AnyMessage := range group.Messages {
			if AnyMessage.Kind != nil {
				msg := AnyMessage.GetMessage()
				if msg.Metadata.MessageType == listener.messageType {
					log.Printf("Received message with %v message type\n", listener.messageType)
					listener.function()
					log.Printf("Triggered the function")
				}
			}
		}
	}

	return nil
}

func (listener MessageTypeListener) OnClose() error {
	log.Println("Listener OnClose")
	return nil
}
