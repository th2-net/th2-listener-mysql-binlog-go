package component

import (
	"errors"
	"fmt"
	p_buff "th2-grpc/th2_grpc_common"

	"github.com/rs/zerolog/log"
	rabbitmq "github.com/th2-net/th2-common-go/schema/modules/mqModule"
	"github.com/th2-net/th2-common-go/schema/queue/MQcommon"
)

type MessageTypeListener struct {
	MessageType string
	Function    func(args ...interface{})
	RootEventID *p_buff.EventID
	Module      *rabbitmq.RabbitMQModule
}

func (listener MessageTypeListener) Handle(delivery *MQcommon.Delivery, batch *p_buff.MessageGroupBatch,
	confirm *MQcommon.Confirmation) error {

	defer func() {
		if r := recover(); r != nil {
			log.Err(fmt.Errorf("%v", r)).Msg("Error occurred while processing the received message.")
		}
	}()

	if err := (*confirm).Confirm(); err != nil {
		log.Err(err).Msg("Error occurred while acknowledging the received message")
		return err
	}

	if batch.Groups == nil {
		listener.Module.MqEventRouter.SendAll(CreateEventBatch(
			listener.RootEventID, CreateEvent(
				CreateEventID(), listener.RootEventID, GetTimestamp(), GetTimestamp(), 0, "Error: metadata not set", "message", nil, nil),
		), "publish")
		log.Err(errors.New("nil Groups")).Msg("No Groups in MessageGroupBatch")
		return nil
	}

	log.Info().Msgf("%v\n", batch)

	for _, group := range batch.Groups {
		for _, AnyMessage := range group.Messages {
			if AnyMessage.Kind != nil {
				msg := AnyMessage.GetMessage()
				if msg.Metadata == nil {
					listener.Module.MqEventRouter.SendAll(CreateEventBatch(
						listener.RootEventID, CreateEvent(
							CreateEventID(), listener.RootEventID, GetTimestamp(), GetTimestamp(), 0, "Error: metadata not set", "message", nil, nil),
					), "publish")
					log.Err(errors.New("nil metadata")).Msg("Metadata not set for the message")
				} else if msg.Metadata.MessageType == listener.MessageType {
					log.Info().Msgf("Received message with %v message type\n", listener.MessageType)
					listener.Function()
					log.Info().Msg("Triggered the function")

					listener.Module.MqEventRouter.SendAll(CreateEventBatch(
						listener.RootEventID, CreateEvent(
							CreateEventID(), listener.RootEventID, GetTimestamp(), GetTimestamp(), 0, "Message Received", "message", nil, nil),
					), "publish")
				}
			}
		}
	}

	return nil
}

func (listener MessageTypeListener) OnClose() error {
	log.Info().Msg("Listener OnClose")
	return nil
}
