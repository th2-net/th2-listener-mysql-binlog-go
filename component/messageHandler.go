package component

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	p_buff "th2-grpc/th2_grpc_common"

	"github.com/rs/zerolog/log"
	rabbitmq "github.com/th2-net/th2-common-go/schema/modules/mqModule"
	"github.com/th2-net/th2-common-go/schema/queue/MQcommon"
)

type MessageTypeListener struct {
	MessageType    string
	Function       func(args ...interface{})
	RootEventID    *p_buff.EventID
	Module         *rabbitmq.RabbitMQModule
	AmountReceived *int
	NBatches       int
	Stats          map[string]int
}

func NewListener(RootEventID *p_buff.EventID, module *rabbitmq.RabbitMQModule, BoxConf *BoxConfiguration, Function func(args ...interface{})) *MessageTypeListener {
	amountReceived := 0
	return &MessageTypeListener{
		MessageType:    BoxConf.MessageType,
		Function:       Function,
		RootEventID:    RootEventID,
		Module:         module,
		AmountReceived: &amountReceived,
		NBatches:       4,
		Stats:          make(map[string]int),
	}
}

func (listener MessageTypeListener) Handle(delivery *MQcommon.Delivery, batch *p_buff.MessageGroupBatch,
	confirm *MQcommon.Confirmation) error {

	defer func() {
		if r := recover(); r != nil {
			log.Err(fmt.Errorf("%v", r)).Msg("Error occurred while processing the received message.")
		}
		*listener.AmountReceived += 1
		if *listener.AmountReceived%listener.NBatches == 0 {
			log.Info().Msg("Sending Statistic Event")
			var encoder bytes.Buffer
			enc := gob.NewEncoder(&encoder)
			table := GetNewTable("Message Type", "Amount")
			table.AddRow("Raw_Message", fmt.Sprint(listener.Stats["Raw"]))
			table.AddRow("Message", fmt.Sprint(listener.Stats["Messsage"]))
			enc.Encode(*table)
			listener.Module.MqEventRouter.SendAll(CreateEventBatch(
				listener.RootEventID, CreateEvent(
					CreateEventID(), listener.RootEventID, GetTimestamp(), GetTimestamp(), 0, "Statistic on Batches", "message", encoder.Bytes(), nil),
			), "publish")
		}
	}()

	if err := (*confirm).Confirm(); err != nil {
		log.Err(err).Msg("Error occurred while acknowledging the received message")
		return err
	}

	for _, group := range batch.Groups {
		for _, AnyMessage := range group.Messages {
			if AnyMessage.Kind != nil {
				if msg := AnyMessage.GetRawMessage(); msg != nil {
					log.Info().Msg("Received Raw Message")
					listener.Stats["Raw"] += 1
					return nil
				}
				msg := AnyMessage.GetMessage()
				listener.Stats["Message"] += 1
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
