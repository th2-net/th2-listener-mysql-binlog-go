package component

import (
	"fmt"
	p_buff "th2-grpc/th2_grpc_common"

	"github.com/rs/zerolog/log"
	"github.com/th2-net/th2-common-go/schema/queue/MQcommon"
)

type MessageTypeListener struct {
	MessageType string
	Function    func(args ...interface{})
}

func (listener MessageTypeListener) Handle(delivery *MQcommon.Delivery, batch *p_buff.MessageGroupBatch,
	confirm *MQcommon.Confirmation) error {

	defer func() {
		if r := recover(); r != nil {
			log.Err(fmt.Errorf("%v", r)).Msg("Error occurred while processing the received message.")
		}
	}()

	if err := (*confirm).Confirm(); err != nil {
		log.Err(err).Msg("Error occurred while processing the received message.")
		return err
	}

	for _, group := range batch.Groups {
		for _, AnyMessage := range group.Messages {
			if AnyMessage.Kind != nil {
				msg := AnyMessage.GetMessage()
				if msg.Metadata.MessageType == listener.MessageType {
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
