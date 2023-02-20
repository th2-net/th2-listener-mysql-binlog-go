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

package component

import (
	"encoding/json"
	"errors"
	"fmt"
	p_buff "th2-grpc/th2_grpc_common"

	"github.com/rs/zerolog/log"
	rabbitmq "github.com/th2-net/th2-common-go/schema/modules/mqModule"
	"github.com/th2-net/th2-common-go/schema/queue/MQcommon"
	utils "github.com/th2-net/th2-common-utils-go/th2_common_utils"
	timestamp "google.golang.org/protobuf/types/known/timestamppb"
)

type MessageTypeListener struct {
	MessageType    string
	Function       func(args ...interface{})
	RootEventID    *p_buff.EventID
	Module         *rabbitmq.RabbitMQModule
	AmountReceived int
	NBatches       int
	Stats          struct {
		MessageCount    int
		RawMessageCount int
	}
}

func NewListener(RootEventID *p_buff.EventID, module *rabbitmq.RabbitMQModule, BoxConf *BoxConfiguration, Function func(args ...interface{})) *MessageTypeListener {
	return &MessageTypeListener{
		MessageType:    BoxConf.MessageType,
		Function:       Function,
		RootEventID:    RootEventID,
		Module:         module,
		AmountReceived: 0,
		NBatches:       4,
		Stats: struct {
			MessageCount    int
			RawMessageCount int
		}{0, 0},
	}
}

func (listener *MessageTypeListener) Handle(delivery *MQcommon.Delivery, batch *p_buff.MessageGroupBatch) error {

	defer func() {
		listener.AmountReceived += 1
		if listener.AmountReceived%listener.NBatches == 0 {
			log.Debug().Msg("Sending Statistic Event")
			table := utils.GetNewTable("Message Type", "Amount")
			table.AddRow("Raw_Message", fmt.Sprint(listener.Stats.RawMessageCount))
			table.AddRow("Message", fmt.Sprint(listener.Stats.MessageCount))
			var payloads []utils.Table
			payloads = append(payloads, *table)
			encoded, _ := json.Marshal(&payloads)
			listener.Module.MqEventRouter.SendAll(utils.CreateEventBatch(listener.RootEventID,
				&p_buff.Event{
					Id:                 utils.CreateEventID(),
					ParentId:           listener.RootEventID,
					StartTimestamp:     timestamp.Now(),
					EndTimestamp:       nil,
					Status:             0,
					Name:               "Statistics Event",
					Type:               "Message",
					Body:               encoded,
					AttachedMessageIds: nil,
				},
			))
		}
	}()

	for _, group := range batch.Groups {
		for _, AnyMessage := range group.Messages {
			switch AnyMessage.GetKind().(type) {
			case *p_buff.AnyMessage_RawMessage:
				log.Debug().Msg("Received Raw Message")
				listener.Stats.RawMessageCount += 1
			case *p_buff.AnyMessage_Message:
				log.Debug().Msg("Received Message")
				listener.Stats.MessageCount += 1
				msg := AnyMessage.GetMessage()
				if msg.Metadata == nil {
					listener.Module.MqEventRouter.SendAll(utils.CreateEventBatch(listener.RootEventID,
						&p_buff.Event{
							Id:                 utils.CreateEventID(),
							ParentId:           listener.RootEventID,
							StartTimestamp:     timestamp.Now(),
							EndTimestamp:       nil,
							Status:             0,
							Name:               "Error: metadata not set for message",
							Type:               "Message",
							Body:               nil,
							AttachedMessageIds: nil,
						},
					))
					log.Err(errors.New("nil metadata")).Msg("Metadata not set for the message")
				} else if msg.Metadata.MessageType == listener.MessageType {
					log.Debug().Msgf("Received message with %v message type\n", listener.MessageType)
					listener.Function()
					log.Debug().Msg("Triggered the function")
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
