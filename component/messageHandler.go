/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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
	"fmt"
	"github.com/th2-net/th2-common-go/pkg/queue"
	"github.com/th2-net/th2-common-go/pkg/queue/event"
	"github.com/th2-net/th2-common-go/pkg/queue/message"
	utils "github.com/th2-net/th2-common-utils-go/pkg/event"
	p_buff "th2-grpc/th2_grpc_common"

	"github.com/rs/zerolog/log"
	"github.com/th2-net/th2-common-utils-go/pkg/event/report"
	timestamp "google.golang.org/protobuf/types/known/timestamppb"
)

type MessageTypeListener struct {
	MessageType    string
	Function       func(args ...interface{})
	RootEventID    *p_buff.EventID
	messageRouter  message.Router
	eventRouter    event.Router
	AmountReceived int
	NBatches       int
	Stats          struct {
		MessageCount    int
		RawMessageCount int
	}
}

func NewListener(
	rootEventID *p_buff.EventID,
	messageRouter message.Router,
	eventRouter event.Router,
	conf *Configuration,
	function func(args ...interface{}),
) *MessageTypeListener {
	return &MessageTypeListener{
		MessageType:    conf.MessageType,
		Function:       function,
		RootEventID:    rootEventID,
		eventRouter:    eventRouter,
		messageRouter:  messageRouter,
		AmountReceived: 0,
		NBatches:       4,
		Stats: struct {
			MessageCount    int
			RawMessageCount int
		}{0, 0},
	}
}

func (listener *MessageTypeListener) Handle(_ queue.Delivery, batch *p_buff.MessageGroupBatch) error {

	defer func() {
		listener.AmountReceived += 1
		if listener.AmountReceived%listener.NBatches == 0 {
			log.Debug().Str("Method", "Handle").Msg("Sending Statistic Event")
			table := report.GetNewTable("Message Type", "Amount")
			table.AddRow("Raw_Message", fmt.Sprint(listener.Stats.RawMessageCount))
			table.AddRow("Message", fmt.Sprint(listener.Stats.MessageCount))
			encoded, err := json.Marshal([]*report.Table{table})
			if err != nil {
				log.Error().Err(err).Msg("cannot marshall table data")
				return
			}
			err = listener.eventRouter.SendAll(utils.CreateEventBatch(listener.RootEventID,
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
			if err != nil {
				log.Error().Err(err).Msg("cannot send event")
			}
		}
	}()

	for _, group := range batch.Groups {
		for _, AnyMessage := range group.Messages {
			switch AnyMessage.GetKind().(type) {
			case *p_buff.AnyMessage_RawMessage:
				log.Debug().Str("Method", "Handle").Interface("MessageID", AnyMessage.GetRawMessage().Metadata.Id).Msg("Received Raw Message")
				listener.Stats.RawMessageCount += 1
			case *p_buff.AnyMessage_Message:
				log.Debug().Str("Method", "Handle").Interface("MessageID", AnyMessage.GetMessage().Metadata.Id).Msg("Received Message")
				listener.Stats.MessageCount += 1
				msg := AnyMessage.GetMessage()
				if msg.Metadata == nil {
					log.Error().Msg("Metadata not set for the message")
					err := listener.eventRouter.SendAll(utils.CreateEventBatch(listener.RootEventID,
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
					if err != nil {
						log.Error().Err(err).Msg("cannot send event about metadata error")
					}
				} else if msg.Metadata.MessageType == listener.MessageType {
					log.Debug().Str("Method", "Handle").
						Str("message_type", msg.Metadata.MessageType).
						Str("session_alias", msg.Metadata.Id.ConnectionId.SessionAlias).
						Interface("data", msg.Fields).
						Msg("received message")
					listener.Function()
					log.Debug().Str("Method", "Handle").Msg("Triggered the function")
				}
			}
		}
	}

	return listener.messageRouter.SendAll(batch, "group")
}

func (listener *MessageTypeListener) OnClose() error {
	log.Info().Msg("Listener OnClose")
	return nil
}
