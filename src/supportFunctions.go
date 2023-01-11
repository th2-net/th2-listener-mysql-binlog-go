package main

import (
	"github.com/google/uuid"
	p_buff "github.com/th2-net/th2-common-go/th2_grpc/th2_grpc_common"
	timestamp "google.golang.org/protobuf/types/known/timestamppb"
)

func CreateEventID() *p_buff.EventID {
	return &p_buff.EventID{Id: uuid.New().String()}
}

func getTimestamp() *timestamp.Timestamp {
	return timestamp.Now()
}

func createEvent(Id *p_buff.EventID,
	ParentId *p_buff.EventID,
	StartTimestamp *timestamp.Timestamp,
	EndTimestamp *timestamp.Timestamp,
	Status p_buff.EventStatus,
	Name string,
	Type string,
	Body []byte,
	AttachedMessageIds []*p_buff.MessageID) *p_buff.Event {
	return &p_buff.Event{
		Id:                 Id,
		ParentId:           ParentId,
		StartTimestamp:     StartTimestamp,
		EndTimestamp:       EndTimestamp,
		Status:             Status,
		Name:               Name,
		Type:               Type,
		Body:               Body,
		AttachedMessageIds: AttachedMessageIds,
	}
}

func CreateEventBatch(ParentEventId *p_buff.EventID, Events ...*p_buff.Event) *p_buff.EventBatch {
	EventBatch := p_buff.EventBatch{
		ParentEventId: ParentEventId,
	}
	EventBatch.Events = append(EventBatch.Events, Events...)
	return &EventBatch
}
