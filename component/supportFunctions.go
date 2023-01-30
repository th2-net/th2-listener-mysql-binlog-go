package component

import (
	p_buff "th2-grpc/th2_grpc_common"

	"github.com/google/uuid"
	timestamp "google.golang.org/protobuf/types/known/timestamppb"
)

type Table struct {
	Type    string        `json:"type"`
	Rows    []interface{} `json:"rows"`
	Headers []string      `json:"headers"`
}

func GetNewTable(headers ...string) *Table {
	return &Table{
		Type:    "table",
		Rows:    nil,
		Headers: headers,
	}
}
func (table *Table) AddRow(args ...string) {
	row := make(map[string]string)
	for i, arg := range args {
		row[table.Headers[i]] = arg
	}
	table.Rows = append(table.Rows, row)
}

func CreateEventID() *p_buff.EventID {
	return &p_buff.EventID{Id: uuid.New().String()}
}

func CreateEvent(Id *p_buff.EventID,
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
