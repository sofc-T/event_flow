package models

import (
	"github.com/google/uuid"
)

type Event struct {
	id          uuid.UUID
	description string
	kind        int
}

type EventConfig struct {
	Id          string
	Description string
	Type        int
}

func MapConfigToEvent(config EventConfig) (*Event, error) {
	id, err := uuid.Parse(config.Id)
	if err != nil {
		return nil, err
	}
	return &Event{
		id:          id,
		description: config.Description,
		kind:        config.Type,
	}, nil
}

func (e *Event) ID() uuid.UUID {
	return e.id
}

func (e *Event) Description() string {
	return e.description
}

func (e *Event) Kind() int {
	return e.kind
}

func SetEventID(e *Event, id uuid.UUID) {
	e.id = id
}

func SetEventDescription(e *Event, description string) {
	e.description = description
}

func SetEventKind(e *Event, kind int) {
	e.kind = kind
}
