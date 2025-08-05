package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

type fieldConfig struct{}

func (fieldConfig) Validate() error {
	return nil
}

func (fieldConfig) CastToString() bool {
	return false
}

type property struct{}

func (m property) String() string {
	return "document"
}

func (m property) Compatible(existing boilerplate.ExistingField) bool {
	return true
}

func (m property) CanMigrate(existing boilerplate.ExistingField) bool {
	return false
}
