package model

import (
	"database/sql"
	"time"
)

type EventDAO struct {
	ID        int
	Name      string
	Status    string
	StartAt   time.Time
	EndAt     time.Time
	CreatedAt time.Time
	UpdatedAt sql.NullTime
}
