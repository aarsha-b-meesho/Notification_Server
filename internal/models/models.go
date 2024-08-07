package models

import (
	"time"
)

type SMS struct {
	ID              string `gorm:"primaryKey"`
	PhoneNumber     string
	Message         string
	Status          string
	FailureCode     string
	FailureComments string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type Number struct {
	PhoneNumber string `json:"PhoneNumber"`
}
