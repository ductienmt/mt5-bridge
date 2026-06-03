package repository

import "errors"

// Repository errors
var (
	ErrNotFound           = errors.New("resource not found")
	ErrDuplicateAccountID = errors.New("account ID already registered as master")
	ErrDuplicateFollower  = errors.New("account ID already registered as follower for this master")
	ErrInvalidInput      = errors.New("invalid input")
	ErrUnauthorized      = errors.New("unauthorized")
)
