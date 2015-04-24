package aero

import (
	"github.com/aerospike/aerospike-client-go"
	"github.com/plimble/utils/errors2"
)

var (
	ErrKeyExist    = errors2.NewBadReq("Key already exists")
	ErrIndexExist  = errors2.NewBadReq("Index already exists")
	ErrKeyNotExist = errors2.NewBadReq("Key is not exists")
	ErrNotFound    = errors2.NewNotFound("not found")
)

func errPut(err error) error {
	if err == nil {
		return nil
	}

	switch err.Error() {
	case "Key already exists":
		return ErrKeyExist
	default:
		return errors2.NewInternal(err.Error())
	}

	return nil
}

func errGet(record *aerospike.Record, err error) error {
	switch {
	case err == nil && record != nil:
		return nil
	case err != nil:
		return errors2.NewInternal(err.Error())
	case record == nil:
		return ErrNotFound
	}

	return nil
}

func errDel(exist bool, err error) error {
	if !exist {
		return ErrKeyNotExist
	}

	if err != nil {
		return errors2.NewInternal(err.Error())
	}

	return nil
}

func errIndex(err error) error {
	if err == nil {
		return nil
	}

	switch err.Error() {
	case "Index already exists":
		return ErrIndexExist
	}

	return errors2.NewInternal(err.Error())
}
