package encodingutils

import (
	"bytes"
	"encoding/gob"
)

func Encode[T any](data T) ([]byte, error) {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(data); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func Decode[T any](data []byte) (value T, err error) {
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&value); err != nil {
		return value, err
	}

	return value, nil
}
