package jlite

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ErrInvalidRequest indicates an invalid operation, identity, key, or value.
var ErrInvalidRequest = errors.New("invalid request")

// ErrRequestConflict means a request identity was reused for different content.
var ErrRequestConflict = errors.New("request identity conflicts with stored write")

// Operation identifies a mutation. Reads are not replicated mutations.
type Operation string

const (
	Put    Operation = "put"
	Delete Operation = "delete"
)

// WriteRequest is one atomic keyed-record mutation. RequestID is scoped to
// Namespace and must be reused unchanged when retrying the same operation.
type WriteRequest struct {
	RequestID string    `json:"request_id"`
	Namespace string    `json:"namespace"`
	Operation Operation `json:"operation"`
	Key       string    `json:"key"`
	Value     []byte    `json:"value"`
}

// Validate checks mutation shape and configured record limits.
func (r WriteRequest) Validate(l Limits) error {
	if err := l.validate(); err != nil {
		return err
	}
	if !validID(r.RequestID) || !validID(r.Namespace) {
		return fmt.Errorf("%w: invalid request or namespace identity", ErrInvalidRequest)
	}
	if err := validateKey(r.Key, l); err != nil {
		return err
	}
	switch r.Operation {
	case Put:
		if len(r.Value) > l.MaxValueBytes {
			return fmt.Errorf("%w: value exceeds byte limit", ErrInvalidRequest)
		}
	case Delete:
		if len(r.Value) != 0 {
			return fmt.Errorf("%w: delete cannot carry a value", ErrInvalidRequest)
		}
	default:
		return fmt.Errorf("%w: unsupported operation", ErrInvalidRequest)
	}
	return nil
}

// WriteResult describes a locally committed write, not publication or replica
// application. Return it only after the owner commits data and request metadata.
type WriteResult struct {
	Namespace string
	Sequence  uint64
}

// StoredWrite is the durable retry metadata committed with a write's data.
// Writer persists this metadata atomically with the record and outbox entry.
type StoredWrite struct {
	RequestID   string
	Fingerprint string
	Result      WriteResult
}

// Fingerprint hashes a canonical structured payload, excluding RequestID.
// Nil and empty put values are the same record; neither is a deletion.
func (r WriteRequest) Fingerprint(l Limits) (string, error) {
	if err := r.Validate(l); err != nil {
		return "", err
	}
	payload := struct {
		Namespace string    `json:"namespace"`
		Operation Operation `json:"operation"`
		Key       string    `json:"key"`
		Value     []byte    `json:"value"`
	}{r.Namespace, r.Operation, r.Key, normalizedValue(r.Value)}
	wire, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(wire)
	return hex.EncodeToString(digest[:]), nil
}

// RetryResult returns the prior locally applied result for an identical retry.
// Callers must load StoredWrite by namespace and request ID from durable storage.
func RetryResult(r WriteRequest, stored StoredWrite, l Limits) (WriteResult, error) {
	fingerprint, err := r.Fingerprint(l)
	if err != nil {
		return WriteResult{}, err
	}
	if stored.Result.Sequence == 0 || stored.Result.Sequence > maxSequence {
		return WriteResult{}, fmt.Errorf("%w: invalid stored result position", ErrInvalidRequest)
	}
	if r.RequestID != stored.RequestID || r.Namespace != stored.Result.Namespace || fingerprint != stored.Fingerprint {
		return WriteResult{}, ErrRequestConflict
	}
	return stored.Result, nil
}

func normalizedValue(value []byte) []byte {
	if len(value) == 0 {
		return []byte{}
	}
	return bytes.Clone(value)
}

// ReadRequest addresses a local keyed record.
type ReadRequest struct {
	Namespace string
	Key       string
}

// ReadResult is the record and contiguous applied position from one local read
// snapshot. Found distinguishes a missing record from an empty value.
type ReadResult struct {
	Value           []byte
	Found           bool
	AppliedSequence uint64
}

func validateKey(key string, l Limits) error {
	if len(key) == 0 || len(key) > l.MaxKeyBytes || !utf8.ValidString(key) || strings.ContainsRune(key, 0) {
		return fmt.Errorf("%w: key must be nonempty UTF-8 without NUL within the byte limit", ErrInvalidRequest)
	}
	return nil
}
