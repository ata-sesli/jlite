package jlite

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"
)

// ProtocolVersion is the only wire protocol supported by v0.
const ProtocolVersion = 1

// Sequences are positive and fit SQLite's signed INTEGER storage.
const maxSequence = math.MaxInt64

var (
	ErrInvalidChange      = errors.New("invalid change")
	ErrUnsupportedVersion = errors.New("unsupported protocol version")
)

// Change carries a single owner's ordered, immutable logical mutation.
// ID hashes the canonical envelope without ID itself. It detects changed
// content, but is not a signature or proof of publisher authorization.
type Change struct {
	Version  int          `json:"version"`
	ID       string       `json:"id,omitempty"`
	Owner    string       `json:"owner"`
	Sequence uint64       `json:"sequence"`
	Request  WriteRequest `json:"request"`
}

// NewChange constructs a validated change on the owner. It copies request bytes
// and checks the actual encoded size before the caller commits an outbox entry.
func (c Config) NewChange(r WriteRequest, sequence uint64) (Change, error) {
	if err := c.ValidateWrite(r); err != nil {
		return Change{}, err
	}
	change := Change{Version: ProtocolVersion, Owner: c.NodeID, Sequence: sequence, Request: r}
	change.Request.Value = normalizedValue(r.Value)
	id, err := change.identity()
	if err != nil {
		return Change{}, err
	}
	change.ID = id
	if _, err := change.Encode(c); err != nil {
		return Change{}, err
	}
	return change, nil
}

// Encode validates ownership, local assignment, identity, and size before
// encoding. Callers must not mutate Change while encoding or applying it.
func (change Change) Encode(c Config) ([]byte, error) {
	if err := change.validate(c); err != nil {
		return nil, err
	}
	change.Request.Value = normalizedValue(change.Request.Value)
	wire, err := json.Marshal(change)
	if err != nil {
		return nil, err
	}
	if len(wire) > c.Limits.MaxChangeBytes {
		return nil, fmt.Errorf("%w: encoded payload exceeds byte limit", ErrInvalidChange)
	}
	return wire, nil
}

// DecodeChange rejects oversized, ambiguous, malformed, and incompatible
// messages before returning a change for a locally assigned namespace.
// NATS credentials/subject permissions must establish publisher identity;
// the envelope's Owner field alone is not trusted authentication.
func DecodeChange(wire []byte, c Config) (Change, error) {
	if err := c.Validate(); err != nil {
		return Change{}, err
	}
	if len(wire) == 0 || len(wire) > c.Limits.MaxChangeBytes || !utf8.Valid(wire) {
		return Change{}, fmt.Errorf("%w: invalid wire size or UTF-8", ErrInvalidChange)
	}
	if err := checkJSONFields(wire); err != nil {
		return Change{}, fmt.Errorf("%w: %v", ErrInvalidChange, err)
	}
	d := json.NewDecoder(bytes.NewReader(wire))
	d.DisallowUnknownFields()
	var change Change
	if err := d.Decode(&change); err != nil {
		return Change{}, fmt.Errorf("%w: %v", ErrInvalidChange, err)
	}
	if err := change.validate(c); err != nil {
		return Change{}, err
	}
	change.Request.Value = normalizedValue(change.Request.Value)
	return change, nil
}

func (change Change) validate(c Config) error {
	if change.Version != ProtocolVersion {
		return ErrUnsupportedVersion
	}
	a, err := c.assignment(change.Request.Namespace)
	if err != nil {
		return err
	}
	if !a.hosts(c.NodeID) {
		return ErrNotAssigned
	}
	if change.Owner != a.Owner || change.Sequence == 0 || change.Sequence > maxSequence {
		return fmt.Errorf("%w: incorrect owner or sequence", ErrInvalidChange)
	}
	if err := change.Request.Validate(c.Limits); err != nil {
		return err
	}
	id, err := change.identity()
	if err != nil {
		return err
	}
	if change.ID != id {
		return fmt.Errorf("%w: identity does not match payload", ErrInvalidChange)
	}
	return nil
}

func (change Change) identity() (string, error) {
	change.ID = ""
	change.Request.Value = normalizedValue(change.Request.Value)
	wire, err := json.Marshal(change)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(wire)
	return hex.EncodeToString(digest[:]), nil
}

// checkJSONFields rejects duplicate keys, non-object roots, trailing values and
// excessive nesting. encoding/json alone accepts duplicate object keys.
func checkJSONFields(wire []byte) error {
	d := json.NewDecoder(bytes.NewReader(wire))
	first, err := d.Token()
	if err != nil {
		return err
	}
	if first != json.Delim('{') {
		return errors.New("expected an object")
	}
	if err := checkObject(d, 1); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON content")
	}
	return nil
}

func checkObject(d *json.Decoder, depth int) error {
	if depth > 4 {
		return errors.New("excessive JSON nesting")
	}
	seen := make(map[string]bool)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return err
		}
		name, ok := key.(string)
		if !ok || seen[name] || !knownField(depth, name) {
			return errors.New("duplicate, unknown, or invalid JSON field")
		}
		seen[name] = true
		value, err := d.Token()
		if err != nil {
			return err
		}
		if delim, ok := value.(json.Delim); ok {
			if delim != '{' {
				return errors.New("arrays are not supported by the change protocol")
			}
			if err := checkObject(d, depth+1); err != nil {
				return err
			}
		}
	}
	if _, err := d.Token(); err != nil {
		return err
	}
	if len(seen) != 5 {
		return errors.New("missing JSON field")
	}
	return nil
}

func knownField(depth int, name string) bool {
	if depth == 1 {
		switch name {
		case "version", "id", "owner", "sequence", "request":
			return true
		}
	}
	if depth == 2 {
		switch name {
		case "request_id", "namespace", "operation", "key", "value":
			return true
		}
	}
	return false
}
