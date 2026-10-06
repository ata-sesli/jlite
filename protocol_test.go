package jlite

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestChangeRoundTripAndImmutableIdentity(t *testing.T) {
	c := testConfig()
	r := testWrite()
	change, err := c.NewChange(r, 1)
	if err != nil {
		t.Fatal(err)
	}
	r.Value[0] = 42
	if change.Request.Value[0] != 0 {
		t.Fatal("change aliases caller's mutable bytes")
	}
	wire, err := change.Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	c.NodeID = "replica"
	decoded, err := DecodeChange(wire, c)
	if err != nil || decoded.ID != change.ID || !bytes.Equal(decoded.Request.Value, change.Request.Value) {
		t.Fatalf("roundtrip = %+v, %v", decoded, err)
	}
	second, err := testConfig().NewChange(testWrite(), 1)
	if err != nil || second.ID != change.ID {
		t.Fatalf("identity is not deterministic: %v", err)
	}
	decoded.Request.Value[0] = 10
	if _, err := decoded.Encode(c); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("mutated payload accepted: %v", err)
	}
}

func TestChangeRejectsInvalidEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Change)
		want error
	}{
		{"future version", func(c *Change) { c.Version++ }, ErrUnsupportedVersion},
		{"zero sequence", func(c *Change) { c.Sequence = 0 }, ErrInvalidChange},
		{"sequence overflow", func(c *Change) { c.Sequence = math.MaxUint64 }, ErrInvalidChange},
		{"wrong origin", func(c *Change) { c.Owner = "other" }, ErrInvalidChange},
		{"different identity", func(c *Change) { c.ID = strings.Repeat("0", 64) }, ErrInvalidChange},
		{"invalid operation", func(c *Change) { c.Request.Operation = "get" }, ErrInvalidRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig()
			change, err := c.NewChange(testWrite(), 1)
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(&change)
			wire, err := json.Marshal(change)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeChange(wire, c); !errors.Is(err, tc.want) {
				t.Fatalf("decode = %v, want %v", err, tc.want)
			}
		})
	}
	c := testConfig()
	change, _ := c.NewChange(testWrite(), 1)
	wire, _ := change.Encode(c)
	c.NodeID = "other"
	if _, err := DecodeChange(wire, c); !errors.Is(err, ErrNotAssigned) {
		t.Fatalf("unassigned receiver accepted change: %v", err)
	}
}

func TestDecodeRejectsMalformedAndOversizedMessages(t *testing.T) {
	c := testConfig()
	change, _ := c.NewChange(testWrite(), 1)
	wire, _ := change.Encode(c)
	for _, input := range [][]byte{
		nil, []byte("null"), []byte("[]"), []byte("{}"), []byte("{"),
		append(bytes.Clone(wire), []byte(" {}")...),
		bytes.Replace(wire, []byte(`"version":1`), []byte(`"version":1,"unknown":true`), 1),
		bytes.Replace(wire, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		bytes.Replace(wire, []byte(`"version":1`), []byte(`"version":1,"Version":1`), 1),
		bytes.Replace(wire, []byte(`"version":1`), []byte(`"Version":1`), 1),
		bytes.Replace(wire, []byte(`"key":"document"`), []byte(`"key":"document","key":"document"`), 1),
		bytes.Replace(wire, []byte(`"key":"document"`), []byte(`"key":"document","extra":1`), 1),
		bytes.Replace(wire, []byte(`"value":"AP8B"`), []byte(`"value":"not base64!"`), 1),
		bytes.Replace(wire, []byte(`"key":"document"`), []byte{'"', 'k', 'e', 'y', '"', ':', '"', 255, '"'}, 1),
		bytes.Repeat([]byte(" "), c.Limits.MaxChangeBytes+1),
	} {
		if _, err := DecodeChange(input, c); err == nil {
			t.Errorf("accepted malformed message (length %d)", len(input))
		}
	}
}

func TestDecodeRequiresExplicitValueField(t *testing.T) {
	c := testConfig()
	r := testWrite()
	r.Value = nil
	change, err := c.NewChange(r, 1)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := change.Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	wire = bytes.Replace(wire, []byte(`,"value":""`), nil, 1)
	if _, err := DecodeChange(wire, c); err == nil {
		t.Fatal("missing value field was accepted as an empty put")
	}
}

func TestNewChangeChecksOwnershipSequenceAndWireBudget(t *testing.T) {
	c := testConfig()
	for _, seq := range []uint64{0, math.MaxUint64} {
		if _, err := c.NewChange(testWrite(), seq); !errors.Is(err, ErrInvalidChange) {
			t.Fatalf("sequence %d accepted: %v", seq, err)
		}
	}
	c.NodeID = "replica"
	if _, err := c.NewChange(testWrite(), 1); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("replica created change: %v", err)
	}
	c = testConfig()
	c.Limits.MaxChangeBytes = 256
	r := testWrite()
	r.Value = make([]byte, 256)
	if _, err := c.NewChange(r, 1); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("wire budget ignored: %v", err)
	}
}

func TestV1WireFixture(t *testing.T) {
	c := testConfig()
	change, err := c.NewChange(testWrite(), 1)
	if err != nil {
		t.Fatal(err)
	}
	const id = "0ec40d9ab203881c11ad29ad36f6c07c6bee05a2c7b787e4492baf36c7da4b4c"
	const fixture = `{"version":1,"id":"0ec40d9ab203881c11ad29ad36f6c07c6bee05a2c7b787e4492baf36c7da4b4c","owner":"owner","sequence":1,"request":{"request_id":"req-1","namespace":"project:alpha","operation":"put","key":"document","value":"AP8B"}}`
	wire, err := change.Encode(c)
	if err != nil || change.ID != id || string(wire) != fixture {
		t.Fatalf("v1 wire changed: %s, %v", wire, err)
	}
	decoded, err := DecodeChange([]byte(fixture), c)
	if err != nil || decoded.ID != id {
		t.Fatalf("v1 fixture rejected: %+v, %v", decoded, err)
	}
	// Upper bound must round-trip without float conversion of the sequence.
	change, err = c.NewChange(testWrite(), math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	wire, err = change.Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = DecodeChange(wire, c)
	if err != nil || decoded.Sequence != math.MaxInt64 {
		t.Fatalf("sequence precision lost: %+v, %v", decoded, err)
	}
}
