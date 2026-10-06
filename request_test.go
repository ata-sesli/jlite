package jlite

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func testWrite() WriteRequest {
	return WriteRequest{RequestID: "req-1", Namespace: "project:alpha", Operation: Put, Key: "document", Value: []byte{0, 255, 1}}
}

func TestWriteRequestRejectsInvalidMutations(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*WriteRequest)
	}{
		{"empty request id", func(r *WriteRequest) { r.RequestID = "" }},
		{"unsafe request id", func(r *WriteRequest) { r.RequestID = "a.*" }},
		{"unknown operation", func(r *WriteRequest) { r.Operation = "get" }},
		{"empty key", func(r *WriteRequest) { r.Key = "" }},
		{"oversized key", func(r *WriteRequest) { r.Key = strings.Repeat("k", 257) }},
		{"invalid utf8", func(r *WriteRequest) { r.Key = string([]byte{255}) }},
		{"nul key", func(r *WriteRequest) { r.Key = "a\x00b" }},
		{"oversized value", func(r *WriteRequest) { r.Value = make([]byte, (64<<10)+1) }},
		{"delete with value", func(r *WriteRequest) { r.Operation = Delete }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := testWrite()
			tc.edit(&r)
			if err := r.Validate(DefaultLimits()); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Validate = %v", err)
			}
		})
	}
	r := testWrite()
	r.Key = strings.Repeat("k", 256)
	r.Value = make([]byte, 64<<10)
	if err := r.Validate(DefaultLimits()); err != nil {
		t.Fatalf("boundary request: %v", err)
	}
}

func TestRetryReturnsStoredResultOrRejectsConflictingPayload(t *testing.T) {
	r := testWrite()
	fingerprint, err := r.Fingerprint(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stored := StoredWrite{RequestID: r.RequestID, Fingerprint: fingerprint, Result: WriteResult{Namespace: r.Namespace, Sequence: 7}}
	got, err := RetryResult(r, stored, DefaultLimits())
	if err != nil || got != stored.Result {
		t.Fatalf("retry = %+v, %v", got, err)
	}
	for _, edit := range []func(*WriteRequest){
		func(r *WriteRequest) { r.Key = "other" },
		func(r *WriteRequest) { r.Value = []byte("different") },
		func(r *WriteRequest) { r.Operation = Delete; r.Value = nil },
		func(r *WriteRequest) { r.Namespace = "other" },
		func(r *WriteRequest) { r.RequestID = "req-2" },
	} {
		r := testWrite()
		edit(&r)
		if _, err := RetryResult(r, stored, DefaultLimits()); !errors.Is(err, ErrRequestConflict) {
			t.Fatalf("conflicting retry returned %v", err)
		}
	}
}

func TestFingerprintCanonicalizesEmptyValues(t *testing.T) {
	r := testWrite()
	r.Value = nil
	a, err := r.Fingerprint(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	r.Value = []byte{}
	r.RequestID = "another-request"
	b, err := r.Fingerprint(DefaultLimits())
	if err != nil || a != b {
		t.Fatalf("same logical payload fingerprint changed: %q, %q, %v", a, b, err)
	}
	// A digest must distinguish ambiguous concatenations of key/value fields.
	r.Key, r.Value = "ab", []byte("c")
	a, _ = r.Fingerprint(DefaultLimits())
	r.Key, r.Value = "a", []byte("bc")
	b, _ = r.Fingerprint(DefaultLimits())
	if bytes.Equal([]byte(a), []byte(b)) {
		t.Fatal("different structured payloads have the same fingerprint")
	}
}

func TestRetryRejectsUncommittedStoredPosition(t *testing.T) {
	r := testWrite()
	fingerprint, err := r.Fingerprint(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stored := StoredWrite{RequestID: r.RequestID, Fingerprint: fingerprint, Result: WriteResult{Namespace: r.Namespace}}
	if _, err := RetryResult(r, stored, DefaultLimits()); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("zero stored position accepted: %v", err)
	}
}
