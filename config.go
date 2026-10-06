package jlite

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidConfig    = errors.New("invalid configuration")
	ErrUnknownNamespace = errors.New("unknown namespace")
	ErrNotOwner         = errors.New("local node is not the namespace owner")
	ErrNotAssigned      = errors.New("local node is not assigned to the namespace")
)

// Limits bounds record sizes, wire messages, and future writer batches.
// BatchWait bounds collection time, not end-to-end latency.
type Limits struct {
	MaxKeyBytes        int
	MaxValueBytes      int
	MaxChangeBytes     int
	MaxBatchOperations int
	MaxBatchBytes      int
	BatchWait          time.Duration
}

// DefaultLimits returns explicit v0 limits. Zero values are not unlimited.
func DefaultLimits() Limits {
	return Limits{
		MaxKeyBytes: 256, MaxValueBytes: 64 << 10, MaxChangeBytes: 128 << 10,
		MaxBatchOperations: 500, MaxBatchBytes: 1 << 20, BatchWait: 10 * time.Millisecond,
	}
}

func (l Limits) validate() error {
	// Leave room for NATS headers under the default 1 MiB payload limit.
	if l.MaxKeyBytes <= 0 || l.MaxValueBytes <= 0 || l.MaxChangeBytes <= 0 ||
		l.MaxChangeBytes > (1<<20)-4096 || l.MaxBatchOperations <= 0 ||
		l.MaxBatchBytes < l.MaxChangeBytes || l.BatchWait <= 0 {
		return fmt.Errorf("%w: limits must be positive, wire size below 1 MiB, and batch bytes at least wire size", ErrInvalidConfig)
	}
	return nil
}

// Assignment fixes one namespace's owner and its additional read replicas.
// The owner is already a host and must not also appear in Replicas.
type Assignment struct {
	Namespace string
	Owner     string
	Replicas  []string
}

// Config describes static cluster assignments from the local node's viewpoint.
// Treat a validated Config as immutable while it is in use. v0 has no ownership
// transfer; deployments must prevent simultaneous instances of one owner.
type Config struct {
	NodeID     string
	Nodes      []string
	Namespaces []Assignment
	Limits     Limits
}

// Validate rejects unknown identities, unsafe subject tokens, duplicate
// assignments, and invalid limits. It does not authenticate NATS publishers.
func (c Config) Validate() error {
	if err := c.Limits.validate(); err != nil {
		return err
	}
	nodes := make(map[string]bool, len(c.Nodes))
	for _, node := range c.Nodes {
		if !validID(node) || nodes[node] {
			return fmt.Errorf("%w: invalid or duplicate node %q", ErrInvalidConfig, node)
		}
		nodes[node] = true
	}
	if !nodes[c.NodeID] {
		return fmt.Errorf("%w: local node is unknown", ErrInvalidConfig)
	}
	namespaces := make(map[string]bool, len(c.Namespaces))
	for _, a := range c.Namespaces {
		if !validID(a.Namespace) || namespaces[a.Namespace] || !nodes[a.Owner] {
			return fmt.Errorf("%w: invalid namespace, duplicate assignment, or unknown owner for %q", ErrInvalidConfig, a.Namespace)
		}
		namespaces[a.Namespace] = true
		replicas := map[string]bool{a.Owner: true}
		for _, node := range a.Replicas {
			if !nodes[node] || replicas[node] {
				return fmt.Errorf("%w: unknown or duplicate host for %q", ErrInvalidConfig, a.Namespace)
			}
			replicas[node] = true
		}
	}
	return nil
}

func (c Config) assignment(namespace string) (Assignment, error) {
	if err := c.Validate(); err != nil {
		return Assignment{}, err
	}
	for _, a := range c.Namespaces {
		if a.Namespace == namespace {
			return a, nil
		}
	}
	return Assignment{}, ErrUnknownNamespace
}

// ValidateWrite admits only valid requests addressed to the configured owner.
// Admission does not apply or persist the request.
func (c Config) ValidateWrite(r WriteRequest) error {
	a, err := c.assignment(r.Namespace)
	if err != nil {
		return err
	}
	if a.Owner != c.NodeID {
		return ErrNotOwner
	}
	return r.Validate(c.Limits)
}

// ValidateRead admits local reads only on hosts assigned to the namespace.
// Replica reads may lag behind the owner.
func (c Config) ValidateRead(r ReadRequest) error {
	a, err := c.assignment(r.Namespace)
	if err != nil {
		return err
	}
	if !a.hosts(c.NodeID) {
		return ErrNotAssigned
	}
	return validateKey(r.Key, c.Limits)
}

func (a Assignment) hosts(node string) bool {
	if node == a.Owner {
		return true
	}
	for _, replica := range a.Replicas {
		if node == replica {
			return true
		}
	}
	return false
}

// ChangeSubject returns the sole mutation subject for a namespace.
func ChangeSubject(namespace string) (string, error) {
	if !validID(namespace) {
		return "", fmt.Errorf("%w: unsafe namespace", ErrInvalidConfig)
	}
	return "jlite.changes." + namespace, nil
}

func validID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for i := range len(id) {
		b := id[i]
		alnum := b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
		if !alnum && (i == 0 || b != '-' && b != '_' && b != ':') {
			return false
		}
	}
	return true
}
