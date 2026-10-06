package jlite

import (
	"errors"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		NodeID: "owner", Nodes: []string{"owner", "replica", "other"},
		Namespaces: []Assignment{{Namespace: "project:alpha", Owner: "owner", Replicas: []string{"replica"}}},
		Limits:     DefaultLimits(),
	}
}

func TestConfigRejectsConflictingAssignments(t *testing.T) {
	if err := testConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*Config)
	}{
		{"unknown local node", func(c *Config) { c.NodeID = "missing" }},
		{"duplicate node", func(c *Config) { c.Nodes = append(c.Nodes, "owner") }},
		{"unknown owner", func(c *Config) { c.Namespaces[0].Owner = "missing" }},
		{"unknown replica", func(c *Config) { c.Namespaces[0].Replicas = []string{"missing"} }},
		{"duplicate replica", func(c *Config) { c.Namespaces[0].Replicas = []string{"replica", "replica"} }},
		{"owner as replica", func(c *Config) { c.Namespaces[0].Replicas = []string{"owner"} }},
		{"duplicate namespace", func(c *Config) { c.Namespaces = append(c.Namespaces, c.Namespaces[0]) }},
		{"conflicting owner", func(c *Config) {
			c.Namespaces = append(c.Namespaces, Assignment{Namespace: "project:alpha", Owner: "other"})
		}},
		{"zero limits", func(c *Config) { c.Limits = Limits{} }},
		{"unbounded wait", func(c *Config) { c.Limits.BatchWait = 0 }},
		{"negative limit", func(c *Config) { c.Limits.MaxValueBytes = -1 }},
		{"batch too small", func(c *Config) { c.Limits.MaxBatchBytes = c.Limits.MaxChangeBytes - 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig()
			tc.edit(&c)
			if err := c.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate = %v, want invalid config", err)
			}
		})
	}
}

func TestNamespaceSubjectRejectsUnsafeNames(t *testing.T) {
	for _, name := range []string{"", "a.b", "*", "a>", "a b", "a\n", "a/b", "a\\b", "a\x00", "é", "-a"} {
		c := testConfig()
		c.Namespaces[0].Namespace = name
		if err := c.Validate(); err == nil {
			t.Errorf("accepted namespace %q", name)
		}
		if _, err := ChangeSubject(name); err == nil {
			t.Errorf("created subject for %q", name)
		}
	}
	got, err := ChangeSubject("project:alpha")
	if err != nil || got != "jlite.changes.project:alpha" {
		t.Fatalf("subject = %q, %v", got, err)
	}
}

func TestConfiguredOwnerWritesAndAssignedNodesRead(t *testing.T) {
	c := testConfig()
	r := WriteRequest{RequestID: "request-1", Namespace: "project:alpha", Operation: Put, Key: "document", Value: []byte("hello")}
	if err := c.ValidateWrite(r); err != nil {
		t.Fatal(err)
	}
	c.NodeID = "replica"
	if err := c.ValidateWrite(r); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("replica write = %v", err)
	}
	if err := c.ValidateRead(ReadRequest{Namespace: r.Namespace, Key: r.Key}); err != nil {
		t.Fatal(err)
	}
	c.NodeID = "other"
	if err := c.ValidateRead(ReadRequest{Namespace: r.Namespace, Key: r.Key}); !errors.Is(err, ErrNotAssigned) {
		t.Fatalf("unassigned read = %v", err)
	}
	c.NodeID = "owner"
	r.Namespace = "missing"
	if err := c.ValidateWrite(r); !errors.Is(err, ErrUnknownNamespace) {
		t.Fatalf("unknown namespace write = %v", err)
	}
}

func TestDefaultLimitsAreBounded(t *testing.T) {
	l := DefaultLimits()
	if l.MaxKeyBytes != 256 || l.MaxValueBytes != 64<<10 || l.MaxChangeBytes != 128<<10 ||
		l.MaxBatchOperations != 500 || l.MaxBatchBytes != 1<<20 || l.BatchWait != 10*time.Millisecond {
		t.Fatalf("unexpected defaults: %+v", l)
	}
}
