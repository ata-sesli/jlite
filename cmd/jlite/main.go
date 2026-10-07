// jlite is the v0 three-node demo process and its local control client.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"jlite"
)

const demoNamespace = "project:alpha"
const defaultSocket = "/run/jlite/control.sock"

func demoConfig(node string) jlite.Config {
	return jlite.Config{NodeID: node, Nodes: []string{"owner", "replica", "other"}, Namespaces: []jlite.Assignment{{Namespace: demoNamespace, Owner: "owner", Replicas: []string{"replica", "other"}}}, Limits: jlite.DefaultLimits()}
}

func brokerOptions(data string, passwords map[string]string) (*server.Options, error) {
	o := &server.Options{}
	if err := o.ProcessConfigString("jetstream { sync_interval: always }"); err != nil {
		return nil, err
	}
	o.Host, o.Port = "127.0.0.1", 4222
	o.JetStream, o.StoreDir, o.NoSigs = true, data, true
	for _, node := range []string{"owner", "replica", "other"} {
		if passwords[node] == "" {
			return nil, fmt.Errorf("missing %s broker password", node)
		}
		p, err := jlite.PermissionsFor(demoConfig(node), demoNamespace)
		if err != nil {
			return nil, err
		}
		o.Users = append(o.Users, &server.User{Username: node, Password: passwords[node], Permissions: &server.Permissions{Publish: &server.SubjectPermission{Allow: p.Publish}, Subscribe: &server.SubjectPermission{Allow: p.Subscribe}}})
	}
	return o, nil
}

type command struct {
	Operation string `json:"operation"`
	RequestID string `json:"request_id,omitempty"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value,omitempty"`
}
type response struct {
	Error  string             `json:"error,omitempty"`
	Write  *jlite.WriteResult `json:"write,omitempty"`
	Read   *jlite.ReadResult  `json:"read,omitempty"`
	Status *jlite.NodeStatus  `json:"status,omitempty"`
}

func applyCommand(ctx context.Context, n *jlite.Node, c command) response {
	r := response{}
	var err error
	switch c.Operation {
	case "status":
		var s jlite.NodeStatus
		s, err = n.Status()
		r.Status = &s
	case "get":
		var s jlite.ReadResult
		s, err = n.Get(jlite.ReadRequest{Namespace: demoNamespace, Key: c.Key})
		r.Read = &s
	case "put", "delete":
		var s jlite.WriteResult
		value := []byte(c.Value)
		s, err = n.Write(ctx, jlite.WriteRequest{Namespace: demoNamespace, RequestID: c.RequestID, Operation: jlite.Operation(c.Operation), Key: c.Key, Value: value})
		r.Write = &s
	default:
		err = errors.New("unsupported command")
	}
	if err != nil {
		return response{Error: err.Error()}
	}
	return r
}

func serveSocket(ctx context.Context, path string, n *jlite.Node) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := os.Chmod(path, 0600); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	var workers sync.WaitGroup
	defer workers.Wait()
	slots := make(chan struct{}, 16)
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case slots <- struct{}{}:
		default:
			conn.Close()
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-slots }()
			defer conn.Close()
			cancel := context.AfterFunc(ctx, func() { conn.Close() })
			defer cancel()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			decoder := json.NewDecoder(io.LimitReader(conn, int64(2*jlite.DefaultLimits().MaxChangeBytes+1)))
			decoder.DisallowUnknownFields()
			var c command
			err := decoder.Decode(&c)
			if err == nil {
				var extra any
				if e := decoder.Decode(&extra); e != io.EOF {
					err = errors.New("expected one command")
				}
			}
			reply := response{}
			if err != nil {
				reply.Error = "invalid command JSON"
			} else {
				reply = applyCommand(ctx, n, c)
			}
			_ = json.NewEncoder(conn).Encode(reply)
		}()
	}
}

func requestSocket(path string, c command) (response, error) {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return response{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(6 * time.Second))
	if err := json.NewEncoder(conn).Encode(c); err != nil {
		return response{}, err
	}
	if err := conn.CloseWrite(); err != nil {
		return response{}, err
	}
	var r response
	err = json.NewDecoder(io.LimitReader(conn, 2<<20)).Decode(&r)
	return r, err
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: jlite broker|serve|ctl")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	data := f.String("data", "/data", "persistent database/broker directory")
	socket := f.String("socket", defaultSocket, "local Unix control socket")
	node := f.String("node", "", "static demo node: owner, replica, other")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	switch args[0] {
	case "broker":
		o, err := brokerOptions(*data, map[string]string{"owner": os.Getenv("OWNER_PASSWORD"), "replica": os.Getenv("REPLICA_PASSWORD"), "other": os.Getenv("OTHER_PASSWORD")})
		if err != nil {
			return err
		}
		s, err := server.NewServer(o)
		if err != nil {
			return err
		}
		s.Start()
		defer func() { s.Shutdown(); s.WaitForShutdown() }()
		if !s.ReadyForConnections(5 * time.Second) {
			return errors.New("broker not ready")
		}
		fmt.Fprintln(os.Stderr, "demo broker ready: authenticated loopback, file storage, sync_interval=always, one server")
		<-ctx.Done()
		return nil
	case "serve":
		if err := os.MkdirAll(*data, 0700); err != nil {
			return err
		}
		o := jlite.DefaultNodeOptions()
		o.Replica.Connection.URL = os.Getenv("NATS_URL")
		if o.Replica.Connection.URL == "" {
			o.Replica.Connection.URL = "nats://127.0.0.1:4222"
		}
		o.Replica.Connection.Username, o.Replica.Connection.Password = *node, os.Getenv("NATS_PASSWORD")
		n, err := jlite.OpenNode(filepath.Join(*data, "namespace.zova"), demoNamespace, demoConfig(*node), o)
		if err != nil {
			return err
		}
		err = serveSocket(ctx, *socket, n)
		return errors.Join(err, n.Close())
	case "ctl":
		a := f.Args()
		if len(a) == 0 {
			return errors.New("usage: jlite ctl [--socket path] status|get key|put request-id key value|delete request-id key")
		}
		c := command{Operation: a[0]}
		switch {
		case a[0] == "status" && len(a) == 1:
		case a[0] == "get" && len(a) == 2:
			c.Key = a[1]
		case a[0] == "put" && len(a) == 4:
			c.RequestID, c.Key, c.Value = a[1], a[2], a[3]
		case a[0] == "delete" && len(a) == 3:
			c.RequestID, c.Key = a[1], a[2]
		default:
			return errors.New("invalid control arguments")
		}
		r, err := requestSocket(*socket, c)
		if err != nil {
			return err
		}
		if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
			return err
		}
		if r.Error != "" {
			return errors.New(r.Error)
		}
		return nil
	default:
		return errors.New("usage: jlite broker|serve|ctl")
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
