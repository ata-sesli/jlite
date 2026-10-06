package jlite

import (
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func validateConnectionOptions(o PublisherOptions) error {
	u, e := url.Parse(o.URL)
	if e != nil || u.Hostname() == "" || u.User != nil || o.Username == "" || o.Password == "" || o.MaxStreamBytes <= 0 || o.Replicas < 1 || o.Replicas > 5 || o.DuplicateWindow < 100*time.Millisecond || o.RequestTimeout <= 0 {
		return ErrInvalidConfig
	}
	ip := net.ParseIP(u.Hostname())
	loopback := u.Hostname() == "localhost" || ip != nil && ip.IsLoopback()
	if u.Scheme != "tls" && (u.Scheme != "nats" || (!loopback && o.TLSConfig == nil)) {
		return fmt.Errorf("%w: TLS required outside loopback", ErrInvalidConfig)
	}
	return nil
}

func connectNamespace(c Config, namespace string, o PublisherOptions) (*nats.Conn, jetstream.JetStream, error) {
	if err := validateConnectionOptions(o); err != nil {
		return nil, nil, err
	}
	permissions, err := PermissionsFor(c, namespace)
	if err != nil {
		return nil, nil, err
	}
	options := []nats.Option{nats.UserInfo(o.Username, o.Password), nats.CustomInboxPrefix(permissions.InboxPrefix), nats.Timeout(o.RequestTimeout), nats.NoReconnect(), nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {})}
	if o.TLSConfig != nil {
		options = append(options, nats.Secure(o.TLSConfig.Clone()))
	}
	nc, err := nats.Connect(o.URL, options...)
	if err != nil {
		return nil, nil, err
	}
	if !nc.AuthRequired() {
		nc.Close()
		return nil, nil, ErrPublisherAuth
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}
