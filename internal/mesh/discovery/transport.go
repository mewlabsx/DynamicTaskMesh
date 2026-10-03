package discovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
)

var ErrTransportClosed = errors.New("presence transport is closed")

type Transport interface {
	Announcements() <-chan []byte
	Publish(context.Context, []byte) error
	Close() error
}

type MulticastConfig struct {
	Group     string
	Port      int
	Interface string
}

type UDPTransport struct {
	receive *net.UDPConn
	send    *net.UDPConn
	target  *net.UDPAddr
	values  chan []byte
	done    chan struct{}
	close   sync.Once
}

func NewUDPTransport(config MulticastConfig) (*UDPTransport, error) {
	ip := net.ParseIP(config.Group)
	if ip == nil || ip.To4() == nil || !ip.IsMulticast() || config.Port < 1024 || config.Port > 65535 {
		return nil, errors.New("invalid IPv4 multicast configuration")
	}
	var networkInterface *net.Interface
	var localIP net.IP
	if config.Interface != "" {
		localIP = net.ParseIP(config.Interface).To4()
		if localIP == nil {
			return nil, errors.New("invalid multicast interface IPv4 address")
		}
		interfaces, err := net.Interfaces()
		if err != nil {
			return nil, err
		}
		for i := range interfaces {
			addresses, addressErr := interfaces[i].Addrs()
			if addressErr != nil {
				continue
			}
			for _, address := range addresses {
				network, _, _ := net.ParseCIDR(address.String())
				if network != nil && network.Equal(localIP) {
					networkInterface = &interfaces[i]
					break
				}
			}
		}
		if networkInterface == nil {
			return nil, fmt.Errorf("multicast interface address %s is not local", config.Interface)
		}
	}
	target := &net.UDPAddr{IP: ip, Port: config.Port}
	listenConfig := net.ListenConfig{Control: reuseAddress}
	packetConnection, err := listenConfig.ListenPacket(context.Background(), "udp4", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		return nil, fmt.Errorf("listen multicast presence: %w", err)
	}
	receive, ok := packetConnection.(*net.UDPConn)
	if !ok {
		_ = packetConnection.Close()
		return nil, errors.New("multicast listener is not UDP")
	}
	group := ipv4.NewPacketConn(receive)
	if err := group.JoinGroup(networkInterface, target); err != nil {
		_ = receive.Close()
		return nil, fmt.Errorf("join multicast group: %w", err)
	}
	send, err := net.ListenUDP("udp4", &net.UDPAddr{IP: localIP})
	if err != nil {
		_ = receive.Close()
		return nil, fmt.Errorf("open multicast publisher: %w", err)
	}
	transport := &UDPTransport{receive: receive, send: send, target: target, values: make(chan []byte, 32), done: make(chan struct{})}
	go transport.readLoop()
	return transport, nil
}

func (transport *UDPTransport) readLoop() {
	defer close(transport.values)
	buffer := make([]byte, 4096)
	for {
		count, _, err := transport.receive.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		value := append([]byte(nil), buffer[:count]...)
		select {
		case transport.values <- value:
		case <-transport.done:
			return
		}
	}
}

func (transport *UDPTransport) Announcements() <-chan []byte { return transport.values }

func (transport *UDPTransport) Publish(ctx context.Context, value []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-transport.done:
		return ErrTransportClosed
	default:
	}
	if err := transport.send.SetWriteDeadline(deadline(ctx)); err != nil {
		return err
	}
	_, err := transport.send.WriteToUDP(value, transport.target)
	return err
}

func deadline(ctx context.Context) time.Time {
	if value, ok := ctx.Deadline(); ok {
		return value
	}
	return time.Now().Add(time.Second)
}

func (transport *UDPTransport) Close() error {
	var result error
	transport.close.Do(func() { close(transport.done); result = errors.Join(transport.receive.Close(), transport.send.Close()) })
	return result
}

type MemoryNetwork struct {
	mu    sync.RWMutex
	peers map[*memoryTransport]struct{}
}
type memoryTransport struct {
	network *MemoryNetwork
	values  chan []byte
	done    chan struct{}
	close   sync.Once
}

func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{peers: make(map[*memoryTransport]struct{})}
}
func (network *MemoryNetwork) NewTransport() Transport {
	peer := &memoryTransport{network: network, values: make(chan []byte, 32), done: make(chan struct{})}
	network.mu.Lock()
	network.peers[peer] = struct{}{}
	network.mu.Unlock()
	return peer
}
func (peer *memoryTransport) Announcements() <-chan []byte { return peer.values }
func (peer *memoryTransport) Publish(ctx context.Context, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	peer.network.mu.RLock()
	targets := make([]*memoryTransport, 0, len(peer.network.peers))
	for target := range peer.network.peers {
		targets = append(targets, target)
	}
	peer.network.mu.RUnlock()
	for _, target := range targets {
		select {
		case target.values <- append([]byte(nil), value...):
		case <-target.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (peer *memoryTransport) Close() error {
	peer.close.Do(func() {
		close(peer.done)
		peer.network.mu.Lock()
		delete(peer.network.peers, peer)
		peer.network.mu.Unlock()
	})
	return nil
}
