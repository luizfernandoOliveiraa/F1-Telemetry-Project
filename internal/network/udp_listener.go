package network

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

type UDPPacket struct {
	Data   []byte
	Length int
}

type UDPListener struct {
	port       int
	conn       *net.UDPConn
	packetChan chan UDPPacket
	pool       *sync.Pool
	ctx        context.Context
	cancel     context.CancelFunc
	running    bool
	mu         sync.Mutex
}

func NewUDPListener(port int, packetChan chan UDPPacket) *UDPListener {
	return &UDPListener{
		port:       port,
		packetChan: packetChan,
		pool: &sync.Pool{
			New: func() interface{} {
				// The maximum F1 packet size is around 1460-2048 bytes
				return make([]byte, 2048)
			},
		},
	}
}

// Start starts listening on the UDP port
func (l *UDPListener) Start(ctx context.Context) error {
	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return fmt.Errorf("UDP listener is already running")
	}

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("0.0.0.0:%d", l.port))
	if err != nil {
		l.mu.Unlock()
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		l.mu.Unlock()
		return err
	}

	l.conn = conn
	l.ctx, l.cancel = context.WithCancel(ctx)
	l.running = true
	l.mu.Unlock()

	// Ingestion loop
	go func() {
		defer func() {
			l.mu.Lock()
			l.running = false
			l.conn.Close()
			l.mu.Unlock()
		}()

		for {
			select {
			case <-l.ctx.Done():
				return
			default:
				// Get a buffer from the pool
				buf := l.pool.Get().([]byte)

				// Set a read deadline so we check l.ctx.Done periodically if no packets arrive
				_ = l.conn.SetReadDeadline(time.Now().Add(1 * time.Second))

				n, _, err := l.conn.ReadFromUDP(buf)
				if err != nil {
					// Put buffer back if read failed (timeouts are expected when idle)
					l.pool.Put(buf)
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue
					}
					// Exit loop if connection is closed or has a fatal error
					select {
					case <-l.ctx.Done():
						return
					default:
						fmt.Printf("UDP Read error: %v\n", err)
						return
					}
				}

				// Send raw packet bytes to channel
				select {
				case l.packetChan <- UDPPacket{Data: buf, Length: n}:
				case <-l.ctx.Done():
					l.pool.Put(buf)
					return
				default:
					// Channel buffer is full! Drop packet to avoid blockages and recycle buffer
					l.pool.Put(buf)
				}
			}
		}
	}()

	return nil
}

// Stop stops the UDP listener
func (l *UDPListener) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.running {
		return
	}
	if l.cancel != nil {
		l.cancel()
	}
	if l.conn != nil {
		l.conn.Close()
	}
	l.running = false
}

// Recycle returns a buffer back to the sync.Pool
func (l *UDPListener) Recycle(buf []byte) {
	l.pool.Put(buf)
}
