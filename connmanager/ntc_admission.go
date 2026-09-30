package connmanager

import (
	"net"
	"strconv"
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

const ntcBufferedBytesSampleInterval = time.Second

type ntcBufferTracker struct {
	conn         *ouroboros.Connection
	trustedLocal bool
	lastSample   int
	closed       bool
	mu           sync.Mutex
}

func (t *ntcBufferTracker) sample(manager *ConnectionManager) {
	current := 0
	if t.conn != nil {
		if muxer := t.conn.Muxer(); muxer != nil {
			current = muxer.ReadBufferInUse()
		}
	}
	t.record(manager, current)
}

func (t *ntcBufferTracker) close(manager *ConnectionManager) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.sampleLocked(manager)
	manager.addNtCBufferedBytes(t.trustedLocal, -t.lastSample)
	t.lastSample = 0
	t.closed = true
}

func (t *ntcBufferTracker) record(manager *ConnectionManager, current int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	manager.addNtCBufferedBytes(t.trustedLocal, current-t.lastSample)
	t.lastSample = current
}

func (t *ntcBufferTracker) sampleLocked(manager *ConnectionManager) {
	current := 0
	if t.conn != nil {
		if muxer := t.conn.Muxer(); muxer != nil {
			current = muxer.ReadBufferInUse()
		}
	}
	manager.addNtCBufferedBytes(t.trustedLocal, current-t.lastSample)
	t.lastSample = current
}

func (c *ConnectionManager) addNtCBufferedBytes(
	trustedLocal bool,
	delta int,
) {
	if trustedLocal {
		c.ntcTrustedLocalBufferedBytes.Add(int64(delta))
		return
	}
	c.ntcRemoteBufferedBytes.Add(int64(delta))
}

func (c *ConnectionManager) reserveNtCSlot(addr net.Addr, trustedLocal bool) func() {
	ipKey := ""
	if !trustedLocal && addr != nil &&
		(addr.Network() == "tcp" || addr.Network() == "tcp4" || addr.Network() == "tcp6") {
		ipKey = ipKeyFromAddr(addr)
	}
	c.ntcAdmissionMutex.Lock()
	reason := ""
	if trustedLocal {
		if c.trustedLocalNtCCount >= c.config.MaxTrustedLocalNtCConns {
			reason = "trusted_local_limit"
		} else {
			c.trustedLocalNtCCount++
		}
	} else {
		switch {
		case c.ntcCount >= c.config.MaxNtCConns:
			reason = "total_limit"
		case ipKey != "" && c.ntcIPConns[ipKey] >= c.config.MaxNtCConnectionsPerIP:
			reason = "per_ip_limit"
		default:
			c.ntcCount++
			if ipKey != "" {
				c.ntcIPConns[ipKey]++
			}
		}
	}
	if reason == "" && c.metrics != nil {
		c.metrics.ntcConnections.WithLabelValues(strconv.FormatBool(trustedLocal)).Inc()
	}
	c.ntcAdmissionMutex.Unlock()
	if reason != "" {
		if c.metrics != nil {
			c.metrics.ntcRejectedConns.WithLabelValues(reason).Inc()
		}
		c.config.Logger.Warn(
			"listener: node-to-client connection limit reached",
			"reason", reason,
			"remote_addr", addr,
		)
		return nil
	}
	return sync.OnceFunc(func() {
		c.ntcAdmissionMutex.Lock()
		defer c.ntcAdmissionMutex.Unlock()
		if trustedLocal {
			c.trustedLocalNtCCount--
		} else {
			c.ntcCount--
		}
		if c.metrics != nil {
			c.metrics.ntcConnections.WithLabelValues(strconv.FormatBool(trustedLocal)).Dec()
		}
		if ipKey != "" {
			c.ntcIPConns[ipKey]--
			if c.ntcIPConns[ipKey] == 0 {
				delete(c.ntcIPConns, ipKey)
			}
		}
	})
}

func (c *ConnectionManager) ntcBufferedBytes(trustedLocal bool) float64 {
	if trustedLocal {
		return float64(c.ntcTrustedLocalBufferedBytes.Load())
	}
	return float64(c.ntcRemoteBufferedBytes.Load())
}
