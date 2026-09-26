package connmanager

import (
	"net"
	"strconv"
	"sync"
)

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
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()
	var total float64
	for _, info := range c.connections {
		if info == nil || !info.isNtC || info.trustedLocal != trustedLocal || info.conn == nil {
			continue
		}
		if muxer := info.conn.Muxer(); muxer != nil {
			total += float64(muxer.ReadBufferInUse())
		}
	}
	return total
}
