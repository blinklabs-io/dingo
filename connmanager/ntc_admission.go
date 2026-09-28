package connmanager

import (
	"net"
	"sync"
)

func (c *ConnectionManager) reserveNtCSlot(addr net.Addr) func() {
	ipKey := ""
	if addr != nil &&
		(addr.Network() == "tcp" || addr.Network() == "tcp4" || addr.Network() == "tcp6") {
		ipKey = ipKeyFromAddr(addr)
	}
	c.ntcAdmissionMutex.Lock()
	reason := ""
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
		c.ntcCount--
		if ipKey != "" {
			c.ntcIPConns[ipKey]--
			if c.ntcIPConns[ipKey] == 0 {
				delete(c.ntcIPConns, ipKey)
			}
		}
	})
}
