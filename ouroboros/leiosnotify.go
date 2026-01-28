// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ouroboros

import (
	"fmt"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
)

func (o *Ouroboros) leiosnotifyServerConnOpts() []oleiosnotify.LeiosNotifyOptionFunc {
	return []oleiosnotify.LeiosNotifyOptionFunc{
		oleiosnotify.WithRequestNextFunc(o.leiosnotifyServerRequestNext),
	}
}

func (o *Ouroboros) leiosnotifyClientConnOpts() []oleiosnotify.LeiosNotifyOptionFunc {
	return []oleiosnotify.LeiosNotifyOptionFunc{
		oleiosnotify.WithNotificationFunc(o.leiosnotifyClientNotification),
	}
}

func (o *Ouroboros) leiosnotifyClientStart(connId ouroboros.ConnectionId) error {
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	if conn.LeiosNotify() == nil {
		// Return silently if LeiosNotify protocol is not supported by peer
		return nil
	}
	if err := conn.LeiosNotify().Client.Sync(); err != nil {
		return err
	}
	return nil
}

func (o *Ouroboros) leiosnotifyClientNotification(
	ctx oleiosnotify.CallbackContext,
	msg protocol.Message,
) error {
	conn := o.ConnManager.GetConnectionById(ctx.ConnectionId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", ctx.ConnectionId.String())
	}
	switch m := msg.(type) {
	case *oleiosnotify.MsgBlockOffer:
		resp, err := conn.LeiosFetch().Client.BlockRequest(m.Point)
		if err != nil {
			return err
		}
		respBlock := resp.(*leiosfetch.MsgBlock)
		o.config.Logger.Info(
			fmt.Sprintf(
				"fetched EB %d.%x with size %d",
				m.Point.Slot,
				m.Point.Hash,
				len(respBlock.BlockRaw),
			),
			"component", "network",
			"protocol", "leios-fetch",
			"role", "client",
			"connection_id", ctx.ConnectionId.String(),
		)
	}
	return nil
}

func (o *Ouroboros) leiosnotifyServerRequestNext(
	ctx oleiosnotify.CallbackContext,
) (protocol.Message, error) {
	// TODO
	return nil, nil
}
