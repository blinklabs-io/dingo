//go:build windows

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

package keystore

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func checkOpenFileOwner(f *os.File) error {
	sd, err := windows.GetSecurityInfo(
		windows.Handle(f.Fd()),
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("get key file %q owner: %w", f.Name(), err)
	}
	defer freeSecurityDescriptor(sd)
	owner, _, err := sd.Owner()
	if err != nil || owner == nil {
		return fmt.Errorf(
			"key file %q has no owner: %w", f.Name(), ErrInsecureFileOwner,
		)
	}
	var token windows.Token
	if err := windows.OpenProcessToken(
		windows.CurrentProcess(), windows.TOKEN_QUERY, &token,
	); err != nil {
		return fmt.Errorf("open current process token for key file %q: %w", f.Name(), err)
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil {
		return fmt.Errorf("get current user for key file %q: %w", f.Name(), err)
	}
	if windows.EqualSid(owner, user.User.Sid) {
		return nil
	}
	adminSID, err := windows.CreateWellKnownSid(
		windows.WinBuiltinAdministratorsSid,
	)
	if err != nil {
		return fmt.Errorf("get administrators SID for key file %q: %w", f.Name(), err)
	}
	adminMember := false
	if windows.EqualSid(owner, adminSID) {
		adminMember, err = token.IsMember(adminSID)
		if err != nil {
			return fmt.Errorf(
				"compare key file %q owner with current process: %w", f.Name(), err,
			)
		}
	}
	if !keyFileOwnerAllowed(owner, user.User.Sid, adminSID, adminMember) {
		return fmt.Errorf(
			"key file %q owner %s does not control the current process: %w",
			f.Name(), owner.String(), ErrInsecureFileOwner,
		)
	}
	return nil
}

func keyFileOwnerAllowed(
	owner *windows.SID,
	currentUser *windows.SID,
	administrators *windows.SID,
	processInAdministrators bool,
) bool {
	return windows.EqualSid(owner, currentUser) ||
		(processInAdministrators && windows.EqualSid(owner, administrators))
}
