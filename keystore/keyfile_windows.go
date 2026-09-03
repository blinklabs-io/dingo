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
	"strings"

	"golang.org/x/sys/windows"
)

// insecureSIDs maps SDDL SID abbreviations and full SID strings to
// human-readable names for groups that must not have access to key
// files.
var insecureSIDs = map[string]string{
	"WD":           "Everyone",
	"S-1-1-0":      "Everyone",
	"BU":           "BUILTIN\\Users",
	"S-1-5-32-545": "BUILTIN\\Users",
	"AU":           "Authenticated Users",
	"S-1-5-11":     "Authenticated Users",
}

// SDDL has four access-allowed ACE forms. Object and callback variants are
// grants just like a basic A ACE and must receive the same trustee checks.
func isAccessAllowedACEType(aceType string) bool {
	switch aceType {
	case "A", "OA", "XA", "ZA":
		return true
	default:
		return false
	}
}

func isKnownNonGrantACEType(aceType string) bool {
	switch aceType {
	case "D", "OD", "XD", // access denied
		"AU", "AL", "OU", "OL", "XU", // audit and alarm
		"ML", "RA", "SP", "TL", "FL": // policy and label ACEs
		return true
	default:
		return false
	}
}

// checkFilePermissions verifies that a key file has appropriate
// access controls on Windows. It converts the file's DACL to an
// SDDL string and rejects files that grant access to Everyone,
// the BUILTIN\Users group, or Authenticated Users.
//
// The implementation stays inside golang.org/x/sys/windows and does not use
// the unsafe package, so security descriptor and SID lifetimes stay owned by
// that package. https://go.dev/issue/73199 was a dangling pointer inside
// those helpers and was fixed there; hand-rolling them here would reopen it.
func checkFilePermissions(path string) error {
	sd, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to get security info for %q: %w",
			path,
			err,
		)
	}
	// GetNamedSecurityInfo frees the Windows heap descriptor itself and
	// returns a Go-heap copy, so sd needs no explicit free here.
	return checkSecurityDescriptor(path, sd)
}

func checkSecurityDescriptor(path string, sd *windows.SECURITY_DESCRIPTOR) error {
	sddl := sd.String()
	if sddl == "" {
		return fmt.Errorf(
			"failed to read security descriptor for %q",
			path,
		)
	}

	return checkSDDL(path, sddl)
}

// checkOpenFilePermissions verifies permissions on an already-opened file.
// On Windows, NTFS prevents replacing a file that is held open, so using
// the file path from the open handle is safe against TOCTOU races.
func checkOpenFilePermissions(f *os.File) error {
	sd, err := windows.GetSecurityInfo(
		windows.Handle(f.Fd()),
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to get security info for %q: %w",
			f.Name(),
			err,
		)
	}
	return checkOpenSecurityDescriptor(f.Name(), sd)
}

func checkOpenSecurityDescriptor(path string, sd *windows.SECURITY_DESCRIPTOR) error {
	daclObject, _, err := sd.DACL()
	if err != nil || daclObject == nil {
		return fmt.Errorf(
			"key file %q has no restrictive DACL: %w",
			path, ErrInsecureFileMode,
		)
	}
	sddl := sd.String()
	if sddl == "" {
		return fmt.Errorf("failed to read security descriptor for %q", path)
	}
	ownerSID, _, err := sd.Owner()
	if err != nil || ownerSID == nil {
		return fmt.Errorf(
			"key file %q has no owner in its security descriptor: %w",
			path, ErrInsecureFileMode,
		)
	}

	dacl := sddlSection(sddl, "D:")
	if dacl == "" {
		return fmt.Errorf(
			"key file %q has no DACL (unrestricted access): %w",
			path, ErrInsecureFileMode,
		)
	}
	return checkOpenDACL(path, ownerSID.String(), dacl)
}

func checkOpenDACL(path, owner, dacl string) error {
	allowed := map[string]bool{
		owner: true,
		"BA":  true, // Built-in Administrators
		"SY":  true, // Local System
		"CO":  true, // Creator Owner
		"OW":  true, // Owner Rights
	}
	// An ACE for the built-in Administrator account is accepted only when the
	// file is owned by Built-in Administrators. In that case every member of
	// that group already has owner-equivalent access, so the LA ace grants
	// nothing the owner does not already have, and BA itself is allowed above.
	//
	// The condition is what keeps this narrow. An LA ace on a file owned by
	// some other principal stays rejected, including the case a domain
	// Administrator shares RID 500 with the local one and differs only in the
	// domain part, which trusteeOwnerMatch must not conflate.
	//
	// Hosts where files are owned by the Administrators group produce owner BA
	// with an LA ace, and so do GitHub's Windows runners.
	if ownerIsBuiltinAdministrators(owner) {
		allowed["LA"] = true
	}
	for {
		start := strings.IndexByte(dacl, '(')
		if start < 0 {
			break
		}
		end := strings.IndexByte(dacl[start:], ')')
		if end < 0 {
			return fmt.Errorf(
				"key file %q has unterminated DACL ACE: %w",
				path, ErrInsecureFileMode,
			)
		}
		ace := dacl[start+1 : start+end]
		fields := strings.Split(ace, ";")
		dacl = dacl[start+end+1:]
		if len(fields) < 6 {
			return fmt.Errorf(
				"key file %q has malformed DACL ACE %q: %w",
				path, ace, ErrInsecureFileMode,
			)
		}
		if !isAccessAllowedACEType(fields[0]) {
			if isKnownNonGrantACEType(fields[0]) {
				continue
			}
			return fmt.Errorf(
				"key file %q has unsupported DACL ACE type %q: %w",
				path, fields[0], ErrInsecureFileMode,
			)
		}
		if allowed[fields[5]] {
			continue
		}
		isOwner, resolveErr := trusteeOwnerMatch(fields[5], owner)
		if resolveErr != nil {
			// One side did not resolve to a SID, so an owner alias cannot be
			// told from a third party. Report that instead of an unexpected
			// trustee: the two need different fixes, and the wrapped cause
			// names which side failed, because an owner string that does not
			// parse arrives here too. Checked before isOwner so that a
			// future (true, err) return cannot turn a denial into a grant.
			return fmt.Errorf(
				"key file %q has DACL trustee %s that could not be compared "+
					"with owner %s: %w: %w",
				path, fields[5], owner, resolveErr, ErrInsecureFileMode,
			)
		}
		if isOwner {
			continue
		}
		return fmt.Errorf(
			"key file %q grants access to unexpected trustee %s, "+
				"which is not the owner %s: %w",
			path, fields[5], owner, ErrInsecureFileMode,
		)
	}
	return nil
}

// ownerIsBuiltinAdministrators reports whether the descriptor owner is the
// Built-in Administrators group. The group SID is resolved through Windows
// rather than compared against a literal, matching how trustee aliases are
// resolved elsewhere in this file.
func ownerIsBuiltinAdministrators(owner string) bool {
	match, err := trusteeOwnerMatch("BA", owner)
	return err == nil && match
}

// trusteeOwnerMatch compares a DACL trustee with the descriptor owner. SDDL
// renders some account SIDs as two-letter aliases, while the owner returned by
// the security descriptor is a canonical SID, so the strings differ for the
// same principal — which would reject a valid owner-only DACL on a runner
// using such an account.
//
// A failed resolution is returned as an error rather than folded into a false
// result, because "could not resolve this alias" and "this trustee is not the
// owner" need different fixes and the caller's message is the only diagnostic
// available when the failure reproduces on Windows CI and nowhere else. It
// never returns (true, non-nil), so a caller may treat any error as no match.
//
// An alias is resolved by asking Windows, not by matching its well-known RID.
// The local administrator alias (LA) and a domain Administrator share RID 500
// and differ only in the domain part, so a suffix match on "-500" treats a
// domain Administrator owner as satisfying an LA ace and admits a trustee who
// is not the owner. Round-tripping the alias through a minimal descriptor
// resolves it against this machine and covers every other alias for free.
func trusteeOwnerMatch(trustee, owner string) (bool, error) {
	if trustee == owner {
		return true, nil
	}
	ownerSID, err := windows.StringToSid(owner)
	if err != nil {
		return false, fmt.Errorf("parse owner SID %q: %w", owner, err)
	}
	trusteeSID, err := resolveTrusteeSID(trustee)
	if err != nil {
		return false, err
	}
	// No runtime.KeepAlive is needed for either SID. StringToSid returns
	// (*SID).Copy, a pointer to the base of a Go []byte, and the alias path
	// returns an interior pointer into a Go-heap descriptor copy; the GC
	// tracks both. windows.EqualSid then passes both through
	// syscall.SyscallN, which is //go:uintptrkeepalive, so the compiler
	// retains both arguments for the duration of the call.
	return windows.EqualSid(trusteeSID, ownerSID), nil
}

// resolveTrusteeSID converts an SDDL trustee to a SID, accepting either a SID
// string or an alias such as LA. Aliases are machine-relative, so they are
// resolved by Windows rather than reconstructed here.
//
// ConvertStringSidToSid, behind windows.StringToSid, already accepts the SID
// string constants, so the first branch handles both canonical SIDs and every
// alias Windows renders into SDDL. The descriptor round-trip is a fallback for
// a token that only the SDDL parser accepts, and the error return is for a
// token neither accepts, which the caller must not treat as a non-owner.
func resolveTrusteeSID(trustee string) (*windows.SID, error) {
	if sid, err := windows.StringToSid(trustee); err == nil {
		return sid, nil
	}
	sd, err := windows.SecurityDescriptorFromString("O:" + trustee)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve trustee alias %q via security descriptor: %w",
			trustee, err,
		)
	}
	sid, _, err := sd.Owner()
	if err != nil {
		return nil, fmt.Errorf(
			"read owner from resolved trustee alias %q: %w", trustee, err,
		)
	}
	if sid == nil {
		return nil, fmt.Errorf(
			"resolved trustee alias %q has no owner SID", trustee,
		)
	}
	// sid is an interior pointer into sd's Go-heap allocation, which keeps
	// that allocation reachable, so sd need not be returned alongside it.
	return sid, nil
}

func sddlSection(sddl, section string) string {
	start := strings.Index(sddl, section)
	if start < 0 {
		return ""
	}
	value := sddl[start+len(section):]
	end := len(value)
	for _, next := range []string{"O:", "G:", "D:", "S:"} {
		if idx := strings.Index(value, next); idx >= 0 && idx < end {
			end = idx
		}
	}
	return value[:end]
}

// checkSDDL parses an SDDL string and returns an error if the DACL
// contains any allow ACEs granting access to well-known insecure
// groups.
func checkSDDL(path, sddl string) error {
	// Extract the DACL portion ("D:" up to the next section).
	daclIdx := strings.Index(sddl, "D:")
	if daclIdx < 0 {
		// No DACL means unrestricted access.
		return fmt.Errorf(
			"key file %q has no DACL (unrestricted access): %w",
			path,
			ErrInsecureFileMode,
		)
	}
	daclStr := sddl[daclIdx+2:]
	// Trim at the SACL section if present.
	if idx := strings.Index(daclStr, "S:"); idx >= 0 {
		daclStr = daclStr[:idx]
	}

	// Walk each ACE (parenthesised entries).
	for {
		start := strings.IndexByte(daclStr, '(')
		if start < 0 {
			break
		}
		end := strings.IndexByte(daclStr[start:], ')')
		if end < 0 {
			return fmt.Errorf(
				"key file %q has unterminated DACL ACE: %w",
				path, ErrInsecureFileMode,
			)
		}
		ace := daclStr[start+1 : start+end]
		daclStr = daclStr[start+end+1:]

		// ACE: type;flags;rights;object;inherit;trustee
		fields := strings.Split(ace, ";")
		if len(fields) < 6 {
			return fmt.Errorf(
				"key file %q has malformed DACL ACE %q: %w",
				path, ace, ErrInsecureFileMode,
			)
		}

		// Only inspect access grants; deny, audit, and policy ACEs do not
		// make the file readable by their trustee.
		if !isAccessAllowedACEType(fields[0]) {
			if isKnownNonGrantACEType(fields[0]) {
				continue
			}
			return fmt.Errorf(
				"key file %q has unsupported DACL ACE type %q: %w",
				path, fields[0], ErrInsecureFileMode,
			)
		}

		trustee := fields[5]
		if name, ok := insecureSIDs[trustee]; ok {
			return fmt.Errorf(
				"key file %q grants access to %s: %w",
				path,
				name,
				ErrInsecureFileMode,
			)
		}
	}

	return nil
}
