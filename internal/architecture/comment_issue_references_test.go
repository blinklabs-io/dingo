package architecture_test

import (
	"regexp"
	"testing"
)

// issueReference matches a citation of an issue or pull request. Code is read
// without GitHub, so a comment states what the reference stood for instead.
// A bare number needs two digits and the spelled-out forms need a `#` or two
// digits, so CBOR tags (`#6.24`), output indexes (`tx#0`) and the verb "issue"
// ("issue 0 at KES period 0") are not references.
var issueReference = regexp.MustCompile(
	`(?i)(?:(?:^|[^\w&])#\d{2,}\b` +
		`|\b[\w.-]+/[\w.-]+#\d+\b` +
		`|\b[a-z][\w-]*#\d{2,}\b` +
		`|github\.com/\S+/(?:issues|pulls?)/\d+` +
		`|\b(?:issue|pull request|PR)s? (?:#\d+|\d{2,})\b` +
		`|\bthis PR\b)`,
)

func TestIssueReferencePattern(t *testing.T) {
	for _, text := range []string{
		"// see #3678",
		"// TODO (#394)",
		"// fixed in dingo#4598",
		"// blinklabs-io/dingo#3854",
		"// gouroboros#17",
		"// https://github.com/blinklabs-io/dingo/issues/4082",
		"// https://github.com/blinklabs-io/dingo/pull/4082",
		"// issue #12",
		"// issue 3776",
		"// PR 5943",
		"// pull request 5943",
		"// this PR moved it",
	} {
		if !issueReference.MatchString(text) {
			t.Errorf("%q is not reported", text)
		}
	}
	for _, text := range []string{
		"// #6.24 wraps the datum",
		"// tag #6.258 marks a set",
		"// spends tx#0 and tx#1",
		"// issue 0 at KES period 0",
		"// &#8212; is an em dash",
		"// see ledger.go#L120",
		"// open pull requests on the relay",
		"// retries 3 times",
	} {
		if issueReference.MatchString(text) {
			t.Errorf("%q is reported", text)
		}
	}
}
