package docsparity_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestExecutableReleaseInputsPinned(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	digest := regexp.MustCompile(`@sha256:[a-f0-9]{64}$`)
	stages := map[string]bool{}
	for _, line := range strings.Split(readRepoFile(t, root, "Dockerfile"), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "FROM" {
			continue
		}
		image := fields[1]
		if !stages[image] && !digest.MatchString(image) {
			t.Errorf("external image is not digest pinned: %s", image)
		}
		if len(fields) >= 4 && strings.EqualFold(fields[2], "AS") {
			stages[fields[3]] = true
		}
	}

	configurators, err := filepath.Glob(filepath.Join(root, "internal/test/*/Dockerfile.configurator"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range configurators {
		for _, line := range strings.Split(string(mustReadFile(t, path)), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "FROM" && !digest.MatchString(fields[1]) {
				t.Errorf("%s: external image is not digest pinned: %s", filepath.Base(filepath.Dir(path)), fields[1])
			}
		}
	}

	composeFiles, err := filepath.Glob(filepath.Join(root, "internal/test/*/docker-compose*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range composeFiles {
		for _, line := range strings.Split(string(mustReadFile(t, path)), "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "image:") {
				continue
			}
			value := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "image:")))[0]
			if strings.HasPrefix(value, "${") {
				fallback := strings.SplitN(value, ":-", 2)
				if len(fallback) != 2 || !strings.HasSuffix(fallback[1], "}") {
					t.Errorf("%s: image variable must have a digest-pinned default: %s", filepath.Base(filepath.Dir(path)), value)
					continue
				}
				value = strings.TrimSuffix(fallback[1], "}")
			}
			if value == "dingo-koios-parity:local" {
				continue
			}
			if !digest.MatchString(value) {
				t.Errorf("%s: external test image is not digest pinned: %s", filepath.Base(filepath.Dir(path)), value)
			}
		}
	}
	paths, err := filepath.Glob(filepath.Join(root, ".github/workflows/*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	sha := regexp.MustCompile(`@[a-f0-9]{40}$`)
	const readOnlyConventionalWorkflow = "blinklabs-io/actions/.github/workflows/reuseable-conventional-commits.yml@main"
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(content), "\n") {
			line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "-"))
			if strings.HasPrefix(line, "image:") {
				value := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "image:")))[0]
				if !digest.MatchString(value) {
					t.Errorf("%s: service image is not digest pinned: %s", filepath.Base(path), value)
				}
			}
			if !strings.HasPrefix(line, "uses:") {
				continue
			}
			value := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "uses:")))[0]
			if !strings.HasPrefix(value, "./") && !sha.MatchString(value) && value != readOnlyConventionalWorkflow {
				t.Errorf("%s: action is not SHA pinned: %s", filepath.Base(path), value)
			}
		}
	}
	for _, path := range []string{
		".github/workflows/go-test.yml",
	} {
		content := readRepoFile(t, root, path)
		assertDockerRunImagesPinned(t, path, content)
		for _, image := range []string{
			"ghcr.io/blinklabs-io/minio:main@sha256:4682bc04bc46ae19aca740dfcdb7341399669fd7fe23f74271e9d09d36a88ada",
		} {
			if !strings.Contains(content, image) {
				t.Errorf("%s: missing pinned image %s", filepath.Base(path), image)
			}
		}
		for _, image := range []struct {
			ref     string
			command string
		}{
			{
				ref:     "postgres:16@sha256:ca0bd484cb98bf4b24eb1010e73fb3fcbd6714d240fbc1a10eea5b7dbecb641d",
				command: "postgres",
			},
			{
				ref:     "mysql:8@sha256:6ea90827b1100f8f2ae306a539f86d2c264a26ed435a2a9f75551dd5c3aeb242",
				command: "--innodb-file-per-table=OFF",
			},
		} {
			runArgument := regexp.MustCompile(`(?m)^\s+` + regexp.QuoteMeta(image.ref) + `\s+` + regexp.QuoteMeta(image.command) + `(?:\s|$)`)
			if !runArgument.MatchString(content) {
				t.Errorf("%s: docker run does not use pinned image %s", filepath.Base(path), image.ref)
			}
		}
		if strings.Contains(content, "AWS_ACCESS_KEY_ID: minioadmin") || strings.Contains(content, "AWS_SECRET_ACCESS_KEY: minioadmin") {
			t.Errorf("%s: MinIO credentials are exposed before the installer checksum is verified", filepath.Base(path))
		}
		installStart := strings.Index(content, "- name: install-aws-cli")
		bucketSetup := strings.Index(content, "- name: setup-s3-bucket")
		if installStart < 0 || bucketSetup <= installStart {
			t.Errorf("%s: missing ordered AWS CLI installation and bucket setup steps", filepath.Base(path))
			continue
		}
		installer := content[installStart:bucketSetup]
		shellOptions := strings.Index(installer, "set -euo pipefail")
		awsURL := strings.Index(installer, "awscli-exe-linux-x86_64-2.37.3.zip")
		awsChecksum := strings.Index(installer, "78903b609938e6c06a02bd2c157af3af4fe184868e5507d6ed588cb68a7a2764  awscliv2.zip")
		checksumCommand := strings.Index(installer, "| sha256sum --check --strict -")
		archiveExtract := strings.Index(installer, "unzip -q awscliv2.zip")
		awsInstall := strings.Index(installer, "sudo ./aws/install --update")
		credentials := strings.Index(installer, "echo 'AWS_ACCESS_KEY_ID=minioadmin'")
		if shellOptions < 0 || awsURL <= shellOptions || awsChecksum <= awsURL || checksumCommand <= awsChecksum || archiveExtract <= checksumCommand || awsInstall <= archiveExtract || credentials <= awsInstall {
			t.Errorf("%s: AWS CLI must use the pinned archive, verify its checksum, install it, and only then expose MinIO credentials", filepath.Base(path))
		}
	}

	for _, path := range []string{
		"internal/test/conformance/docker-compose.yml",
	} {
		content := readRepoFile(t, root, path)
		for _, image := range []string{
			"postgres:16@sha256:ca0bd484cb98bf4b24eb1010e73fb3fcbd6714d240fbc1a10eea5b7dbecb641d",
			"mysql:8@sha256:6ea90827b1100f8f2ae306a539f86d2c264a26ed435a2a9f75551dd5c3aeb242",
		} {
			if !strings.Contains(content, image) {
				t.Errorf("%s: missing pinned image %s", path, image)
			}
		}
	}

	cardanoNode := "ghcr.io/blinklabs-io/cardano-node:11.1.3-2@sha256:dc2b885811294fc497bcd50224bdf1e51d693f6113c749a3b21be1428af79ef8"
	for _, path := range []string{
		"internal/test/devnet/Dockerfile.configurator",
		"internal/test/devnet/docker-compose.yml",
		"internal/test/erastest/Dockerfile.configurator",
		"internal/test/erastest/docker-compose.yml",
	} {
		if !strings.Contains(readRepoFile(t, root, path), cardanoNode) {
			t.Errorf("%s: missing pinned cardano-node image", path)
		}
	}

	dockerfile := readRepoFile(t, root, "Dockerfile")
	antithesisExpected := []string{
		"version=v0.8.0",
		"expected=h1:bVV5ZAuwaREn4klRSJusYaA+A7NXq8Qi9C/knCaQ2N4=",
		"antithesis-sdk-go@${version}",
		"antithesis-go-instrumentor@${version}",
		"snapshot.debian.org/archive/debian/20261020T000000Z",
		"snapshot.debian.org/archive/debian-security/20261020T000000Z",
		"sha256sum --check --strict -",
	}
	for _, expected := range antithesisExpected {
		if !strings.Contains(dockerfile, expected) {
			t.Errorf("Dockerfile: missing pinned release input verification %q", expected)
		}
	}
	antithesisVersion := strings.Index(dockerfile, "version=v0.8.0")
	antithesisSum := strings.Index(dockerfile, `test "$actual" = "$expected"`)
	antithesisGoGet := strings.Index(dockerfile, `go get "github.com/antithesishq/antithesis-sdk-go@${version}"`)
	antithesisInstall := strings.Index(dockerfile, `go install "github.com/antithesishq/antithesis-sdk-go/tools/antithesis-go-instrumentor@${version}"`)
	if antithesisVersion < 0 || antithesisSum <= antithesisVersion || antithesisGoGet <= antithesisSum || antithesisInstall <= antithesisGoGet {
		t.Error("Dockerfile: verify the pinned Antithesis module checksum before go get and install")
	}
	if strings.Contains(dockerfile, "antithesis-sdk-go@latest") || strings.Contains(dockerfile, "antithesis-go-instrumentor@latest") {
		t.Error("Dockerfile: Antithesis Go tooling must use a pinned version")
	}

}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

func assertDockerRunImagesPinned(t *testing.T, path, content string) {
	t.Helper()
	digest := regexp.MustCompile(`@sha256:[a-f0-9]{64}(?:\s|$)`)
	var command strings.Builder
	collecting := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if !collecting {
			if !strings.Contains(trimmed, "docker run ") {
				continue
			}
			collecting = true
			command.Reset()
		}
		continued := strings.HasSuffix(trimmed, "\\")
		command.WriteString(strings.TrimSuffix(trimmed, "\\"))
		command.WriteByte(' ')
		if continued {
			continue
		}
		if !digest.MatchString(command.String()) {
			t.Errorf("%s: docker run image is not digest pinned: %s", filepath.Base(path), command.String())
		}
		collecting = false
	}
	if collecting {
		t.Errorf("%s: unterminated docker run command", filepath.Base(path))
	}
}
