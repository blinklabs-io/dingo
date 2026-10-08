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

package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/blinklabs-io/dingo/internal/secretfile"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
)

// secretFileSetting pairs a literal secret with a file holding it. The two
// are one setting: a source that sets either replaces both from every
// lower-precedence source, and a single source setting both is an error.
// Presence counts as set, even with an empty value: a YAML key, a defined
// environment variable, or a flag passed on the command line.
type secretFileSetting struct {
	field, fileField string
	yaml, fileYAML   string
	env, fileEnv     string
	flag, fileFlag   string
}

var secretFileSettings = []secretFileSetting{
	{
		field:     "KoiosParity.APIKey",
		fileField: "KoiosParity.APIKeyFile",
		yaml:      "koiosParity.apiKey",
		fileYAML:  "koiosParity.apiKeyFile",
		env:       "DINGO_KOIOS_PARITY_API_KEY",
		fileEnv:   "DINGO_KOIOS_PARITY_API_KEY_FILE",
		flag:      "koios-parity-api-key",
		fileFlag:  "koios-parity-api-key-file",
	},
}

// checkSecretFileYAML rejects a YAML document that sets both forms of one
// secret. The decoded Config cannot tell an absent key from an empty one, so
// it inspects the document itself.
func checkSecretFileYAML(buf []byte) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(buf, &doc); err != nil {
		return nil //nolint:nilerr // the decoder reports parse errors
	}
	if len(doc.Content) == 0 {
		return nil
	}
	root := doc.Content[0]
	if configNode := mappingValue(root, "config"); configNode != nil {
		root = configNode
	}
	for _, s := range secretFileSettings {
		if yamlPathSet(root, s.yaml) && yamlPathSet(root, s.fileYAML) {
			return fmt.Errorf(
				"%s and %s are both set; set only one",
				s.yaml,
				s.fileYAML,
			)
		}
	}
	return nil
}

func yamlPathSet(node *yaml.Node, path string) bool {
	for key := range strings.SplitSeq(path, ".") {
		if node = mappingValue(node, key); node == nil {
			return false
		}
	}
	return true
}

// configEnvSet mirrors envconfig.Process("cardano", ...) for the Config
// field at dotted path field, tagged envconfig:"name". envconfig reads a
// prefixed key first, built from CARDANO_ and each enclosing struct field's
// name (CARDANO_KOIOSPARITY_<name> for KoiosParity.APIKey), then name itself.
func configEnvSet(field, name string) bool {
	parts := strings.Split(field, ".")
	prefixed := append([]string{"CARDANO"}, parts[:len(parts)-1]...)
	key := strings.ToUpper(strings.Join(append(prefixed, name), "_"))
	if _, ok := os.LookupEnv(key); ok {
		return true
	}
	_, ok := os.LookupEnv(name)
	return ok
}

// applySecretFileEnvironment runs after envconfig has merged the
// environment over YAML. envconfig sets each variable independently, so an
// environment value would otherwise sit beside the other form from YAML.
func applySecretFileEnvironment(cfg *Config) error {
	for _, s := range secretFileSettings {
		valueSet := configEnvSet(s.field, s.env)
		fileSet := configEnvSet(s.fileField, s.fileEnv)
		switch {
		case valueSet && fileSet:
			return fmt.Errorf(
				"%s and %s are both set; set only one",
				s.env,
				s.fileEnv,
			)
		case fileSet:
			targetValue(cfg, s.field).SetString("")
		case valueSet:
			targetValue(cfg, s.fileField).SetString("")
		}
	}
	return nil
}

func applySecretFileFlags(flags *pflag.FlagSet, cfg *Config) error {
	for _, s := range secretFileSettings {
		valueSet := flags.Changed(s.flag)
		fileSet := flags.Changed(s.fileFlag)
		switch {
		case valueSet && fileSet:
			return fmt.Errorf(
				"--%s and --%s are both set; set only one",
				s.flag,
				s.fileFlag,
			)
		case fileSet:
			targetValue(cfg, s.field).SetString("")
		case valueSet:
			targetValue(cfg, s.fileField).SetString("")
		}
	}
	return nil
}

// ResolveSecretFiles reads each file-backed secret into its setting and
// clears the file path, so calling it again is a no-op. Call it once every
// configuration source has been merged, before the secrets are used.
func (c *Config) ResolveSecretFiles() error {
	for _, s := range secretFileSettings {
		file := targetValue(c, s.fileField)
		if file.String() == "" {
			continue
		}
		value := targetValue(c, s.field)
		if value.String() != "" {
			return fmt.Errorf(
				"%s and %s are both set; set only one",
				s.yaml,
				s.fileYAML,
			)
		}
		contents, err := secretfile.Read(file.String())
		if err != nil {
			return fmt.Errorf("%s: %w", s.fileYAML, err)
		}
		value.SetString(contents)
		file.SetString("")
	}
	return nil
}
