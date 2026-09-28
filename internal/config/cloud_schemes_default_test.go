//go:build !dingo_extra_plugins

package config

func unsupportedCloudSchemeTestError(string) string {
	return "unavailable in this build"
}
