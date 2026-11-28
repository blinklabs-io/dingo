{self, ...}: {
  perSystem = {
    config,
    pkgs,
    lib,
    ...
  }: {
    packages.default = pkgs.buildGo126Module {
      pname = "dingo";
      version = "0-unstable-${self.shortRev or "dirty"}";
      src = lib.cleanSource ../.;
      vendorHash = "sha256-ovuJlOsi1wnsL0kaavL/k8Z+902B9o229M8/AsowIgo=";
      subPackages = ["cmd/dingo"];
      tags = ["dingo_extra_plugins"];
      env = {
        CGO_ENABLED = "0";
        GOTOOLCHAIN = "local";
      };
      ldflags = [
        "-s"
        "-w"
        "-X github.com/blinklabs-io/dingo/internal/version.CommitHash=${self.shortRev or "dirty"}"
      ];
      # Node conformance and race tests run explicitly in the development shell.
      doCheck = false;
      meta = {
        description = "Cardano node written in Go";
        license = lib.licenses.asl20;
        mainProgram = "dingo";
      };
    };
    apps.default = {
      type = "app";
      program = lib.getExe config.packages.default;
      meta.description = config.packages.default.meta.description;
    };
  };
}
