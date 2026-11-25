{
  perSystem = {
    config,
    pkgs,
    ...
  }: {
    devShells.default = pkgs.mkShell {
      packages = with pkgs; [
        go_1_26
        gopls
        golines
        golangci-lint
        gnumake
        stdenv.cc
        git
        config.treefmt.build.wrapper
      ];
      GOTOOLCHAIN = "local";
    };
  };
}
