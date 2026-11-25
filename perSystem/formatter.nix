{
  perSystem = {config, ...}: {
    treefmt = {
      projectRootFile = "flake.nix";
      programs.alejandra.enable = true;
    };
    formatter = config.treefmt.build.wrapper;
  };
}
