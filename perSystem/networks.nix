{
  perSystem = {
    config,
    pkgs,
    lib,
    ...
  }: let
    networks = ["preview" "preprod" "mainnet"];
  in {
    packages = lib.genAttrs networks (network:
      pkgs.writeShellApplication {
        name = "dingo-${network}";
        text = ''
          export CARDANO_DATABASE_PATH="''${CARDANO_DATABASE_PATH:-.dingo-${network}}"
          exec ${lib.getExe config.packages.default} --network ${network} "$@"
        '';
        meta.description = "Run Dingo on ${network}";
      });
    apps = lib.genAttrs networks (network: {
      type = "app";
      program = lib.getExe config.packages.${network};
      meta.description = config.packages.${network}.meta.description;
    });
  };
}
