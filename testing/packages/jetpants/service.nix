{ config, lib, pkgs, ... }:
let
  inherit (lib) mkIf mkOption types;

  # Okay, this is a bit funny looking but bear with me:

  # 1. This file contains a function.
  # 2. The `{ ... }:` syntax (the `:`) makes it a function.
  # 3. When the function is called, it is passed in `config` which is
  #    the config  of the whole system.
  # 4. We return something like a dictionary which contains `options`
  #    and `config`.
  # 5. The options and config get merged together with the greater
  #    system.

  # In this way, we specify an option called
  # `services.jetpants.enable` (this is what we set to true in the
  # [`configuration.nix`](../../configuration.html))
  cfg = config.services.jetpants;
in {
  options = {
    services.jetpants = {
      enable = mkOption {
        type = types.bool;
        default = false;
      };
    };
  };

  config = mkIf cfg.enable rec {
    # If `services.jetpants.enable` is `true`, we create
    # `jetpants.yaml` on the host. It reads the contents from
    # [`./jetpants.yaml`](./jetpants.html) and then replaces the
    # placeholder `%gzip%` to an actual path to gzip.

    # Note this `jetpants.yaml` is configured to use our local
    # collins. This could be made configurable, but was skipped for
    # hack week.
    environment.etc."jetpants.yaml".source = (pkgs.mutatedScript {
      name = "jetpants.yaml";
      src = ./jetpants.yaml;
      doCheck = false;
      mutate = ''
        substituteInPlace $loc \
          --replace "%gzip%" "${pkgs.gzip}"
      '';
    });
  };
}
