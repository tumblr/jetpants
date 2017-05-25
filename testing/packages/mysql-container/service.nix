{ config, lib, pkgs, ... }:
let
  inherit (lib) mkIf mkOption mkDefault attrValues types;

  # Alright, now we're jumping head-first in to it. If you haven't
  # read [the jetpants service](../jetpants/service.html)... well, god
  # help you
  ipPrefix = "10.50.2.";
  bridgeCidr = 24;


  containerOpts = { name, config, ... }: {
    # Here we're defining a set of options per container. Technically
    # these could be configured per-container, but the defaults work
    # correctly.

    # By default:
    # - the name is "mysql-container-<last-octet>"
    # - the mysql server-id will be the last octet of the IP
    # - the IP address is 10.50.2.<last-octet>
    # - you could also override the mysql package to use on a
    #   particular container, but I haven't tried it.

    # I know this looks out of place compared to jetpants, but look
    # further down where we see `type = types.loaOf types.optionSet;`
    # and you'll see more familiar parts from jetpants. The reason
    # this is different is jetpants accepts config like:
    # ```
    # services.jetpants.enable = true;
    # ```
    # where as this one accepts multiple configurations:
    # ```
    # services.mysql-containers.1.enable = true
    # services.mysql-containers.2.enable = true
    # services.mysql-containers.3.enable = true
    # ```
    config = let def = "${name}"; in rec {
      name = mkDefault "mysql-container-${def}";
      suffix = mkDefault def;
      server-id = mkDefault def;
      localAddress = mkDefault "${ipPrefix}${def}";
    };

    options = {
      # Usually we would have an enable default to false, but since
      # this is (as noted above) multiple configurations, it would be
      # annoying to specify each one you add as enabled, when by
      # default there are none.
      enable = mkOption {
        type = types.bool;
        default = true;
      };

      name = mkOption {
        type = types.str;
      };

      suffix = mkOption {
        type = types.str;
      };

      server-id = mkOption {
        type = types.str;
      };

      localAddress = mkOption {
        type = types.str;
      };

      mysql_package = mkOption {
        type = types.package;
        default = pkgs.percona-server56;
      };
    };
  };

  # This is very similar to what was happening in the `config =` code
  # in the jetpants service, except reworked as a function (that `:`
  # syntax, remember?) to be applied to multiple container
  # configurations.
  makeContainer = target: {
    name = "${target.suffix}${target.name}";
    value = {
      # We are configuring a container here to be in a private network
      # with bridge `br0` and the IP address configured from above.
      autoStart = true;
      privateNetwork = true;
      hostBridge = "br0";
      localAddress = "${target.localAddress}/${toString bridgeCidr}";
      # Inside `config` we are describing the configuration
      # identically to how `configuration.nix` looks. We could set up
      # Collins in here, or jetpants, or anything.
      config = let outer_config = config; in { config, pkgs, ...}: {
        # Jetpants expects to have MySQL
        services.openssh.enable = true;

        # Of course MySQL, configured with the specified server-id and
        # mysql_package
        services.mysql = {
          enable = true;
          package = target.mysql_package;
          extraOptions = ''
            log-bin=mysql-bin
            server-id=${target.server-id}
            innodb_file_per_table=ON
            bind-address=0.0.0.0
            secure_file_priv=
          '';

          # Our puppet tooling automatically primes each server with
          # default credentials, so we'll go ahead and do the same
          # here. You can match these users and passwords to the
          # [`jetpants.yaml`](../jetpants/jetpants.html).
          initialScript = pkgs.writeText "setup.sql" ''
	    GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
	    CREATE DATABASE identity;
	    CREATE TABLE identity.ident (name VARCHAR(25) NOT NULL);
	    INSERT INTO identity.ident (name) VALUES ("${target.name}");
            GRANT ALL PRIVILEGES ON *.* TO 'myapp'@'10.50.2.%' IDENTIFIED BY "password" WITH GRANT OPTION;
            GRANT ALL PRIVILEGES ON *.* TO 'repl'@'10.50.2.%' IDENTIFIED BY "password" WITH GRANT OPTION;
            DROP DATABASE IF EXISTS myapp;
            CREATE DATABASE myapp;
	  '';
        };

        # Jetpants wants certain tools to be installed globally like
        # netstat, netcat, grep. These aren't installed by default in
        # NixOS, so go ahead and add them to `systemPackages` which
        # basically puts them in the global PATH.
        environment.systemPackages = with pkgs; [
          nettools
          gnugrep
          coreutils
          percona-toolkit

          # Provides a fake `service` program to make sure we
          # correctly identify the machine is using systemd. Calling service
          # will break the VM.
          service-stub
        ];


        networking = {
          # We need:
          # - 3306 for mysql replication and clients
          # - 22 for ssh between jetpants and mysql
          # - 7000 for the netcat fast copy chain
          firewall = {
            allowedTCPPorts = [ 3306 22 7000 ];
            allowPing = true;
          };
        };

        # This is a bit funny, but basically we the config from the
        # host and copy its ssh keys in to the container.
        users.extraUsers.root.openssh.authorizedKeys.keyFiles = outer_config.users.extraUsers.root.openssh.authorizedKeys.keyFiles;

        # Same as before, but including the custom packages defined in
        # `packages.nix`
        nixpkgs = outer_config.nixpkgs;
      };
    };
  };
in {
  options = {
    services.mysql-containers = {
      containers = mkOption {
        default = [];
        type = types.loaOf types.optionSet;
        options = containerOpts;
      };
    };
  };

  config = {
    networking = {
      # Configure a `br0` bridge and its IP address / CIDR
      bridges = { br0 = { interfaces = []; }; };
      firewall.extraCommands = "iptables -I FORWARD 1 -i br0 -o br0 -j ACCEPT";
      interfaces = {
        br0 = { ip4 = [ {
          address = "${ipPrefix}1";
          prefixLength = bridgeCidr;
        } ]; };
      };
    };

    # Hook up the container set in
    # `services.mysql-containers.containers` by running
    # `makeContainer` over each one, and then setting the result to
    # `containers`.
    containers = builtins.listToAttrs (map makeContainer
      (attrValues config.services.mysql-containers.containers)
    );
  };
}
