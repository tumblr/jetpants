{ config, lib, pkgs, ... }:
let
  inherit (lib) last mkIf mkOption types attrValues;
  cfg = config.services.collins;

  # Here we load [`./collins-cli.yaml'](./collins-cli.html) in to the
  # Nix Store, which then makes the `collins_cli` variable a path to
  # the file.
  collins_yml = ./collins-cli.yaml;


  # (containerChain container: "${container.name}.service" [
  #   { name = "foo"; }
  #   { name = "bar"; }
  #   { name = "baz"; }
  #   { name = "tux"; }
  # ]) == [
  #   { container = { name = "foo"; }; after_serialize = ["bar.service"]; }
  #   { container = { name = "bar"; }; after_serialize = ["baz.service"]; }
  #   { container = { name = "baz"; }; after_serialize = ["tux.service"]; }
  #   { container = { name = "tux"; }; after_serialize = []; }
  # ]
  # where `after_serialize` can be used to force tux to start, then baz,
  # then bar, then foo.
  containerChain = f: containers: (
    (lib.zipListsWith
        (first: second: { container = first; after_serialize =  [ (f second) ]; })
        containers
        (lib.drop 1 containers)
        )
    ++ [ { container = (lib.last containers); after_serialize = []; } ]
    );


in {
  options = {
    services.collins = {
      enable = mkOption {
        type = types.bool;
        default = false;
      };

      imageName = mkOption {
        type = types.string;
        default = "tumblr/collins:${pkgs.collins-container.ident}";
      };

      containerName = mkOption {
        type = types.string;
        default = "collins";
      };

      arguments = mkOption {
        type = types.string;
        default = "-p 9000:9000";
      };
    };
  };

  config = mkIf cfg.enable rec {
    # Collins is run in a docker container, so turn docker on for this
    # system
    virtualisation.docker.enable = true;

    # Create a systemd service unit for Collins.
    systemd.services = {
      collins = {
        description = "Collins";
        wantedBy = [ "multi-user.target" ];
        requires = [ "docker.service" ];
        after = [ "network.target" "docker.service" ];
        enable = cfg.enable;

        serviceConfig = {
          Restart = "always";
          TimeoutStartSec = 6000;
          ExecStartPre = [
            # Our first pre-start script will delete any docker
            # container if it was running.
            (pkgs.writeScript "cleanup-container-collins" ''
              #!${pkgs.bash}/bin/bash

              set -eu

              PATH=${pkgs.docker}/bin/

              if docker inspect '${cfg.containerName}'; then
                docker rm -f '${cfg.containerName}'
              fi
            '')

            # The second step in starting the container is loading in
            # the collins image. We also delete the image first, just to
            # be sure we're clean, but with the imageId we could skip
            # this.

            # Note that we're not running `docker pull` because this
            # machine doesn't have any network access.
            (pkgs.writeScript "import-image-collins" ''
              #!${pkgs.bash}/bin/bash

              set -eu

              PATH=${pkgs.docker}/bin/

              docker rmi "${cfg.imageName}" || true
              docker load < ${pkgs.collins-container.img}
            '')
          ];

          # classic docker run, using some local configurations tailored
          # to support jetpants, which the default Collins container
          # doesn't support.
          ExecStart = "${pkgs.docker}/bin/docker run -i "
                + " -v ${./permissions.yaml}:/opt/collins/conf/permissions.yaml:ro"
                + " -v ${./production.conf}:/opt/collins/conf/production.conf:ro"
                + " -v ${./profiles.yaml}:/opt/collins/conf/profiles.yaml:ro"
                + " -v ${./logger.xml}:/opt/collins/conf/logger.xml:ro"
                + " --name '${cfg.containerName}' ${cfg.arguments} '${cfg.imageName}'";


          ExecStartPost = [
            # First step: Block the service from being ready until after
            # it is responding to http requests
            (pkgs.writeScript "test-up" ''
              #!${pkgs.bash}/bin/bash

              set -eu

              PATH=${pkgs.curl}/bin/:${pkgs.gnugrep}/bin:${pkgs.coreutils}/bin

              test_host() {
                set +e

                curl -v http://127.0.0.1:9000 2>&1 | grep -q "Location: "
                RET=$?
                set -e
                return $RET
              }

              while ! test_host; do
                echo "Not yet ready ..."
                sleep 1
              done
            '')

            # Second step: Tell Collins about the `Allocated:Spare`
            # state which jetpants expects spares to be in.
            (pkgs.writeScript "setup-collins-config" ''
              #!${pkgs.bash}/bin/bash

              set -eu

              PATH=${pkgs.curl}/bin:${pkgs.collins-client}/bin

              curl --basic -X PUT -H "Accept: text/plain" \
                -u blake:admin:first \
                -d label='Spare' \
                -d description='Allocated Spare' \
                -d status=Allocated \
                "http://localhost:9000/api/state/spare"

              curl --basic -X PUT -H "Accept: text/plain" \
                -u blake:admin:first \
                "http://127.0.0.1:9000/api/asset/mysql-shard-pool-posts" \
                -d "type=CONFIGURATION"

              collins modify -C  ${collins_yml} \
                -t "mysql-shard-pool-posts" \
                -S allocated -r "Setting up shard pool" \
                -a "shard_pool:POSTS" \
                -a "primary_role:MYSQL_SHARD_POOL"


               curl --basic -X PUT -H "Accept: text/plain" \
                 -u blake:admin:first \
                 "http://127.0.0.1:9000/api/asset/mysql-posts-1-infinity" \
                 -d "type=CONFIGURATION"

              collins modify -C  ${collins_yml} \
                 -t "mysql-posts-1-infinity" \
                 -S allocated -r "Setting up shard" \
                 -a "shard_max_id:INFINITY" \
                 -a "shard_min_id:1" \
                 -a "shard_pool:POSTS" \
                 -a "shard_state:READY" \
                 -a "primary_role:MYSQL_SHARD" \
                 -a "pool:POSTS-1-INFINITY"
            '')
          #]
          # For each container, run that setupMysqlInCollins script,
          # configuring them as allocated spares in the right pool with
          # the right nodeclass and roles.
          #++ (map setupMysqlInCollins (attrValues config.services.mysql-containers.containers))

          # Finally, we pick the first container ("10") and use it as a
          # sacrificial node. This is because jetpants doesn't know what
          # to do if there are no nodes or shards yet. We manually add
          # it as the master of the POSTS-1-INFINITY shard
          ];

          ExecStopPost = "${pkgs.docker}/bin/docker stop ${cfg.containerName}";
        };
      };

      collins-build-sacrificial = let
        sacrificial = config.services.mysql-containers.containers."10"; in
      {
        description = "Configure the first container to be the POSTS-1-INFINITY pool";
        after = [ "collins.service" "collins-intake-${sacrificial.name}.service"
                  "${(last (attrValues config.services.mysql-containers.containers)).name}.service" ];
        wants = [ "collins.service" ];
        wantedBy = [ "collins.service" "multi-user.target" ];
        requiredBy = [ "multi-user.target" ];
        bindsTo = [ "collins.service" ];
        serviceConfig = {
          Type = "oneshot";
        };

        script = ''
          set -eu

          PATH=${pkgs.mysql}/bin:${pkgs.curl}/bin/:${pkgs.gnugrep}/bin:${pkgs.coreutils}/bin:${pkgs.collins-client}/bin

          collins modify -C  ${collins_yml} \
            -t "${sacrificial.name}" \
            -S maintenance:maint_noop -r "Making into pool master"

          collins provision -C  ${collins_yml} \
            -t "${sacrificial.name}" \
            -n databasenode -r DATABASE -b blake \
            --secondary-role MASTER \
            -p POSTS-1-INFINITY

          collins modify -C  ${collins_yml} \
            -t "${sacrificial.name}" \
            -S allocated:running -r "Making into pool master"
        '';
      };
    } // builtins.listToAttrs
      (map ({container, after_serialize}: {
        name = "collins-intake-${container.name}";
        value = {
          description = "Collins MySQL Node Intake: ${container.name}";
          after = [ "collins.service" "container@${container.name}.service" ] ++ after_serialize;
          wants = [ "collins.service" ];
          wantedBy = [ "collins.service" "multi-user.target" ];
          requiredBy = [ "multi-user.target" ];
          bindsTo = [ "collins.service" ];
          serviceConfig = {
            Type = "oneshot";
          };

          script = ''
            PATH=${pkgs.curl}/bin/:${pkgs.collins-client}/bin:${pkgs.coreutils}/bin

            set -eu

            URL="http://localhost:9000/api/asset"
            TAG="${container.name}"
            LLDP_FILE="${./single.lldp}"
            LSHW_FILE="${./virident.lshw}"

            curl --basic -X PUT -H "Accept: text/plain" \
                 -u blake:admin:first "$URL/$TAG"

            curl --basic -H "Accept: text/plain" -u blake:admin:first \
                 --data-urlencode "lldp@$LLDP_FILE" \
                 --data-urlencode "lshw@$LSHW_FILE" \
                 --data-urlencode 'CHASSIS_TAG=Testing this' \
                 "$URL/$TAG"

            collins modify -C "${collins_yml}" -t "${container.name}" -S maintenance:maint_noop -r "Provisioning"
            collins provision -C "${collins_yml}" -t "${container.name}"  -n databasenode  -r DATABASE -b blake
            collins modify -C "${collins_yml}" -t "${container.name}" \
              -S Allocated:SPARE \
              -r "Provisioning" \
              -a "HOSTNAME:${container.name}"
            curl --basic -u blake:admin:first -X POST \
              -d pool=DATABASE \
              -d address=${container.localAddress} \
              -d gateway=10.50.2.1 \
              -d netmask=255.255.255.0 \
              http://localhost:9000/api/asset/${container.name}/address
          '';
        };
      })
      (containerChain (container: "collins-intake-${container.name}.service") (attrValues config.services.mysql-containers.containers)));
  };
}
