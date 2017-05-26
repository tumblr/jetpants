{ testname,
  starting-spare-dbs ? 0,
  starting-slave-dbs ? 1,
  test-script }:
        # NixOS has this incredible make-test.nix helper which creates a QEMU
# instance running NixOS. Here, we use it:
import <nixpkgs/nixos/tests/make-test.nix> ({ pkgs, ...} :

let

  # Define an SSH key that is only used in this test.
  sshKey = pkgs.runCommand "sshkey" {} ''
    mkdir $out
    ${pkgs.openssh}/bin/ssh-keygen -f $out/key -N "";
  '';

  # Create an SSH config for this test, too, which turns off key
  # checking. This is to simplify the test, and since the test has no
  # external network access, it shouldn't matter.
  # Note we could do `IdentityFile ${sshKey}/id_rsa` here but SSH will
  # complain about the private key being world-readable, so we copy it
  # in to place later.
  sshConfig = pkgs.writeText "sshconfig" ''
    IdentityFile /root/.ssh/id_rsa
    StrictHostKeyChecking=no
    UserKnownHostsFile=/dev/null
  '';
in {
  # Name the test `cap-shard`
  name = testname;

  # Here we're specifying the configuration for the test. We could
  # specify multiple servers here, but we only need the one.
  # only
  machine =
    { pkgs, config, lib, ... }:
    {
      imports = [
        # Dive in to [`./configuration.nix`](./configuration.html) to
        # fall down the rabbit hole
        ./configuration.nix
      ];

      # Tell our root user on the test system (and thusly the
      # containers) to use the sshKey we generated above
      users.extraUsers.root.openssh.authorizedKeys.keyFiles = [ "${sshKey}/key.pub" ];

      # These containers are _not_ Docker containers, but instead Systemd
      # based. They run their own journal, mysql, and ssh services. This
      # is important, because Jetpants expects the mysql servers to be
      # fully capable servers.
      services.mysql-containers = {
        containers = let
          f = n: m: if n == m then [] else [n] ++ f (n + 1) m;
        in builtins.listToAttrs
          (map (n: { name = "${toString n}"; value = {}; })
            (f 10 (10 + 1 + starting-slave-dbs + starting-spare-dbs)) );  };


      systemd.services.snakeoilSSHCredentials = {
        wantedBy = [ "default.target" ];
        before = [ "default.target" ];
        serviceConfig = {
          type = "oneshot";
          RemainAfterExit = true;
        };

        script = ''
          mkdir /root/.ssh
          pushd /root/.ssh
          cat ${sshConfig} > config
          cat ${sshKey}/key > id_rsa
          chmod 600 id_rsa
          popd
        '';
      };

      systemd.services.setup-infinity-slave = {
        requiredBy = [ "multi-user.target" ];
        wantedBy = [ "multi-user.target" ];
        before = [ "multi-user.target" ];
        after = [
          "snakeoilSSHCredentials.service"
          "collins-intake-mysql-container-10-start.service"
          "collins-intake-mysql-container-11-start.service"
          "collins-build-sacrificial.service"
        ];
        enable = true;

        serviceConfig = {
          type = "oneshot";
          TimeoutStartSec = 6000;
        };

        preStart = (toString (pkgs.jetpants.ruby_script "setup-infinity-shards" ''
            p = Jetpants.pool('posts-1-infinity')
            (0...${toString starting-slave-dbs}).each do |n|
              slave = "10.50.2.#{11 + n}".to_db
              until (slave.running?) do
                slave.probe!
              end
              slave.claim!
              slave.change_master_to p.master
              slave.resume_replication
            end
            p.sync_configuration

            p.master.mysql_root_cmd('CREATE TABLE myapp.posts (id MEDIUMINT(8) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, user_id INT NOT NULL, message VARCHAR(255) NOT NULL) ENGINE=innodb;')
            p.master.mysql_root_cmd("INSERT INTO myapp.posts (user_id, message) VALUES (1, 'hi there');")
            p.master.mysql_root_cmd("INSERT INTO myapp.posts (user_id, message) VALUES (2, 'ok here is another row');")
          ''));


        script = ":";
      };

      virtualisation.docker.storageDriver = "overlay2";
      virtualisation.graphics = false;
      # We need quite a bit of RAM and disk to support all the mysql
      # containers.
      virtualisation.memorySize = 4096;
      virtualisation.diskSize = 5120;
      virtualisation.qemu.options = ["-smp 12"];
      # virtualisation.cores = 12; Can use this after 17.09 is released
    };

  # testScript is a perl script (hi Bnu) with a set of tools
  # specifically designed for this type of testing.
  testScript = ''
    # Start all the QEMU machines
    startAll;

    # Wait for our machine (described above) to be in the `default`
    # target, which indicates our containers and collins have all
    # started.
    $machine->waitForUnit("default.target");
    $machine->succeed("systemd-cat -t testscript ${test-script}");
  '';
})
