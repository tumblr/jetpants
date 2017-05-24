let
  pkgs = import <nixpkgs> {
    config = (import ./packages/packages.nix).nixpkgs.config;
  };
  inherit (pkgs) lib;
  inherit (pkgs.callPackage ./test-helpers.nix {}) verify-test-case
    phase expect-phase jetpants-phase assert-shard-exists
    assert-shard-does-not-exist assert-master-has-n-slaves assert-shard-master assert-shard-slave;

  inherit (pkgs.callPackage ./wrapper.nix {}) build-wrapper;

in (build-wrapper [
  (verify-test-case "verify-default-environment" {
    starting-spare-dbs = 0;

    test-phases = [
      (assert-shard-exists "POSTS-1-INFINITY")
      (assert-master-has-n-slaves "POSTS-1-INFINITY" 1)
    ];
  })
  (verify-test-case "simple-shard-cutover" {
    starting-spare-dbs = 2;

    test-phases = [
      (phase "shard-cutover" ''
        jetpants shard-cutover --cutover-id=10000 --shard-pool=posts
      '')
      (assert-shard-exists "POSTS-1-9999")
      (assert-shard-exists "POSTS-10000-INFINITY")
      (assert-master-has-n-slaves "POSTS-10000-INFINITY" 1)
    ];
  })
  (verify-test-case "all-shard-alter-delayed-alter" {
    starting-spare-dbs = 4;

    test-phases = [
      (phase "shard-cutover-10000" ''
        jetpants shard-cutover --cutover-id=10000 --shard-pool=posts
      '')
      (phase "shard-cutover-20000" ''
        jetpants shard-cutover --cutover-id=20000 --shard-pool=posts
      '')
      (phase "alter-all-shards" ''
        (
          echo "YES" # Aprove after the dry run
          echo "YES" # Aprove after first shard altered
        ) | jetpants alter_table --skip-rename\
                             --all-shards --shard-pool=posts \
                             --database=myapp --table=posts \
                             --alter="add column my_new_column int"
      '')

      (phase "rename-after-alter" ''
        (
          echo "YES" # Execute rename
          echo "YES" # Clean up DSN table
          echo "YES" # Drop triggers
          echo "YES" # Drop pt-osc user
          echo "YES" # Drop _posts_old
          echo "YES" # Run on all shards
        ) | jetpants  alter_table_rename --all-shards \
                                     --database=myapp \
                                     --shard-pool=posts \
                                     --orig-table=posts \
                                     --copy-table=_posts_new
      '')
      (phase "assert-posts-has-new-column" ''
        set -o pipefail

        for i in `seq 10 15`; do
          ${pkgs.mysql}/bin/mysql --host=10.50.2.$i myapp -N -e "describe posts" | grep -q my_new_column
        done
      '')


    ];
   })

  (verify-test-case "all-shard-alter-stopped-mysql" {
    starting-spare-dbs = 4;

    test-phases = [
      (phase "shard-cutover-10000" ''
        jetpants shard-cutover --cutover-id=10000 --shard-pool=posts
      '')
      (phase "shard-cutover-20000" ''
        jetpants shard-cutover --cutover-id=20000 --shard-pool=posts
      '')
      (expect-phase "alter-with-downed-mysql" ''
        # 10 minutes
        set timeout 600

        spawn jetpants console
        set spawns(console) $spawn_id

        spawn jetpants alter_table --all-shards --shard-pool=posts --database=myapp --table=posts "--alter=add column my_new_column int"
        set spawns(alter) $spawn_id

        expect "Dry run complete. Continuing means running the following command:"
        expect "Would you like to continue?"
        send "YES\n"

        expect "Do you want to immediately: Clean up the dsn table? (YES/no)"
        send "YES\n"

        expect "Do you want to immediately: Drop triggers? (YES/no)"
        send "YES\n"

        expect "Do you want to immediately: Clean up pt-osc user? (YES/no)"
        send "YES\n"

        expect "Do you want to immediately: Drop crufty table '_posts_old'? (YES/no)"
        send "YES\n"

        expect "First shard complete would you like to continue with the rest of the shards?:(YES/no)"

        expect -i $spawns(console) "Jetpants >"
        send -i $spawns(console) "slave = pool(\"posts-10000-19999\").slaves.first; nil\n"
        expect -i $spawns(console) -re  "=>.+nil"
        send -i $spawns(console) "slave.stop_mysql; nil\n"
        expect -i $spawns(console) -re "=>.+nil"

        send "YES\n"
        expect "not running!"

        send -i $spawns(console) "slave.start_mysql; nil\n"
        expect -i $spawns(console) -re "=>.+nil"

        expect "Successfully altered `myapp`.`posts`."

        expect "The following keys returned `true':"
        expect "posts-1-9999"
        expect "posts-20000-infinity"
        expect "posts-20000-infinity"
      '')
      (phase "assert-posts-has-new-column" ''
        set -o pipefail

        for i in `seq 10 15`; do
          ${pkgs.mysql}/bin/mysql --host=10.50.2.$i myapp -N -e "describe posts" | grep -q my_new_column
        done
      '')


    ];
  })

  (verify-test-case "shard-master-promotion" {
    starting-spare-dbs = 1;

    test-phases = [
      (phase "slave-clone" ''
        (
          echo "YES" # Confirm, cloning from standby_slave
        ) | jetpants clone_slave --source=10.50.2.11 --target=spare
      '')
      (phase "jetpants-promotion" ''
        (
          echo "YES" # Approve for promotion
          echo "YES" # Approve after summary output. Confirmation.
        ) | jetpants promotion --demote=10.50.2.10 --promote=10.50.2.11
      '')
      (assert-shard-master "POSTS-1-INFINITY" "10.50.2.11")
      (assert-shard-slave "POSTS-1-INFINITY" "10.50.2.10")
      (assert-shard-slave "POSTS-1-INFINITY" "10.50.2.12")
    ];
  })

])
