import ../make-test.nix  ({ helpers, pkgs, ... }:
{
  name = "all-shard-alter-stopped-mysql";
  starting-spare-dbs = 4;

  test-phases = with helpers; [
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
