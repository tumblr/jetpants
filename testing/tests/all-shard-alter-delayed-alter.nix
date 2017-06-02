import ../make-test.nix  ({ helpers, pkgs, ... }:
{
  name = "all-shard-alter-delayed-alter";
  starting-spare-dbs = 4;

  test-phases = with helpers; [
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
