import ../make-test.nix  ({ helpers, ... }:
{
  name = "shard-master-mlockless-promotion";
  starting-slave-dbs = 2;

  test-phases = with helpers; [
    (phase "jetpants-shard-promote-master" ''
      jetpants shard_promote_master --min_id=1 --max_id=infinity --new_master=10.50.2.11 --shard_pool=posts
    '')
    (phase "jetpants-shard_promote_master_reads" ''
      jetpants shard_promote_master_reads --min_id=1 --max_id=infinity --shard_pool=posts
    '')
    (phase "jetpants-shard_promote_master_writes" ''
      (
        echo "YES" # Approve for writes promotion
      ) | jetpants shard_promote_master_writes --shard_pool=posts
    '')
    (phase "jetpants-shard_promote_master_cleanup" ''
      (
        echo "YES" # Approve for old master cleanup
      ) | jetpants shard_promote_master_cleanup --shard_pool=posts
    '')
    (assert-shard-master "POSTS-1-INFINITY" "10.50.2.11")
    (assert-shard-slave "POSTS-1-INFINITY" "10.50.2.12")
 ];
})
