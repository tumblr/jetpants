import ../make-test.nix  ({ helpers, pkgs, ... }:
{
  name = "simple-shard-cutover";
  starting-spare-dbs = 2;

  test-phases = with helpers; [
    (phase "shard-cutover" ''
      jetpants shard-cutover --cutover-id=10000 --shard-pool=posts
    '')
    (assert-shard-exists "POSTS-1-9999")
    (assert-shard-exists "POSTS-10000-INFINITY")
    (assert-master-has-n-slaves "POSTS-10000-INFINITY" 1)
  ];
})
