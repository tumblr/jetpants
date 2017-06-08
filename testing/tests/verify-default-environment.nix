import ../make-test.nix  ({ helpers, ... }:
{
  name = "verify-default-environment";

  starting-spare-dbs = 0;

  test-phases = with helpers; [
    (assert-shard-exists "POSTS-1-INFINITY")
    (assert-master-has-n-slaves "POSTS-1-INFINITY" 1)
  ];
})
