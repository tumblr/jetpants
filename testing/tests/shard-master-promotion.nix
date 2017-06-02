import ../make-test.nix  ({ helpers, pkgs, ... }:
{
  name = "shard-master-promotion";
  starting-slave-dbs = 2;

  test-phases = with helpers; [
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
