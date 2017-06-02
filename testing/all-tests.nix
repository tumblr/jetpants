(map import [
  ./tests/verify-default-environment.nix
  ./tests/simple-shard-cutover.nix
  ./tests/all-shard-alter-delayed-alter.nix
  ./tests/all-shard-alter-stopped-mysql.nix
  ./tests/shard-master-promotion.nix
  ./tests/shard-master-lockless-promotion.nix
])
