f:
let
  pkgs = import <nixpkgs> {
    config = (import ./packages/packages.nix).nixpkgs.config;
  };
  helpers = pkgs.callPackage ./test-helpers.nix {};

  test = f { inherit helpers pkgs; };
  name = test.name;
  body = removeAttrs test ["name"];
in helpers.verify-test-case name body
