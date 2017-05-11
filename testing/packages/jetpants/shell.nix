{ pkgs ? import <nixpkgs> {} }:
pkgs.stdenv.mkDerivation {
  name = "jetpants";
  buildInputs = with pkgs; [
    (pkgs.callPackage ./default.nix {})
  ];
}
