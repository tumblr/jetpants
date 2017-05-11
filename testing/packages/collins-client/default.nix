# This works identically to the [jetpants
# package](../jetpants/default.html) so please check that out.
{ stdenv, lib, bundlerEnv, makeWrapper }:
stdenv.mkDerivation rec {
  name = "collins-cli";

  env = bundlerEnv {
    name = "${name}-gems";
    gemfile = ./Gemfile;
    lockfile = ./Gemfile.lock;
    gemset = ./gemset.nix;
  };

  phases = [ "installPhase" ];

  buildInputs = [ makeWrapper ];

  installPhase = ''
    mkdir -p $out/bin
    makeWrapper ${env}/bin/collins $out/bin/collins
  '';
}
