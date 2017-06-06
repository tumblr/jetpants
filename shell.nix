let
  pkgs = import <nixpkgs> { };

  inherit (pkgs) stdenv callPackage;

  jp = (pkgs.callPackage ./testing/packages/jetpants {});
  env = jp.env;
in stdenv.mkDerivation {
  name = "jetpants-development-environment";

  buildInputs = [
    "${env}/${env.ruby.gemPath}"
    env
    env.ruby
  ];

  shellHook = ''
    export GEM_HOME="${env}/${env.ruby.gemPath}"
  '';
}
