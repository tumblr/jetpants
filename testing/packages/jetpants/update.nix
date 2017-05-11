{ pkgs ? import <nixpkgs>  {} }:
let
  inherit (pkgs) stdenv ruby bundler bundix mysql55 zlib openssl;
in
stdenv.mkDerivation {
  name = "jetpants-updater";

  buildInputs = [
    ruby
    bundler
    bundix
    percona-server56
    zlib
    openssl
  ];

  shellHook = ''
    set -eux
    cd jetpants
    test -d vendor && rm -rf vendor
    test -d .bundle && rm -rf .bundle
    bundle install --path=vendor
    bundix || true
    mv Gemfile.lock ../
    mv gemset.nix ../
    test -d vendor && rm -rf vendor
    test -d .bundle && rm -rf .bundle
  '';
}
