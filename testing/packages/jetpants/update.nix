{ pkgs ? import <nixpkgs>  {} }:
let
  inherit (pkgs) stdenv ruby bundler bundix mysql55 zlib openssl;


in
stdenv.mkDerivation {
  name = "jetpants-updater";

  src = (import ./gemset-shim.nix).jetpants.src;

  buildInputs = [
    ruby
    bundler
    bundix
    mysql55
    zlib
    openssl
  ];

  shellHook = ''
    set -eux
    cp -r $src ./jp
    chmod -R u+w ./jp

    (
      cd jp
      test -d vendor && rm -rf vendor
      test -d .bundle && rm -rf .bundle
      bundle install --path=vendor
      bundix || true
      echo "If you saw errors like "Skipping jetpants: unkown bundler source", we're OK."
      mv Gemfile.lock ../
      mv gemset.nix ../
      test -d vendor && rm -rf vendor
      test -d .bundle && rm -rf .bundle
    )
    rm -rf jp
    exit
  '';
}
