{ stdenv, shellcheck }:
{ name, src, mutate, doCheck ? true }:
  stdenv.mkDerivation {
  inherit name src doCheck;

  loc = "./${name}";

  unpackPhase = ''
    cp $src $loc
  '';

  buildPhase = mutate;

  postBuild = ''
    patchShebangs $loc

  '';

  checkPhase = ''
    ${shellcheck}/bin/shellcheck -x $loc
  '';

  installPhase = ''
    install -D -m755 $loc $out
  '';
}
