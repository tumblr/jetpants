{ stdenv, lib, curl, jq, jshon, python, runCommand }:

# Inspired and simplified version of fetchurl.
# For simplicity we only support sha256.

# Currently only registry v1 is supported, compatible with Docker Hub.

{ imageName, imageTag ? "latest", imageId ? null
, sha256, name ? "${imageName}-${imageTag}"
, indexUrl ? "https://index.docker.io"
, registryVersion ? "v2"
, curlOpts ? ""
, ident }:

assert registryVersion == "v2";

let layer = stdenv.mkDerivation {
  name = "foobarbaz";
  inherit imageName imageTag imageId
          indexUrl registryVersion curlOpts ident;

  builder = ./pull.sh;
  detjson = ./detjson.py;

  buildInputs = [ curl jq jshon python ];

  outputHashAlgo = "sha256";
  outputHash = sha256;
  outputHashMode = "recursive";

  impureEnvVars = [
    # This variable allows the user to pass additional options to curl
    "NIX_CURL_FLAGS"
  ];

  # Doing the download on a remote machine just duplicates network
  # traffic, so don't do that.
  preferLocalBuild = true;
};

in runCommand "foobarbaz.tar.gz" {} ''
  tar -C ${layer} -czf $out .
''
