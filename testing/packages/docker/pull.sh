# Reference: docker src contrib/download-frozen-image.sh

source $stdenv/setup

set -x

# Curl flags to handle redirects, not use EPSV, handle cookies for
# servers to need them during redirects, and work on SSL without a
# certificate (this isn't a security problem because we check the
# cryptographic hash of the output anyway).
curl=$(command -v curl)
curl() {
  [[ -n ${token:-} ]] && set -- -H "Authorization: Bearer $token" "$@"
  $curl \
    --location --max-redirs 20 \
    --retry 3 \
    --fail \
    --disable-epsv \
    --cookie-jar cookies \
    --insecure \
    $curlOpts \
    $NIX_CURL_FLAGS \
    "$@"
}

fetchLayer() {
    local url="$1"
    local dest="$2"
    local curlexit=18;

    # if we get error code 18, resume partial download
    while [ $curlexit -eq 18 ]; do
        # keep this inside an if statement, since on failure it doesn't abort the script
        if curl -C - "$url" --output "$dest"; then
            return 0
        else
            curlexit=$?;
        fi
    done

    return $curlexit
}

fetchToken() {
    response=$(curl -s -H "$headers" "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${imageName}:pull")

    echo $response | jq '.token' | xargs echo
}


token=$(fetchToken)
registryUrl="https://registry-1.docker.io"
baseUrl="$registryUrl/$registryVersion"

curl -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
     "$baseUrl/$imageName/manifests/$imageTag" > manifest

cat manifest

mkdir $out

touch $out/manifest.json


config=$(cat manifest | jq --raw-output '.config.digest')
config_name=$(echo $config | sed -e "s/sha256://" -e 's/$/.json/')
curl "$baseUrl/${imageName}/blobs/${config}" > $out/$config_name

printf '[{"Config":"%s","RepoTags": ["%s"], "Layers": [' "$config_name" "${imageName}:${ident}" >> $out/manifest.json


first=1
for digest in $(cat manifest | jq --raw-output '.layers[].digest'); do
    echo "fetching layer $digest"

    layer_name=$(echo $digest | sed -e "s/sha256://" -e 's/$/.tar/')
    fetchLayer "$baseUrl/${imageName}/blobs/${digest}" "$out/$layer_name"
    if [ $first -eq 0 ]; then
        echo "," >> $out/manifest.json
    fi
    printf '"%s"' "$layer_name" >> $out/manifest.json
    first=0
    latest="$layer_name"
done
printf "]}]" >> $out/manifest.json

printf '{"%s": { "%s": "%s" } }' "$imageName" "$imageTag" "$ident" > $out/repositories

cat $out/manifest.json
