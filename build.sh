#!/usr/bin/env bash

set -ue

# working dir relative to this file
CWD="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

TAG="tools/places:$(<$CWD/VERSION)"
echo "build ${TAG}"
docker build ${CWD}/ \
    --build-arg DOCKER_HUB=${DOCKER_HUB:-index.docker.io} \
    --build-arg http_proxy=${http_proxy:-} \
    --build-arg https_proxy=${https_proxy:-} \
    --build-arg SMDRM_UID=$(id -u) \
    --tag $TAG
mkdir -p /data/geocoder
docker run --rm -it -v /data/geocoder:/app/data $TAG
echo OK
