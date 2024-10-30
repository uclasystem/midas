#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR=$( cd -- "$SCRIPT_DIR/.." &> /dev/null && pwd )

apps=( MediaService UrlShortenService PostStorageService UserMentionService ComposePostService SocialGraphService UserService HomeTimelineService TextService UserTimelineService UniqueIdService Client )

rm -rf ${ROOT_DIR}/services
mkdir -p ${ROOT_DIR}/services

for app in ${apps[@]}
do
#     echo $app
    cp ${ROOT_DIR}/build/src/$app/$app ${ROOT_DIR}/services/
done