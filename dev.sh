#!/usr/bin/env bash

set -euo pipefail

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

function usage {
    CMD="dev.sh"

    cat <<EOT
Helper script for Proactive Query Analyzer

Usage:
    ${CMD} help                 Print this message and quit
    ${CMD} init                 Generate the .env file with basic configurations
    ${CMD} build [--docker]     Build the project optionally the docker images as well
    ${CMD} run-jar [...]        Run analyzer jar file
    ${CMD} run-docker [...]     Run analyzer jar file in cli docker container
EOT
}

function cmd-version() {
    QA_VERSION=$(grep -Po 'sdlQueryAnalyzerVersion\s*=\s*['\''"]\K[^'\''"]+' build.gradle)
    echo "Proactive query analyzer version: $QA_VERSION"

    if [[ -z $QA_VERSION ]]; then
        echo "** Couldn't find the version of proactive query analyzer"
        exit 1
    fi
}

function generate_password() {
    set +o pipefail
    r=$(LC_ALL=C tr -cd '[:alnum:]' </dev/urandom | fold -w24 | head -n 1)
    set -o pipefail
    password="${r:0:6}-${r:6:6}-${r:12:6}-${r:18:6}"
    echo "$password"
}

function cmd-init() {

    if [[ -f .env ]]; then
        echo ".env file exists; not overwriting"
        return 1
    fi

    root_password=$(generate_password)
    db_password=$(generate_password)
    volume_base="$SCRIPT_PATH/data/volume"
    api_port=8080

    cmd-version

    mkdir -p "$volume_base"

    echo "Generating .env file; please adjust accordingly"
    cat <<EOT >"$SCRIPT_PATH/.env"
MYSQL_ROOT_PASSWORD=$root_password
MYSQL_PROACTIVE_QUERY_ANALYZER_PASSWORD=$db_password
VOLUME_BASE=$volume_base
SDL_QUERY_ANALYZER_VERSION=$QA_VERSION
SDL_QUERY_ANALYZER_PORT=$api_port
EOT

}

function cmd-build() {
    cmd-version

    pushd webservice >/dev/null
    ./gradlew clean
    popd >/dev/null

    ./gradlew build

    if [[ "$*" == *"--docker"* ]]; then
        build_docker_image cli
        build_docker_image webservice
    fi
}

function build_docker_image() {

    if [[ $1 == 'cli' ]]; then
        docker_file='cli.Dockerfile'
        docker_image="secure-dl/query-analyzer-cli:$QA_VERSION"
        docker_image_latest="secure-dl/query-analyzer-cli:latest"

    elif [[ $1 == 'webservice' ]]; then

        docker_file='Dockerfile'
        docker_image="secure-dl/query-analyzer-webservice:$QA_VERSION"
        docker_image_latest="secure-dl/query-analyzer-webservice:latest"
    else
        echo "Invalid docker image name"
        exit 1
    fi

    existing_containers=$(docker ps -a -q -f "ancestor=${docker_image}")

    if [[ -n $existing_containers ]]; then
        # shellcheck disable=SC2086
        docker rm ${existing_containers}
    fi

    existing_image=$(docker images -q "$docker_image")

    if [[ -n $existing_image ]]; then
        echo "$existing_image"
        docker rmi "$docker_image"
    fi

    docker build . -f $docker_file -t "$docker_image" -t "$docker_image_latest" --build-arg "SDL_QUERY_ANALYZER_VERSION=$QA_VERSION"
}

function cmd-run-jar() {
    cmd-version
    java -jar "query-analyzer/build/libs/proactive-query-analyzer-${QA_VERSION}.jar" "$@"
}

function cmd-run-docker() {
    docker run --rm -v "${SCRIPT_PATH}/samples/db:/db" secure-dl/query-analyzer-cli "$@"
}

if [[ -z "$*" || "$1" == '-h' || "$1" == '--help' || "$1" == 'help' ]]; then
    usage
    exit 0
fi

command="cmd-${1}"

if [[ $(type -t "${command}") != "function" ]]; then
    echo "Error: No command found"
    usage
    exit 1
fi

pushd "${SCRIPT_PATH}" >/dev/null || exit 1

${command} "${@:2}"

popd >/dev/null || exit 1
