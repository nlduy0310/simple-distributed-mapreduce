#!/usr/bin/env bash

proj_name="simple-distributed-mapreduce"
wd_name=$(basename $(pwd))
if [[ "$wd_name" != "$proj_name" ]]; then
    echo "this script must be run from the main directory $proj_name" >&2
    exit 1
fi

echo "starting master dev server"

source .env
(mkdir bin || true) >/dev/null 2>&1

svr_pid=""

stop() {
    if [[ -n "$svr_pid" ]]; then
        kill "$svr_pid"
        wait "$svr_pid"
        svr_pid=""
    fi
}

cleanup() {
    local ret=$?
    if [[ -n "$svr_pid" ]]; then
        echo "stopping server (PID $svr_pid)"
        stop
    fi

    exit "$ret"
}

trap cleanup SIGINT SIGTERM SIGHUP EXIT

cmd_help() {
    echo "commands: [q] quit, [r] reload"
}

entry="./cmd/master"
bin="./bin/master"
rel_nfs_root="mnt/sample"
opts=(
    --port "${MASTER_PORT}"
    --advertise-address "localhost:${MASTER_PORT}"
    --healthy-duration 10s
    --nfs-root "$(pwd)/${rel_nfs_root}"
    --input "**/*.txt"
)
while true; do
    echo "starting server"

    go build -o "$bin" "$entry"
    "$bin" "${opts[@]}" &
    svr_pid=$!

    echo "server is running (PID $svr_pid)"
    cmd_help

    while read -r cmd; do
        if [[ "$cmd" == "q" ]]; then
            echo "exiting"
            stop
            exit 0
        elif [[ "$cmd" == "r" ]]; then
            echo "reloading"
            stop
            break
        else
            echo "unknown command '$cmd'"
            cmd_help
        fi
    done
done