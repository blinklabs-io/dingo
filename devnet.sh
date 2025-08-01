#!/usr/bin/env bash

export CARDANO_NETWORK=devnet
export CARDANO_CONFIG=./config/cardano/devnet/config.json
export CARDANO_DATABASE_PATH=.devnet
export CARDANO_DEV_MODE=true

conf=$(dirname $CARDANO_CONFIG)
now=$(date -u +%s)
echo setting start time in $conf to $now
sed -i -e "s/startTime\": .*,/startTime\": $now,/" $conf/byron-genesis.json
sed -i -e "s/systemStart\": .*,/systemStart\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ --date=@$now)\",/" $conf/shelley-genesis.json

echo resetting .devnet
rm -rf .devnet/*

go run ./cmd/dingo/ --debug
