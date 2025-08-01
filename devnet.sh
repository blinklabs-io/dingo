#!/usr/bin/env bash

export CARDANO_NETWORK=devnet
export CARDANO_CONFIG=./config/cardano/${CARDANO_NETWORK}/config.json
export CARDANO_DATABASE_PATH=.devnet
export CARDANO_DEV_MODE=true

echo resetting .devnet
rm -rf .devnet/*

go run ./cmd/dingo/ --debug
