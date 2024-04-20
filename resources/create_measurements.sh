#!/bin/bash

set -eo pipefail

script_home=$(dirname $0)

pushd $script_home || exit
java CreateMeasurementsFast $1
popd || exit
