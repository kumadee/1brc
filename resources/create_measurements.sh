#!/bin/bash

set -eo pipefail

script_home=$(dirname $0)

pushd $script_home || exit
java -Xmx12g CreateMeasurementsFast.java $1 >"measurements-$1.txt"
popd || exit
