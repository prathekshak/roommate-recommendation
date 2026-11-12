#!/bin/sh
set -eu
cd "$(dirname "$0")"
python3 ml/preprocess.py >/dev/null
trap 'kill 0' INT TERM EXIT
python3 ds/service.py --node A --config config.json &
python3 ds/service.py --node B --config config.json &
python3 ds/service.py --node C --config config.json &
wait
