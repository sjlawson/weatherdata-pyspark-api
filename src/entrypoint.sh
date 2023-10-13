#!/usr/bin/env bash

set -e

echo "Reset DB"

flask db upgrade

echo "starting wgsi"

flask load-data
flask analyze

gunicorn --bind 0.0.0.0:${APP_PORT} weather_data:app
