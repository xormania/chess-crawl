#!/bin/sh
set -eu

# Keep the signing key out of Compose interpolation and container arguments.
MERCURE_PUBLISHER_JWT_KEY=$(cat /run/secrets/mercure_signing_key)
MERCURE_SUBSCRIBER_JWT_KEY=$MERCURE_PUBLISHER_JWT_KEY
export MERCURE_PUBLISHER_JWT_KEY MERCURE_SUBSCRIBER_JWT_KEY

exec caddy run --config /etc/caddy/Caddyfile --adapter caddyfile
