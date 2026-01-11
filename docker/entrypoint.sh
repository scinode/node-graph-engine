#!/usr/bin/env bash
set -euo pipefail

profile="${AIIDA_PROFILE:-default}"
export AIIDA_PATH="${AIIDA_PATH:-/var/lib/aiida}"
mkdir -p "$AIIDA_PATH"

profiles_list="$(verdi profile list 2>/dev/null || true)"
if ! printf "%s\n" "$profiles_list" | grep -q "^${profile}\\b"; then
  echo "Initializing AiiDA profile: ${profile}"
  verdi quicksetup --non-interactive \
    --profile "${profile}" \
    --email "${AIIDA_EMAIL:-aiida@scinode.local}" \
    --first-name "${AIIDA_FIRST_NAME:-Sci}" \
    --last-name "${AIIDA_LAST_NAME:-Node}" \
    --institution "${AIIDA_INSTITUTION:-SciNode}" \
    --db-backend postgresql \
    --db-host "${AIIDA_DB_HOST:-postgres}" \
    --db-port "${AIIDA_DB_PORT:-5432}" \
    --db-name "${AIIDA_DB_NAME:-aiida}" \
    --db-username "${AIIDA_DB_USER:-aiida}" \
    --db-password "${AIIDA_DB_PASSWORD:-aiida}" \
    --broker-protocol amqp \
    --broker-host "${AIIDA_BROKER_HOST:-rabbitmq}" \
    --broker-port "${AIIDA_BROKER_PORT:-5672}" \
    --broker-username "${AIIDA_BROKER_USER:-aiida}" \
    --broker-password "${AIIDA_BROKER_PASSWORD:-aiida}"
fi

if [ "${AIIDA_START_DAEMON:-1}" = "1" ]; then
  verdi daemon start || true
fi

exec "$@"
