#!/usr/bin/env bash
# Starts the web panel with its profile and log directories. The container comes up with no
# stations; developers add them from the panel. OCPP_SIM_STATIONS (comma separated charge box
# ids) is optional pre-provisioning: a profile is written for each id that has none yet, in the
# same shape the panel writes (lowest free suffix and admin port from 9901), and the panel then
# starts it like any other.
set -euo pipefail
cd /app
export SIM_PROFILES_DIR=/app/profiles
export SIM_LOG_DIR=/app/logs
export SIM_ADMIN_PORT_BASE="${SIM_ADMIN_PORT_BASE:-9901}"
# The Cosmos node every pre-created station connects to; the image defaults it to localhost and
# a compose stack sets it to its own Cosmos service.
export WS_URL="${WS_URL:-ws://localhost:9000}"
mkdir -p "$SIM_PROFILES_DIR" "$SIM_LOG_DIR"

has_profile_for() { grep -qsx "CP_ID=$1" "$SIM_PROFILES_DIR"/.env.sim* 2>/dev/null; }
port_is_claimed() { grep -qsx "ADMIN_PORT=$1" "$SIM_PROFILES_DIR"/.env.sim* 2>/dev/null; }

IFS=',' read -r -a stations <<< "${OCPP_SIM_STATIONS:-}"
for raw in "${stations[@]}"; do
    station="$(echo "$raw" | tr -d '[:space:]')"
    [ -z "$station" ] && continue
    if has_profile_for "$station"; then
        echo "[simulator] ${station}: profile already present"
        continue
    fi
    n=1
    while [ -e "${SIM_PROFILES_DIR}/.env.sim${n}" ]; do n=$((n + 1)); done
    port="$SIM_ADMIN_PORT_BASE"
    while port_is_claimed "$port"; do port=$((port + 1)); done
    cat > "${SIM_PROFILES_DIR}/.env.sim${n}" <<PROFILE
CP_ID=${station}
WS_URL=${WS_URL}
ADMIN_PORT=${port}
CONNECTORS=${CONNECTORS:-1}
TOKEN=${TOKEN:-SIMTAG1}
CONTINUE_ON_UNKNOWN_MESSAGE_ID=true
METER_PHASE_SAMPLES=${METER_PHASE_SAMPLES:-true}
ACT_DELAY_MS=${ACT_DELAY_MS:-1000}
REPLY_DELAY_MS=${REPLY_DELAY_MS:-0}
AUTHORIZE_REMOTE_TX_REQUESTS=${AUTHORIZE_REMOTE_TX_REQUESTS:-false}
PROFILE
    echo "[simulator] ${station} -> ${WS_URL}/${station}, admin :${port} (.env.sim${n}, pre-provisioned)"
done
# The panel auto-cycles random sessions on every station it does not treat as a "test charger",
# and only test chargers get the fault controls. "*" makes every station one, so nothing a
# developer adds here is ever auto-cycled.
export AUTO_EXCLUDE_CP_IDS="${AUTO_EXCLUDE_CP_IDS:-*}"
echo "[simulator] panel on :${WEB_PORT:-8080}, profiles in ${SIM_PROFILES_DIR}, logs in ${SIM_LOG_DIR}, auto sessions off for: ${AUTO_EXCLUDE_CP_IDS}"
exec npx tsx web/server.ts
