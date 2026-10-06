#!/usr/bin/env bash
#
# Orchestrator for the GCM OCPP charge-point simulators.
#
# Two connection models are supported via WS_MODE:
#
#   shared (default) — staging serves OCPP over ONE TLS endpoint
#       (wss://cosmos-staging.eosvolt.com) and routes by the CP_ID in the URL
#       path. Each simulator is distinguished by its distinct CP_ID, so no
#       per-port juggling is needed. This is the endpoint verified reachable
#       (the :3000-3100 pool was not reachable from this host).
#
#   ports — legacy model: the server accepts one connection per port from a
#       given IP, so each simulator needs a distinct port from the 3000-3100
#       pool. The script probes the pool (with retries) and pins one simulator
#       to each reachable port. Use this if the pool is exposed (e.g. on VPN):
#           WS_MODE=ports ./run_simulators.sh
#
# In both modes, auto-restart handles in-connection resilience (reconnect on
# drop / after an OCPP Reset).
#
# Usage:
#   ./run_simulators.sh                    # shared mode, run until Ctrl-C
#   RUN_SECONDS=30 ./run_simulators.sh     # run for 30s then tear down (smoke)
#   WS_MODE=ports ./run_simulators.sh      # legacy per-port mode
#
set -uo pipefail

# --- Common configuration (override via environment) ------------------------
WS_MODE="${WS_MODE:-shared}"
NUM_SIMS="${NUM_SIMS:-10}"
INDEX_FILE="${INDEX_FILE:-index_16.ts}"
RUN_SECONDS="${RUN_SECONDS:-0}"       # 0 = run until interrupted
PROBE_TIMEOUT="${PROBE_TIMEOUT:-3}"   # seconds per TCP connect attempt
PROBE_RETRIES="${PROBE_RETRIES:-2}"   # extra attempts before marking a port closed

# --- shared-mode configuration ---------------------------------------------
# If set, overrides each profile's WS_URL (host only, no port/path). Empty ->
# each simulator uses the WS_URL from its own .env.simN file.
SHARED_WS_URL="${SHARED_WS_URL:-wss://cosmos-staging.eosvolt.com}"
SHARED_PROBE_HOST="${SHARED_PROBE_HOST:-cosmos-staging.eosvolt.com}"
SHARED_PROBE_PORT="${SHARED_PROBE_PORT:-443}"

# --- ports-mode configuration ----------------------------------------------
WS_SCHEME="${WS_SCHEME:-ws}"
WS_HOST="${WS_HOST:-cosmos-staging.eosvolt.com}"
PORT_START="${PORT_START:-3000}"
PORT_END="${PORT_END:-3100}"

cd "$(dirname "${BASH_SOURCE[0]:-$0}")"
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# tsx's own loader hooks, invoked directly via `node` below -- see the
# comment on launch_sim() for why this replaces `npm run start:auto-restart`.
REPO_ROOT="$(pwd)"
TSX_PREFLIGHT="${REPO_ROOT}/node_modules/tsx/dist/preflight.cjs"
TSX_LOADER="file://${REPO_ROOT}/node_modules/tsx/dist/loader.mjs"

PIDS=()

# --- TCP reachability probe (no external deps; works on macOS bash 3.2) -----
# Opens /dev/tcp/<host>/<port> in a subshell and kills it if it hangs past the
# timeout, so an unreachable host never blocks the scan.
probe_port() {
  local host="$1" port="$2"
  local attempt
  for ((attempt = 1; attempt <= PROBE_RETRIES + 1; attempt++)); do
    (
      exec 3<>"/dev/tcp/${host}/${port}"
    ) 2>/dev/null &
    local conn_pid=$!
    (
      sleep "$PROBE_TIMEOUT"
      kill -9 "$conn_pid" 2>/dev/null
    ) 2>/dev/null &
    local killer_pid=$!
    if wait "$conn_pid" 2>/dev/null; then
      kill -9 "$killer_pid" 2>/dev/null
      wait "$killer_pid" 2>/dev/null
      return 0
    fi
    kill -9 "$killer_pid" 2>/dev/null
    wait "$killer_pid" 2>/dev/null
  done
  return 1
}

# --- Recursively kill a process and all its descendants ---------------------
kill_tree() {
  local pid="$1" child
  for child in $(pgrep -P "$pid" 2>/dev/null); do
    kill_tree "$child"
  done
  kill "$pid" 2>/dev/null
}

cleanup() {
  echo ""
  echo "[orchestrator] Shutting down ${#PIDS[@]} simulator(s)..."
  local pid
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill_tree "$pid"
  done
  wait 2>/dev/null
  echo "[orchestrator] Done."
}
trap cleanup EXIT INT TERM

# --- Launch one simulator: launch_sim <suffix> <ws_url_override|""> ---------
#
# Invokes `node` directly with tsx's own loader hooks (--require preflight.cjs
# --import loader.mjs) instead of `npm run start:auto-restart`, which was
# really a 5-process chain (npm run -> npm start -> ./start.sh -> npx dotenvx
# run -> tsx) where only the last link did any OCPP work -- the other four
# were pure launch-script overhead (~140MB/sim RSS) that stayed resident for
# the simulator's whole lifetime. Node's native --env-file replaces dotenvx.
#
# AUTO_RESTART=true (inside the tsx process) only reconnects the websocket
# after an OCPP Reset -- it does not survive the process itself crashing
# (e.g. an uncaught exception). Wrap the launch in a restart loop here so a
# crashed simulator comes back on its own instead of silently staying dead.
launch_sim() {
  local suffix="$1" ws_url="$2"
  local log_file="${LOG_DIR}/${suffix}.log"
  local env_file="${REPO_ROOT}/.env.${suffix}"
  if [[ -n "$ws_url" ]]; then
    # Exported WS_URL wins over --env-file: Node's --env-file (like dotenvx)
    # does not override a variable already present in the environment.
    export WS_URL="$ws_url"
  else
    unset WS_URL   # defer to the value in .env.<suffix>
  fi
  : >"$log_file"  # truncate once per orchestrator run, same as before
  (
    export AUTO_RESTART=true
    while true; do
      echo "[orchestrator] (re)starting ${suffix}..." >>"$log_file"
      node --env-file="$env_file" --require "$TSX_PREFLIGHT" --import "$TSX_LOADER" \
        "$INDEX_FILE" >>"$log_file" 2>&1
      local exit_code=$?
      echo "[orchestrator] ${suffix} exited (code ${exit_code}), restarting in 3s..." >>"$log_file"
      sleep 3
    done
  ) &
  PIDS+=("$!")
  unset WS_URL
}

run_shared_mode() {
  echo "[orchestrator] Mode: shared (path-routed by CP_ID)"
  # Pre-flight over TLS (matches how the simulator actually connects). A bare
  # TCP probe gives false negatives against the staging proxy, so use curl and
  # treat ANY HTTP response (incl. 5xx) as "proxy reachable". Advisory only:
  # never abort here -- auto-restart will keep retrying the real WS connection.
  echo "[orchestrator] Pre-flight: https://${SHARED_PROBE_HOST}/ ..."
  if command -v curl >/dev/null 2>&1; then
    local code
    code="$(curl -sk -o /dev/null -w '%{http_code}' --max-time "$((PROBE_TIMEOUT * 3))" "https://${SHARED_PROBE_HOST}/" 2>/dev/null)"
    if [[ -n "$code" && "$code" != "000" ]]; then
      echo "  endpoint reachable (HTTP ${code})."
    else
      echo "  WARNING: could not confirm reachability (curl code '${code:-none}'). Proceeding; auto-restart will retry." >&2
    fi
  else
    echo "  (curl not found; skipping pre-flight, relying on auto-restart)."
  fi
  echo "[orchestrator] Launching ${NUM_SIMS} simulator(s)..."
  local i suffix
  for ((i = 1; i <= NUM_SIMS; i++)); do
    suffix="sim${i}"
    echo "  ${suffix}  ->  ${SHARED_WS_URL:-<from .env.${suffix}>}/<CP_ID>   (log: ${LOG_DIR}/${suffix}.log)"
    launch_sim "$suffix" "$SHARED_WS_URL"
  done
}

run_ports_mode() {
  echo "[orchestrator] Mode: ports (legacy per-port)"
  echo "[orchestrator] Probing ${WS_HOST} ports ${PORT_START}-${PORT_END} for ${NUM_SIMS} open port(s)..."
  local port
  REACHABLE=()
  for ((port = PORT_START; port <= PORT_END; port++)); do
    if probe_port "$WS_HOST" "$port"; then
      REACHABLE+=("$port")
      echo "  port ${port}: OPEN   (assigned #${#REACHABLE[@]})"
      [[ "${#REACHABLE[@]}" -ge "$NUM_SIMS" ]] && break
    else
      echo "  port ${port}: closed"
    fi
  done
  if [[ "${#REACHABLE[@]}" -eq 0 ]]; then
    echo "[orchestrator] ERROR: no reachable ports in ${PORT_START}-${PORT_END}. Aborting." >&2
    exit 1
  fi
  if [[ "${#REACHABLE[@]}" -lt "$NUM_SIMS" ]]; then
    echo "[orchestrator] WARNING: only ${#REACHABLE[@]} reachable port(s); launching that many." >&2
    NUM_SIMS="${#REACHABLE[@]}"
  fi
  echo "[orchestrator] Launching ${NUM_SIMS} simulator(s)..."
  local i suffix
  for ((i = 1; i <= NUM_SIMS; i++)); do
    port="${REACHABLE[$((i - 1))]}"
    suffix="sim${i}"
    echo "  ${suffix}  ->  ${WS_SCHEME}://${WS_HOST}:${port}/<CP_ID>   (log: ${LOG_DIR}/${suffix}.log)"
    launch_sim "$suffix" "${WS_SCHEME}://${WS_HOST}:${port}"
  done
}

# --- Dispatch ---------------------------------------------------------------
case "$WS_MODE" in
  shared) run_shared_mode ;;
  ports)  run_ports_mode ;;
  *) echo "[orchestrator] ERROR: unknown WS_MODE='${WS_MODE}' (use 'shared' or 'ports')." >&2; exit 1 ;;
esac

echo "[orchestrator] ${#PIDS[@]} simulator(s) running (PIDs: ${PIDS[*]})."

if [[ "$RUN_SECONDS" -gt 0 ]]; then
  echo "[orchestrator] Running for ${RUN_SECONDS}s, then tearing down..."
  sleep "$RUN_SECONDS"
  exit 0   # trap cleanup() handles teardown
else
  echo "[orchestrator] Press Ctrl-C to stop."
  wait
fi
