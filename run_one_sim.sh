#!/usr/bin/env bash
#
# Launch a SINGLE simulator profile by number, with the same crash-restart
# supervision the orchestrator uses -- without pulling in the whole
# NUM_SIMS fleet. Intended for on-demand / dedicated sims (e.g. the
# gcp-sim11 fault-injection charger) that must not permanently add to the
# resident set of the OOM-constrained staging VM.
#
# Usage:
#   ./run_one_sim.sh 11                 # run .env.sim11 until Ctrl-C
#   RUN_SECONDS=30 ./run_one_sim.sh 11  # run 30s then tear down (smoke)
#
# WS_URL is taken from the profile's .env.simN (do NOT export WS_URL here --
# staging is path-routed by CP_ID over the single wss host in the profile).
set -uo pipefail

SUFFIX_NUM="${1:-}"
if [[ -z "$SUFFIX_NUM" || ! "$SUFFIX_NUM" =~ ^[0-9]+$ ]]; then
  echo "usage: $0 <sim-number>   (e.g. $0 11)" >&2
  exit 1
fi

cd "$(dirname "${BASH_SOURCE[0]:-$0}")"
REPO_ROOT="$(pwd)"
INDEX_FILE="${INDEX_FILE:-index_16.ts}"
RUN_SECONDS="${RUN_SECONDS:-0}"
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

suffix="sim${SUFFIX_NUM}"
env_file="${REPO_ROOT}/.env.${suffix}"
if [[ ! -f "$env_file" ]]; then
  echo "[run_one_sim] ERROR: ${env_file} not found." >&2
  exit 1
fi

log_file="${LOG_DIR}/${suffix}.log"
TSX_PREFLIGHT="${REPO_ROOT}/node_modules/tsx/dist/preflight.cjs"
TSX_LOADER="file://${REPO_ROOT}/node_modules/tsx/dist/loader.mjs"

SUP_PID=""
cleanup() {
  echo ""
  echo "[run_one_sim] Shutting down ${suffix}..."
  if [[ -n "$SUP_PID" ]]; then
    # kill the supervisor subshell and its node child
    for child in $(pgrep -P "$SUP_PID" 2>/dev/null); do kill "$child" 2>/dev/null; done
    kill "$SUP_PID" 2>/dev/null
  fi
  wait 2>/dev/null
  echo "[run_one_sim] Done."
}
trap cleanup EXIT INT TERM

: >"$log_file"
echo "[run_one_sim] Launching ${suffix} (env: ${env_file}, log: ${log_file})"
(
  export AUTO_RESTART=true
  unset WS_URL   # defer to the value in .env.<suffix>
  while true; do
    echo "[run_one_sim] (re)starting ${suffix}..." >>"$log_file"
    node --env-file="$env_file" --require "$TSX_PREFLIGHT" --import "$TSX_LOADER" \
      "$INDEX_FILE" >>"$log_file" 2>&1
    echo "[run_one_sim] ${suffix} exited (code $?), restarting in 3s..." >>"$log_file"
    sleep 3
  done
) &
SUP_PID=$!

echo "[run_one_sim] ${suffix} running (supervisor PID ${SUP_PID}). Logs: ${log_file}"
if [[ "$RUN_SECONDS" -gt 0 ]]; then
  echo "[run_one_sim] Running for ${RUN_SECONDS}s, then tearing down..."
  sleep "$RUN_SECONDS"
  exit 0
else
  echo "[run_one_sim] Press Ctrl-C to stop."
  wait
fi
