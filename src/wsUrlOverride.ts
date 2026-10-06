// Per-charge-point OCPP URL overrides.
//
// The baseline OCPP endpoint for a simulator comes from WS_URL in its
// .env.sim<N> profile and is only read at boot. To let an operator repoint a
// (test) charger at a different backend WITHOUT hand-editing the .env file and
// manually restarting, we persist an optional per-CP_ID override here. The
// override wins over WS_URL at boot (see index_16.ts), so it survives crash
// restarts and full process relaunches, and the "reset" action just clears it
// to fall back to the .env baseline.
//
// Stored as a small JSON map keyed by CP_ID at the repo root:
//   { "gcp-sim11": "ws://cosmos.eosvolt.com" }
//
// Changing an override takes effect on the next (re)connect; the /ws-url admin
// endpoint persists the value and then restarts the process so the fresh boot
// picks it up (mirrors the /restart flow).
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { logger } from "./logger";

// The launch scripts (run_one_sim.sh / run_simulators.sh / the web server) all
// cd to the repo root before starting node, same assumption dotenv makes when
// it loads .env from the cwd, so the overrides file lives at the cwd root.
const OVERRIDES_FILE = join(process.cwd(), "ws-url-overrides.json");

// Normalize to the bare-host form the VCP expects: trimmed, no trailing
// slash(es). The backend routes by CP_ID via a path the VCP appends as
// "/<CP_ID>", so a trailing slash here would yield a double slash.
export function normalizeWsUrl(url: string): string {
  return url.trim().replace(/\/+$/, "");
}

function readAll(): Record<string, string> {
  if (!existsSync(OVERRIDES_FILE)) return {};
  try {
    const parsed = JSON.parse(readFileSync(OVERRIDES_FILE, "utf8"));
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, string>;
    }
    logger.warn(`Ignoring malformed ${OVERRIDES_FILE} (not an object)`);
    return {};
  } catch (err) {
    logger.warn(`Could not read ${OVERRIDES_FILE}: ${err}`);
    return {};
  }
}

function writeAll(map: Record<string, string>): void {
  writeFileSync(OVERRIDES_FILE, `${JSON.stringify(map, null, 2)}\n`);
}

// The persisted override for a CP, or null when none is set.
export function readWsUrlOverride(cpId: string): string | null {
  const v = readAll()[cpId];
  return v ? normalizeWsUrl(v) : null;
}

// Persist (or replace) the override for a CP. Returns the normalized value.
export function writeWsUrlOverride(cpId: string, url: string): string {
  const normalized = normalizeWsUrl(url);
  const map = readAll();
  map[cpId] = normalized;
  writeAll(map);
  logger.info(`WS_URL override for ${cpId} set to ${normalized}`);
  return normalized;
}

// Remove the override for a CP (revert to the .env WS_URL baseline).
export function clearWsUrlOverride(cpId: string): void {
  const map = readAll();
  if (cpId in map) {
    delete map[cpId];
    writeAll(map);
    logger.info(
      `WS_URL override for ${cpId} cleared (reverting to .env WS_URL)`,
    );
  }
}
