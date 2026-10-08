// OCPP simulator control panel.
//
// A small Hono server that fronts each simulator's admin HTTP endpoint
// (localhost:<ADMIN_PORT>/health and /execute) and its log file, and serves a
// single dashboard page. Binds to 127.0.0.1 by default — reach it over an SSH
// tunnel so nothing is exposed publicly:
//
//   ssh <simulator host> -N -L 8080:localhost:8080
//   # then open http://localhost:8080
//
// Config comes from the .env.sim* profiles in SIM_PROFILES_DIR (the repo root
// by default), so it stays in sync with whatever the orchestrator runs. The
// panel is also a process manager: POST /api/sims writes a profile and runs
// the station under its own restart loop, DELETE /api/sims/:id stops it, and
// at startup every profile whose admin port is not already answering is
// spawned the same way (stations the shell scripts run stay theirs).

import { type ChildProcess, spawn } from "node:child_process";
import {
  appendFileSync,
  closeSync,
  existsSync,
  fstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readSync,
  readdirSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { serve } from "@hono/node-server";
import { type Context, Hono } from "hono";

const REPO_ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
// Where the .env.sim<N> profiles live. Default: the repo root, which is where
// run_simulators.sh / run_one_sim.sh read them, so a panel started next to the
// shell scripts sees their stations. Spark's dev container points this at
// /app/profiles so no env file ever lands in the checkout.
const PROFILES_DIR = process.env.SIM_PROFILES_DIR
  ? resolve(process.env.SIM_PROFILES_DIR)
  : REPO_ROOT;
const LOG_DIR = process.env.SIM_LOG_DIR
  ? resolve(process.env.SIM_LOG_DIR)
  : join(REPO_ROOT, "logs");
const HOST = process.env.WEB_HOST ?? "127.0.0.1";
const PORT = Number.parseInt(process.env.WEB_PORT ?? "8080", 10);
// Defaults for the profiles the panel writes (POST /api/sims). WS_URL is the
// backend a new station connects to unless the request names one.
const DEFAULT_WS_URL = process.env.WS_URL ?? "ws://localhost:9000";
const ADMIN_PORT_BASE = Number.parseInt(
  process.env.SIM_ADMIN_PORT_BASE ?? "9901",
  10,
);
const INDEX_FILE = process.env.INDEX_FILE ?? "index_16.ts";
const RESTART_DELAY_MS = 3_000; // same pause as the shell loops
const KILL_GRACE_MS = 5_000; // SIGTERM -> SIGKILL

// --- Simulated transactions (randomized auto-cycling, overridable) --------
//
// Bypasses the Spark app layer: drives each sim's own admin API
// (POST /execute, GET /transactions) with private charging keys (idTags),
// same as python-services/orchestrator.py, but as an in-process scheduler
// so it's controllable from this same page. Don't also run orchestrator.py
// against the same simulators -- both would race for connectors/keys.
const KEYS_FILE = join(REPO_ROOT, "charging_keys.txt");
const TX_LOG_FILE = join(LOG_DIR, "transactions.csv");
// Every transactionId the scheduler has ever been handed, per sim, persisted so
// it survives panel restarts. See startConfirmed() for why.
const SEEN_TX_FILE = join(LOG_DIR, "seen_tx_ids.json");
const TX_MIN_MS =
  Number.parseFloat(process.env.TX_MIN_MINUTES ?? "15") * 60_000;
const TX_MAX_MS =
  Number.parseFloat(process.env.TX_MAX_MINUTES ?? "180") * 60_000;
const TX_GAP_MS = Number.parseFloat(process.env.TX_GAP_SECONDS ?? "30") * 1_000;
const TX_RETRY_MS =
  Number.parseFloat(process.env.TX_RETRY_BACKOFF_SECONDS ?? "10") * 1_000;
// A batch of auto sessions stops after this many complete; 0 = unlimited (default: run continuously).
// "Resume auto" starts a fresh batch if this is ever set > 0.
const TX_SESSION_LIMIT = Number.parseInt(
  process.env.TX_SESSION_LIMIT ?? "0",
  10,
);
// Dedicated fault-injection test chargers (comma-separated CP IDs). These are
// NOT auto-cycled by the in-process scheduler -- the scheduler leaves connector
// 1 alone so an external driver (the CSMS's RemoteStartTransaction) owns it
// -- and they are the ONLY sims that get the fault-injection controls in the
// panel (RemoteStart fail-mode + Faulted / High-temperature toggles). The live
// fleet stays clean. Default: none, so a fleet deployment names its test
// chargers in AUTO_EXCLUDE_CP_IDS. "*" makes every station a test charger,
// which is what a local dev stack wants: nothing auto-cycles and every station
// gets the fault controls.
const TEST_CHARGER_CP_IDS = new Set(
  (process.env.AUTO_EXCLUDE_CP_IDS ?? "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean),
);
const isTestCharger = (cpId: string) =>
  TEST_CHARGER_CP_IDS.has("*") || TEST_CHARGER_CP_IDS.has(cpId.toLowerCase());
const TX_CSV_FIELDS = [
  "started_at",
  "finished_at",
  "session_seconds",
  "sim",
  "connector_id",
  "id_tag",
  "label",
  "transaction_id",
  "kwh",
  "source",
  "status",
] as const;

interface Sim {
  id: string; // "sim1"
  cpId: string;
  wsUrl: string;
  adminPort: number;
  numConnectors: number; // physical plugs; from CONNECTORS in the profile (default 1)
  profile: string; // ".env.sim1"
  profilePath: string;
  logFile: string;
}

function parseEnvFile(path: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const raw of readFileSync(path, "utf8").split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq === -1) continue;
    out[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  }
  return out;
}

function simNumber(f: string): number {
  return Number.parseInt(f.replace(/^\.env\.sim/, ""), 10);
}

function simFromProfile(profile: string): Sim {
  const id = profile.replace(/^\.env\./, ""); // "sim1"
  const profilePath = join(PROFILES_DIR, profile);
  const env = parseEnvFile(profilePath);
  return {
    id,
    cpId: env.CP_ID ?? id,
    wsUrl: env.WS_URL ?? "",
    adminPort: Number.parseInt(env.ADMIN_PORT ?? "0", 10),
    numConnectors: Math.max(1, Number.parseInt(env.CONNECTORS ?? "1", 10) || 1),
    profile,
    profilePath,
    logFile: join(LOG_DIR, `${id}.log`),
  };
}

function loadSims(): Sim[] {
  if (!existsSync(PROFILES_DIR)) return [];
  return readdirSync(PROFILES_DIR)
    .filter((f) => /^\.env\.sim\d+$/.test(f))
    .sort((a, b) => simNumber(a) - simNumber(b))
    .map(simFromProfile);
}

// Physical connector (plug) ids for a sim: [1] for a single-plug charger,
// [1, 2] for a two-plug charger, etc.
function connectorIds(sim: Sim): number[] {
  return Array.from({ length: sim.numConnectors }, (_, i) => i + 1);
}

// Read the last `maxBytes` of a file and return the last `lines` lines.
function tailFile(path: string, lines = 200, maxBytes = 131072): string {
  if (!existsSync(path)) return "";
  const fd = openSync(path, "r");
  try {
    const size = fstatSync(fd).size;
    const start = Math.max(0, size - maxBytes);
    const len = size - start;
    const buf = Buffer.alloc(len);
    readSync(fd, buf, 0, len, start);
    const text = buf.toString("utf8");
    return text.split("\n").slice(-lines).join("\n");
  } finally {
    closeSync(fd);
  }
}

// The sim's GET /health: up/down, plus the delay clocks and meter state it
// reports (a sim built before those existed answers plain "OK", which still
// counts as up).
interface HealthState {
  up: boolean;
  connected: boolean | null;
  // The station's manual-offline flag (admin /disconnect); null from a sim
  // built before it existed.
  offline: boolean | null;
  delays: { replyMs: number; actMs: number } | null;
  unplugMode: "auto" | "manual" | null;
  meter: { auto: boolean; intervalMs: number; kw: number | null } | null;
  // When the socket opened and the CSMS last answered a Heartbeat/Boot, and
  // every connector's last status, battery and live reading (null from a sim
  // built before they existed).
  connectedSince: string | null;
  lastHeartbeatAt: string | null;
  // biome-ignore lint/suspicious/noExplicitAny: shape comes from the sim's /health
  connectors: any[] | null;
}

async function checkHealth(port: number): Promise<HealthState> {
  const down: HealthState = {
    up: false,
    connected: null,
    offline: null,
    delays: null,
    unplugMode: null,
    meter: null,
    connectedSince: null,
    lastHeartbeatAt: null,
    connectors: null,
  };
  if (!port) return down;
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 1500);
  try {
    const res = await fetch(`http://127.0.0.1:${port}/health`, {
      signal: ctl.signal,
    });
    if (!res.ok) return down;
    const text = await res.text();
    try {
      const body = JSON.parse(text);
      return {
        up: true,
        connected: body.connected ?? null,
        offline: body.offline ?? null,
        delays: body.delays ?? null,
        unplugMode: body.unplugMode ?? null,
        meter: body.meter ?? null,
        connectedSince: body.connectedSince ?? null,
        lastHeartbeatAt: body.lastHeartbeatAt ?? null,
        connectors: body.connectors ?? null,
      };
    } catch {
      return { ...down, up: true };
    }
  } catch {
    return down;
  } finally {
    clearTimeout(t);
  }
}

// --- station processes ---------------------------------------------------
//
// A managed station is one whose process the panel spawned: the same
// node + tsx-loader invocation as run_one_sim.sh, AUTO_RESTART=true, stdout and
// stderr appended to its log, and a restart loop that relaunches it 3 s after
// any exit (a crash, or the station's own /restart and /ws-url, which exit the
// process for exactly this reason) until the station is removed.

interface ManagedStation {
  sim: Sim;
  child: ChildProcess | null;
  removed: boolean;
  restartTimer?: ReturnType<typeof setTimeout>;
}

const managed = new Map<string, ManagedStation>(); // sim.id -> station

function panelLog(sim: Sim, line: string): void {
  mkdirSync(LOG_DIR, { recursive: true });
  appendFileSync(
    sim.logFile,
    `[panel] ${new Date().toISOString()} ${sim.id} ${line}\n`,
  );
}

function spawnStation(station: ManagedStation): void {
  const { sim } = station;
  station.restartTimer = undefined;
  mkdirSync(LOG_DIR, { recursive: true });
  const logFd = openSync(sim.logFile, "a");
  // Node's --env-file never overrides a variable that is already in the
  // environment, so everything the profile defines is dropped from the
  // inherited one: the panel's own WS_URL / CONNECTORS / TOKEN are defaults for
  // NEW profiles, not overrides for existing ones (run_one_sim.sh unsets
  // WS_URL for the same reason).
  const env: NodeJS.ProcessEnv = { ...process.env, AUTO_RESTART: "true" };
  for (const key of Object.keys(parseEnvFile(sim.profilePath))) {
    delete env[key];
  }
  const child = spawn(
    process.execPath,
    [
      `--env-file=${sim.profilePath}`,
      "--require",
      join(REPO_ROOT, "node_modules/tsx/dist/preflight.cjs"),
      "--import",
      pathToFileURL(join(REPO_ROOT, "node_modules/tsx/dist/loader.mjs")).href,
      INDEX_FILE,
    ],
    { cwd: REPO_ROOT, env, stdio: ["ignore", logFd, logFd] },
  );
  closeSync(logFd); // the child holds its own descriptor
  station.child = child;
  panelLog(
    sim,
    `started ${sim.cpId} (pid ${child.pid}) -> ${sim.wsUrl}/${sim.cpId}, admin :${sim.adminPort}`,
  );
  child.on("error", (err) => panelLog(sim, `spawn failed: ${err}`));
  child.on("exit", (code, signal) => {
    station.child = null;
    const why = signal ?? `code ${code}`;
    if (station.removed) {
      panelLog(sim, `stopped (${why})`);
      return;
    }
    panelLog(sim, `exited (${why}), restarting in ${RESTART_DELAY_MS / 1000}s`);
    station.restartTimer = setTimeout(
      () => spawnStation(station),
      RESTART_DELAY_MS,
    );
  });
}

function startManaging(sim: Sim): ManagedStation {
  const station: ManagedStation = { sim, child: null, removed: false };
  managed.set(sim.id, station);
  spawnStation(station);
  return station;
}

// Stop the restart loop and the process: SIGTERM, SIGKILL if it is still
// there after KILL_GRACE_MS. Resolves once the process is gone.
async function stopStation(station: ManagedStation): Promise<void> {
  station.removed = true;
  if (station.restartTimer) clearTimeout(station.restartTimer);
  const child = station.child;
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((done) => {
    const hardKill = setTimeout(() => child.kill("SIGKILL"), KILL_GRACE_MS);
    child.once("exit", () => {
      clearTimeout(hardKill);
      done();
    });
    child.kill("SIGTERM");
  });
}

// At startup every profile already in PROFILES_DIR gets a process, which is
// how stations survive a container restart. A profile whose admin port already
// answers belongs to someone else's supervisor (run_simulators.sh on the same
// host) and is listed as unmanaged instead of being spawned twice.
async function adoptExistingProfiles(): Promise<void> {
  for (const sim of loadSims()) {
    const health = await checkHealth(sim.adminPort);
    if (health.up) {
      console.log(
        `[sims] ${sim.id} (${sim.cpId}) already answers on :${sim.adminPort}; not managed by the panel`,
      );
      continue;
    }
    startManaging(sim);
    console.log(
      `[sims] ${sim.id} (${sim.cpId}) started from ${sim.profile}, admin :${sim.adminPort}`,
    );
  }
}

function stopAllStations(signal: NodeJS.Signals): void {
  for (const station of managed.values()) {
    station.removed = true;
    if (station.restartTimer) clearTimeout(station.restartTimer);
    station.child?.kill("SIGTERM");
  }
  console.log(`[sims] ${signal}: stopped ${managed.size} managed station(s)`);
  process.exit(0);
}

// Something as plain as a serial number: no whitespace, no path separators.
const CP_ID_RE = /^[^\s/\\]+$/;

function lowestFree(used: Set<number>, from: number): number {
  let n = from;
  while (used.has(n)) n++;
  return n;
}

// A port no profile claims may still be held by a process outside PROFILES_DIR
// (another container's published ports, a stray station), so bind-probe it.
function portIsFree(port: number): Promise<boolean> {
  return new Promise((done) => {
    const probe = createServer();
    probe.once("error", () => done(false));
    probe.listen(port, "127.0.0.1", () => probe.close(() => done(true)));
  });
}

// Write .env.sim<N> for a new station: N the lowest free suffix, ADMIN_PORT the
// lowest free port from ADMIN_PORT_BASE, the remaining keys from the panel's
// environment with the same defaults the Spark dev container used to write.
async function writeProfile(
  cpId: string,
  connectors: number,
  wsUrl: string,
): Promise<Sim> {
  const sims = loadSims();
  const usedNumbers = new Set(sims.map((s) => simNumber(s.profile)));
  const usedPorts = new Set(sims.map((s) => s.adminPort));
  const n = lowestFree(usedNumbers, 1);
  let port = lowestFree(usedPorts, ADMIN_PORT_BASE);
  while (!(await portIsFree(port))) port = lowestFree(usedPorts, port + 1);
  const lines = [
    `CP_ID=${cpId}`,
    `WS_URL=${wsUrl}`,
    `ADMIN_PORT=${port}`,
    `CONNECTORS=${connectors}`,
    `TOKEN=${process.env.TOKEN ?? "SIMTAG1"}`,
    "CONTINUE_ON_UNKNOWN_MESSAGE_ID=true",
    `METER_PHASE_SAMPLES=${process.env.METER_PHASE_SAMPLES ?? "true"}`,
    `ACT_DELAY_MS=${process.env.ACT_DELAY_MS ?? "1000"}`,
    `REPLY_DELAY_MS=${process.env.REPLY_DELAY_MS ?? "0"}`,
    `AUTHORIZE_REMOTE_TX_REQUESTS=${process.env.AUTHORIZE_REMOTE_TX_REQUESTS ?? "false"}`,
  ];
  const profile = `.env.sim${n}`;
  mkdirSync(PROFILES_DIR, { recursive: true });
  // "wx": two simultaneous adds racing for the same N fail loudly, never clobber.
  writeFileSync(join(PROFILES_DIR, profile), `${lines.join("\n")}\n`, {
    flag: "wx",
  });
  return simFromProfile(profile);
}

// The static part of a station's listing entry (no admin-port round trip).
function simSummary(sim: Sim) {
  const station = managed.get(sim.id);
  return {
    id: sim.id,
    cpId: sim.cpId,
    wsUrl: sim.wsUrl,
    adminPort: sim.adminPort,
    numConnectors: sim.numConnectors,
    managed: station !== undefined,
    pid: station?.child?.pid ?? null,
    profile: sim.profile,
    log: sim.logFile,
  };
}

// --- charging key pool -------------------------------------------------

interface ChargingKey {
  idTag: string;
  label: string;
}

let allKeys: ChargingKey[] = [];
let freeQueue: string[] = [];
const inUseKeys = new Set<string>();

function parseKeysText(text: string): ChargingKey[] {
  const keys: ChargingKey[] = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const [idTag, label] = line.split(",", 2).map((s) => s.trim());
    if (idTag) keys.push({ idTag, label: label || idTag });
  }
  return keys;
}

function reloadKeys(): void {
  const text = existsSync(KEYS_FILE) ? readFileSync(KEYS_FILE, "utf8") : "";
  allKeys = parseKeysText(text);
  freeQueue = allKeys
    .map((k) => k.idTag)
    .filter((idTag) => !inUseKeys.has(idTag));
}

function keyLabel(idTag: string): string {
  return allKeys.find((k) => k.idTag === idTag)?.label ?? idTag;
}

function acquireKey(): string | undefined {
  const idTag = freeQueue.shift();
  if (idTag) inUseKeys.add(idTag);
  return idTag;
}

function releaseKey(idTag: string): void {
  inUseKeys.delete(idTag);
  if (allKeys.some((k) => k.idTag === idTag)) freeQueue.push(idTag);
}

// --- transaction CSV log -------------------------------------------------

function ensureTxLog(): void {
  mkdirSync(LOG_DIR, { recursive: true });
  if (!existsSync(TX_LOG_FILE)) {
    writeFileSync(
      TX_LOG_FILE,
      `${TX_CSV_FIELDS.map((f) => `"${f}"`).join(",")}\n`,
    );
  }
}

function appendTxLog(
  row: Record<(typeof TX_CSV_FIELDS)[number], string | number>,
): void {
  ensureTxLog();
  const line = `${TX_CSV_FIELDS.map((f) => `"${String(row[f] ?? "").replace(/"/g, '""')}"`).join(",")}\n`;
  appendFileSync(TX_LOG_FILE, line);
}

function parseCsvLine(line: string): string[] {
  const fields: string[] = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else inQuotes = false;
      } else cur += ch;
    } else if (ch === '"') inQuotes = true;
    else if (ch === ",") {
      fields.push(cur);
      cur = "";
    } else cur += ch;
  }
  fields.push(cur);
  return fields;
}

function readTxLog(lines: number): Record<string, string>[] {
  if (!existsSync(TX_LOG_FILE)) return [];
  const text = tailFile(TX_LOG_FILE, lines + 1);
  return text
    .split("\n")
    .filter((l) => l && !l.startsWith('"started_at"'))
    .map((line) => {
      const cols = parseCsvLine(line);
      const row: Record<string, string> = {};
      TX_CSV_FIELDS.forEach((f, i) => {
        row[f] = cols[i] ?? "";
      });
      return row;
    })
    .reverse();
}

function simulateKwh(sessionSeconds: number): number {
  const rateKw = 3 + Math.random() * 8; // 3-11 kW, matches python-services/orchestrator.py
  return Math.round(rateKw * (sessionSeconds / 3600) * 1000) / 1000;
}

async function adminExecute(
  sim: Sim,
  action: string,
  payload: unknown,
): Promise<boolean> {
  try {
    const res = await fetch(`http://127.0.0.1:${sim.adminPort}/execute`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, payload }),
    });
    return res.ok;
  } catch {
    return false;
  }
}

// biome-ignore lint/suspicious/noExplicitAny: shape comes from the sim's own /transactions endpoint
async function adminTransactions(sim: Sim): Promise<any[]> {
  try {
    const res = await fetch(`http://127.0.0.1:${sim.adminPort}/transactions`);
    if (!res.ok) return [];
    return await res.json();
  } catch {
    return [];
  }
}

// Reply the CSMS sent to one specific OCPP call (via the sim's /execute-sync),
// or "unreachable" if the sim's admin API itself didn't answer.
type CallOutcome =
  // biome-ignore lint/suspicious/noExplicitAny: ocpp payload
  | { status: "result"; payload: any }
  | { status: "error"; errorCode: string; errorDescription: string }
  | { status: "timeout" | "not_sent" | "unreachable" };

async function adminExecuteSync(
  sim: Sim,
  action: string,
  payload: unknown,
  timeoutMs = 15_000,
): Promise<CallOutcome> {
  try {
    const res = await fetch(`http://127.0.0.1:${sim.adminPort}/execute-sync`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, payload, timeoutMs }),
    });
    return (await res.json()) as CallOutcome;
  } catch {
    return { status: "unreachable" };
  }
}

// Stop a transaction and report whether the CSMS actually acked the stop.
// (The sim stops metering locally the moment it sends StopTransaction, so an
// unacked stop can no longer keep a session alive -- but it must be visible.)
async function stopConfirmed(
  sim: Sim,
  transactionId: number,
  meterStop: number,
  reason = "Local",
): Promise<boolean> {
  const outcome = await adminExecuteSync(sim, "StopTransaction", {
    transactionId,
    meterStop,
    timestamp: new Date().toISOString(),
    reason,
  });
  if (outcome.status !== "result") {
    console.warn(
      `[tx] ${sim.id} StopTransaction ${transactionId} NOT acked by CSMS (${outcome.status})`,
    );
    return false;
  }
  return true;
}

// Close any transaction still open on this connector that the scheduler does
// not own (left over from an unacked stop, a web-panel restart, ...). A
// connector carries one session at a time, so these must go before a new start.
async function closeStaleTransactions(
  sim: Sim,
  connectorId: number,
): Promise<void> {
  for (const t of await adminTransactions(sim)) {
    if (t.connectorId !== connectorId) continue;
    console.warn(
      `[tx] ${sim.id} connector ${connectorId}: closing stale transaction ${t.transactionId} before new start`,
    );
    const acked = await stopConfirmed(
      sim,
      t.transactionId,
      Math.round(t.meterWh ?? 0),
      "Other",
    );
    appendTxLog({
      started_at: t.startedAt,
      finished_at: new Date().toISOString(),
      session_seconds: Math.round(
        (Date.now() - Date.parse(t.startedAt)) / 1000,
      ),
      sim: sim.id,
      connector_id: connectorId,
      id_tag: t.idTag,
      label: keyLabel(t.idTag),
      transaction_id: t.transactionId,
      kwh: 0,
      source: "auto",
      status: acked ? "stale_tx_closed" : "stale_tx_closed+stop_unconfirmed",
    });
  }
}

const SEEN_TX_CAP = 500; // per sim; plenty to cover any realistic reuse window
function loadSeenTxIds(): Record<string, number[]> {
  try {
    return JSON.parse(readFileSync(SEEN_TX_FILE, "utf8"));
  } catch {
    return {};
  }
}
const seenTxIds = loadSeenTxIds();
function markTxSeen(sim: Sim, transactionId: number): void {
  seenTxIds[sim.id] ??= [];
  const ids = seenTxIds[sim.id];
  ids.push(transactionId);
  if (ids.length > SEEN_TX_CAP) ids.splice(0, ids.length - SEEN_TX_CAP);
  try {
    writeFileSync(SEEN_TX_FILE, JSON.stringify(seenTxIds));
  } catch (err) {
    console.warn(`[tx] could not persist ${SEEN_TX_FILE}: ${err}`);
  }
}

// Start a transaction and learn its id from the CSMS's own StartTransaction.conf
// for THIS call (matched by OCPP messageId) -- never by searching open sessions,
// which on 2026-09-28 latched onto a stale session for days. Then confirm the
// sim is metering exactly that transactionId on this connector.
async function startConfirmed(
  sim: Sim,
  connectorId: number,
  idTag: string,
  startedAt: number,
): Promise<
  { ok: true; transactionId: number } | { ok: false; reason: string }
> {
  const outcome = await adminExecuteSync(sim, "StartTransaction", {
    connectorId,
    idTag,
    meterStart: 0,
    timestamp: new Date(startedAt).toISOString(),
  });
  if (outcome.status === "error")
    return { ok: false, reason: `start_rejected_${outcome.errorCode}` };
  if (outcome.status !== "result") {
    return {
      ok: false,
      reason:
        outcome.status === "timeout"
          ? "start_unconfirmed"
          : "start_request_failed",
    };
  }
  const transactionId = outcome.payload?.transactionId;
  if (typeof transactionId !== "number")
    return { ok: false, reason: "start_no_transaction_id" };
  // Non-Accepted idTag: the sim itself sends StopTransaction(DeAuthorized).
  if (outcome.payload?.idTagInfo?.status !== "Accepted")
    return { ok: false, reason: "not_authorized" };
  // The CSMS may answer a StartTransaction on a connector that
  // still has an open session with THAT session's id instead of a new one --
  // e.g. after a StopTransaction it never processed. Accepting it silently
  // "continues" an old session (meter reset to 0, days-long sessions). A
  // transactionId we've already been handed is never a new session: close it
  // and fail this attempt, so the retry gets a genuinely fresh transaction.
  if (seenTxIds[sim.id]?.includes(transactionId)) {
    console.warn(
      `[tx] ${sim.id} connector ${connectorId}: CSMS returned already-used transactionId ${transactionId} -- closing it and retrying`,
    );
    await stopConfirmed(sim, transactionId, 0, "Other");
    return { ok: false, reason: "csms_reused_transaction_id" };
  }
  markTxSeen(sim, transactionId);
  const deadline = Date.now() + 3_000;
  while (Date.now() < deadline) {
    const txns = await adminTransactions(sim);
    if (
      txns.some(
        (t) =>
          t.transactionId === transactionId && t.connectorId === connectorId,
      )
    ) {
      return { ok: true, transactionId };
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  await stopConfirmed(sim, transactionId, 0, "Other");
  return { ok: false, reason: "start_not_registered" };
}

// --- per-connector scheduler ----------------------------------------------

interface TxState {
  transactionId: number;
  idTag: string;
  label: string;
  startedAt: number;
  source: "auto" | "manual";
}

interface ConnectorState {
  sim: Sim;
  connectorId: number;
  mode: "auto" | "manual";
  status: "Available" | "Charging" | "Finishing";
  tx?: TxState;
  timer?: ReturnType<typeof setTimeout>;
  sessionCount: number; // auto sessions completed in the current batch
  pauseReason?: "user" | "batch_complete"; // only meaningful while mode === "manual" and idle
  consecutiveFailures: number; // resets to 0 on any successful start
}

// After this many consecutive failed start attempts, back off the retry
// interval (instead of hammering a dead admin API every TX_RETRY_MS forever)
// and flag it clearly so a downed simulator doesn't fail silently for hours.
const FAILURE_ALERT_THRESHOLD = 5;
const FAILURE_BACKOFF_MS = 120_000;

const connectorStates = new Map<string, ConnectorState>();
const stateKey = (simId: string, connectorId: number) =>
  `${simId}:${connectorId}`;

function clearTimer(state: ConnectorState): void {
  if (state.timer) {
    clearTimeout(state.timer);
    state.timer = undefined;
  }
}

async function beginTransaction(
  state: ConnectorState,
  idTag: string,
  source: "auto" | "manual",
): Promise<boolean> {
  const label = keyLabel(idTag);
  await closeStaleTransactions(state.sim, state.connectorId);
  const startedAt = Date.now();
  const started = await startConfirmed(
    state.sim,
    state.connectorId,
    idTag,
    startedAt,
  );
  if (started.ok) {
    if (state.consecutiveFailures >= FAILURE_ALERT_THRESHOLD) {
      console.warn(
        `[tx] ${state.sim.id} connector ${state.connectorId} recovered after ${state.consecutiveFailures} failed attempt(s)`,
      );
    }
    state.consecutiveFailures = 0;
    state.tx = {
      transactionId: started.transactionId,
      idTag,
      label,
      startedAt,
      source,
    };
    state.status = "Charging";
    await adminExecute(state.sim, "StatusNotification", {
      connectorId: state.connectorId,
      errorCode: "NoError",
      status: "Charging",
    });
    return true;
  }
  state.consecutiveFailures++;
  if (state.consecutiveFailures === FAILURE_ALERT_THRESHOLD) {
    console.warn(
      `[tx] ${state.sim.id} connector ${state.connectorId} has failed ${state.consecutiveFailures} start attempts in a row ` +
        `(last: ${started.reason}) -- backing off to ${FAILURE_BACKOFF_MS / 1000}s between retries`,
    );
  }
  releaseKey(idTag);
  appendTxLog({
    started_at: new Date(startedAt).toISOString(),
    finished_at: new Date().toISOString(),
    session_seconds: 0,
    sim: state.sim.id,
    connector_id: state.connectorId,
    id_tag: idTag,
    label,
    transaction_id: "",
    kwh: 0,
    source,
    status: started.reason,
  });
  return false;
}

function retryDelay(state: ConnectorState): number {
  return state.consecutiveFailures >= FAILURE_ALERT_THRESHOLD
    ? FAILURE_BACKOFF_MS
    : TX_RETRY_MS;
}

async function endTransaction(
  state: ConnectorState,
  status: string,
): Promise<void> {
  const tx = state.tx;
  if (!tx) return;
  const sessionSeconds = Math.round((Date.now() - tx.startedAt) / 1000);
  const kwh = simulateKwh(sessionSeconds);
  const acked = await stopConfirmed(
    state.sim,
    tx.transactionId,
    Math.round(kwh * 1000),
  );
  const finishedAt = new Date().toISOString();
  appendTxLog({
    started_at: new Date(tx.startedAt).toISOString(),
    finished_at: finishedAt,
    session_seconds: sessionSeconds,
    sim: state.sim.id,
    connector_id: state.connectorId,
    id_tag: tx.idTag,
    label: tx.label,
    transaction_id: tx.transactionId,
    kwh,
    source: tx.source,
    status: acked ? status : `${status}+stop_unconfirmed`,
  });
  releaseKey(tx.idTag);
  state.tx = undefined;
  // A session started from the panel follows the station's unplug mode: in
  // "manual" the cable stays in (Finishing) until Unplug. Auto-cycles always
  // free the connector, or the next auto start would land on a busy plug.
  const unplug =
    tx.source === "manual"
      ? await adminGet(state.sim.adminPort, "/unplug-mode")
      : null;
  const next = unplug?.mode === "manual" ? "Finishing" : "Available";
  state.status = next;
  await adminExecute(state.sim, "StatusNotification", {
    connectorId: state.connectorId,
    errorCode: "NoError",
    status: next,
  });
}

function scheduleAutoStart(state: ConnectorState, delayMs = 0): void {
  clearTimer(state);
  state.timer = setTimeout(async () => {
    if (state.mode !== "auto") return;
    const idTag = acquireKey();
    if (!idTag) {
      scheduleAutoStart(state, TX_RETRY_MS);
      return;
    }
    const ok = await beginTransaction(state, idTag, "auto");
    if (state.mode !== "auto") {
      // mode flipped to manual while the network calls above were in flight
      if (state.tx) await endTransaction(state, "mode_changed_during_start");
      return;
    }
    if (!ok) {
      scheduleAutoStart(state, retryDelay(state));
      return;
    }
    const durationMs = TX_MIN_MS + Math.random() * (TX_MAX_MS - TX_MIN_MS);
    state.timer = setTimeout(() => autoStop(state), durationMs);
  }, delayMs);
}

async function autoStop(state: ConnectorState): Promise<void> {
  if (state.mode !== "auto") return;
  await endTransaction(state, "auto_cycle_complete");
  state.sessionCount++;
  if (TX_SESSION_LIMIT > 0 && state.sessionCount >= TX_SESSION_LIMIT) {
    // batch finished; stays paused until resume-auto starts a fresh batch
    state.mode = "manual";
    state.pauseReason = "batch_complete";
    return;
  }
  if (state.mode === "auto") scheduleAutoStart(state, TX_GAP_MS);
}

async function manualStart(
  state: ConnectorState,
): Promise<{ ok: boolean; error?: string }> {
  clearTimer(state);
  if (state.tx) await endTransaction(state, "overridden_by_manual_start");
  state.mode = "manual";
  state.pauseReason = "user";
  const idTag = acquireKey();
  if (!idTag) return { ok: false, error: "no free charging keys available" };
  const ok = await beginTransaction(state, idTag, "manual");
  if (!ok)
    return {
      ok: false,
      error: "start rejected (not authorized) or admin API unreachable",
    };
  return { ok: true };
}

async function manualStop(state: ConnectorState): Promise<void> {
  clearTimer(state);
  if (state.tx) await endTransaction(state, "stopped_manually");
  state.mode = "manual"; // stays paused until resume-auto is called
  state.pauseReason = "user";
}

function resumeAuto(state: ConnectorState): void {
  clearTimer(state);
  state.mode = "auto";
  state.pauseReason = undefined;
  state.sessionCount = 0; // resume-auto always starts a fresh batch
  if (state.tx) {
    // hand the in-progress session over to the auto scheduler with a fresh random duration
    const remainingMs = TX_MIN_MS + Math.random() * (TX_MAX_MS - TX_MIN_MS);
    state.timer = setTimeout(() => autoStop(state), remainingMs);
  } else {
    scheduleAutoStart(state);
  }
}

// Every physical plug gets its own scheduler state (so it shows status and
// can be driven manually), but only connector 1 auto-cycles. Extra plugs on
// a multi-plug charger come up manual/idle so the concurrent auto-transaction
// count is unchanged -- they charge only when a Start is triggered.
function registerConnectorStates(sim: Sim): void {
  const excluded = isTestCharger(sim.cpId);
  for (const connectorId of connectorIds(sim)) {
    const autoCycle = !excluded && connectorId === 1;
    const state: ConnectorState = {
      sim,
      connectorId,
      mode: autoCycle ? "auto" : "manual",
      status: "Available",
      sessionCount: 0,
      consecutiveFailures: 0,
      pauseReason: autoCycle ? undefined : "user",
    };
    connectorStates.set(stateKey(sim.id, connectorId), state);
    if (!autoCycle) {
      const why = excluded ? "test charger" : "secondary plug";
      console.log(
        `[tx] ${sim.id} (${sim.cpId}) connector ${connectorId} not auto-cycled (${why}, manual only)`,
      );
      continue; // externally / manually driven -- don't schedule an auto start
    }
    scheduleAutoStart(state, Math.random() * 5_000); // stagger startup across sims
  }
}

// Forget a removed station's plugs: no pending timer may fire for it and a key
// it held goes back to the pool (the process is gone, so there is nothing to
// stop over OCPP).
function dropConnectorStates(simId: string): void {
  for (const [key, state] of connectorStates) {
    if (state.sim.id !== simId) continue;
    clearTimer(state);
    if (state.tx) releaseKey(state.tx.idTag);
    connectorStates.delete(key);
  }
}

function initTransactionSim(): void {
  reloadKeys();
  ensureTxLog();
  for (const sim of loadSims()) registerConnectorStates(sim);
}

const app = new Hono();

// The dashboard page, read per request so an edit shows on reload.
const PAGE_FILE = join(dirname(fileURLToPath(import.meta.url)), "index.html");
app.get("/", (c) => c.html(readFileSync(PAGE_FILE, "utf8")));

// GET a small JSON doc from a sim's admin endpoint (fail-mode / fault state).
// biome-ignore lint/suspicious/noExplicitAny: shape comes from the sim's admin endpoint
async function adminGet(port: number, path: string): Promise<any | null> {
  try {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), 1500);
    const res = await fetch(`http://127.0.0.1:${port}${path}`, {
      signal: ctl.signal,
    });
    clearTimeout(t);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

app.get("/api/sims", async (c) => {
  const sims = loadSims();
  const withHealth = await Promise.all(
    sims.map(async (s) => {
      const health = await checkHealth(s.adminPort);
      const up = health.up;
      const testCharger = isTestCharger(s.cpId);
      // Live energy of any active session(s) -- one entry per busy connector,
      // with the transaction id so the per-connector Stop button can quote it.
      const txns = up ? await adminTransactions(s) : [];
      const charging = txns.map((t) => ({
        connectorId: t.connectorId ?? 1,
        transactionId: t.transactionId,
        idTag: t.idTag,
        meterWh: Math.round(t.meterWh ?? 0),
        kwh: Math.round(((t.meterWh ?? 0) / 1000) * 1000) / 1000,
      }));
      // Fault-injection state only matters for (and only exists on) test chargers.
      const [failMode, faults, chargingPower, chargeTarget, wsUrlState] =
        up && testCharger
          ? await Promise.all([
              adminGet(s.adminPort, "/fail-mode"),
              adminGet(s.adminPort, "/fault"),
              adminGet(s.adminPort, "/charging-power"),
              adminGet(s.adminPort, "/charge-target"),
              adminGet(s.adminPort, "/ws-url"),
            ])
          : [null, null, null, null, null];
      return {
        ...simSummary(s),
        up,
        connected: health.connected,
        offline: health.offline,
        delays: health.delays,
        unplugMode: health.unplugMode,
        meter: health.meter,
        connectedSince: health.connectedSince,
        lastHeartbeatAt: health.lastHeartbeatAt,
        connectors: health.connectors,
        isTestCharger: testCharger,
        charging,
        failMode,
        faults,
        chargingPower,
        chargeTarget,
        wsUrlState,
      };
    }),
  );
  return c.json({
    sims: withHealth,
    defaults: {
      wsUrl: DEFAULT_WS_URL,
      profilesDir: PROFILES_DIR,
      logDir: LOG_DIR,
      adminPortBase: ADMIN_PORT_BASE,
    },
  });
});

// Add a station: write its profile and run it under the panel's restart loop.
// Body: { cpId: string, connectors?: number (default 1), wsUrl?: string }.
// 201 with the station summary, 400 for a bad body, 409 for a duplicate id.
app.post("/api/sims", async (c) => {
  let body: { cpId?: unknown; connectors?: unknown; wsUrl?: unknown };
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "invalid JSON body" }, 400);
  }
  const cpId = typeof body.cpId === "string" ? body.cpId.trim() : "";
  if (!cpId || !CP_ID_RE.test(cpId)) {
    return c.json(
      {
        error: "cpId must be a non-empty string without whitespace or slashes",
      },
      400,
    );
  }
  const connectors =
    body.connectors === undefined || body.connectors === null
      ? 1
      : Number(body.connectors);
  if (!Number.isInteger(connectors) || connectors < 1 || connectors > 20) {
    return c.json({ error: "connectors must be an integer from 1 to 20" }, 400);
  }
  let wsUrl = DEFAULT_WS_URL;
  if (typeof body.wsUrl === "string" && body.wsUrl.trim() !== "") {
    wsUrl = body.wsUrl.trim().replace(/\/+$/, "");
    if (!/^wss?:\/\/\S+$/i.test(wsUrl)) {
      return c.json({ error: "wsUrl must start with ws:// or wss://" }, 400);
    }
  } else if (body.wsUrl !== undefined && body.wsUrl !== null) {
    return c.json({ error: "wsUrl must be a string" }, 400);
  }
  // Cosmos and its hub upper-case station ids, so two ids that differ only in
  // case would be one charge box to them.
  const taken = loadSims().find(
    (s) => s.cpId.toLowerCase() === cpId.toLowerCase(),
  );
  if (taken) {
    return c.json(
      { error: `${taken.cpId} already exists (${taken.profile})` },
      409,
    );
  }
  let sim: Sim;
  try {
    sim = await writeProfile(cpId, connectors, wsUrl);
  } catch (err) {
    return c.json({ error: `could not write the profile: ${err}` }, 500);
  }
  startManaging(sim);
  registerConnectorStates(sim);
  console.log(
    `[sims] added ${sim.id} (${sim.cpId}) -> ${sim.wsUrl}/${sim.cpId}, admin :${sim.adminPort}`,
  );
  return c.json(
    { sim: { ...simSummary(sim), up: false, connected: null } },
    201,
  );
});

// Remove a station the panel manages: stop the restart loop and the process,
// delete the profile, keep the log. :id is the sim id ("sim4") or the cpId.
// 404 for an unknown station, 409 for one a shell script runs.
app.delete("/api/sims/:id", async (c) => {
  const id = c.req.param("id");
  const sim = loadSims().find(
    (s) => s.id === id || s.cpId.toLowerCase() === id.toLowerCase(),
  );
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  const station = managed.get(sim.id);
  if (!station) {
    return c.json(
      {
        error: `${sim.cpId} is not managed by the panel: stop its supervisor and delete ${sim.profile} yourself`,
      },
      409,
    );
  }
  dropConnectorStates(sim.id);
  await stopStation(station);
  managed.delete(sim.id);
  try {
    unlinkSync(sim.profilePath);
  } catch (err) {
    return c.json(
      { error: `process stopped, profile not deleted: ${err}` },
      500,
    );
  }
  console.log(
    `[sims] removed ${sim.id} (${sim.cpId}); log kept at ${sim.logFile}`,
  );
  return c.json({ ok: true, removed: { ...simSummary(sim), managed: true } });
});

// Toggle a connector fault (faulted / high temperature) on a sim. Proxies to
// its admin POST /fault. Body: { connectorId?, type: faulted|high_temperature, on }
app.post("/api/sims/:id/fault", async (c) => {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  let body: unknown;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "invalid JSON body" }, 400);
  }
  try {
    const res = await fetch(`http://127.0.0.1:${sim.adminPort}/fault`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    return c.json({ ok: res.ok, status: res.status, response: text });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 502);
  }
});

// Set charging speed (kW) on a sim. Proxies to admin POST /charging-power.
// Body: { kw: number | null }  (null = restore legacy fixed rate)
app.post("/api/sims/:id/charging-power", async (c) => {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  let body: unknown;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "invalid JSON body" }, 400);
  }
  try {
    const res = await fetch(
      `http://127.0.0.1:${sim.adminPort}/charging-power`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      },
    );
    const text = await res.text();
    return c.json({ ok: res.ok, status: res.status, response: text });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 502);
  }
});

// Restart a sim's process. Proxies to admin POST /restart, which exits the
// process so its supervisor relaunches it with fresh .env + code: the panel's
// own restart loop for a managed station, the shell loop otherwise.
app.post("/api/sims/:id/restart", async (c) => {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  try {
    const res = await fetch(`http://127.0.0.1:${sim.adminPort}/restart`, {
      method: "POST",
    });
    const text = await res.text();
    return c.json({ ok: res.ok, status: res.status, response: text });
  } catch (err) {
    // The process may drop the connection as it exits before replying; that
    // still means the restart was initiated.
    return c.json({ ok: true, initiated: true, note: String(err) });
  }
});

// Repoint a test charger at a different OCPP backend URL. Proxies to admin
// POST /ws-url. Body: { url: string | null }  (null = reset to the .env WS_URL
// baseline). The sim persists the override then restarts to reconnect, so this
// is gated to test chargers to keep the live fleet on their configured host.
app.post("/api/sims/:id/ws-url", async (c) => {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  if (!isTestCharger(sim.cpId)) {
    return c.json(
      { error: "ws-url change is only allowed on test chargers" },
      403,
    );
  }
  let body: unknown;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "invalid JSON body" }, 400);
  }
  try {
    const res = await fetch(`http://127.0.0.1:${sim.adminPort}/ws-url`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    return c.json({ ok: res.ok, status: res.status, response: text });
  } catch (err) {
    // The process exits ~250ms after replying to reconnect; if the connection
    // drops before we read the reply, the change was still persisted.
    return c.json({ ok: true, initiated: true, note: String(err) });
  }
});

// Create a complete transaction with an EXACT energy amount (e.g. 0 kWh or
// 1 kWh) by starting and immediately stopping with a fixed meterStop. Restricted
// to test chargers so it can't disturb the live fleet. Body: { kwh: number }.
app.post("/api/sims/:id/quick-tx", async (c) => {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  if (!isTestCharger(sim.cpId)) {
    return c.json({ error: "quick-tx is only allowed on test chargers" }, 403);
  }
  let kwh = 0;
  try {
    const body = (await c.req.json()) as { kwh?: number };
    kwh = Number(body.kwh ?? 0);
  } catch {
    return c.json({ error: "invalid JSON body" }, 400);
  }
  if (!Number.isFinite(kwh) || kwh < 0) {
    return c.json({ error: "kwh must be a non-negative number" }, 400);
  }
  const connectorId = 1;
  // Refuse if a transaction is already open on the connector.
  const existing = await adminTransactions(sim);
  if (existing.some((t) => t.connectorId === connectorId)) {
    return c.json(
      {
        ok: false,
        error: "connector busy — stop the running transaction first",
      },
      409,
    );
  }
  const idTag = acquireKey();
  if (!idTag)
    return c.json({ ok: false, error: "no free charging keys available" }, 409);
  const startedAt = Date.now();
  const label = keyLabel(idTag);
  try {
    const started = await startConfirmed(sim, connectorId, idTag, startedAt);
    if (!started.ok) {
      return c.json(
        { ok: false, error: `StartTransaction failed: ${started.reason}` },
        502,
      );
    }
    const txn = { transactionId: started.transactionId };
    const meterStop = Math.round(kwh * 1000);
    const acked = await stopConfirmed(sim, txn.transactionId, meterStop);
    const finishedAt = new Date().toISOString();
    await adminExecute(sim, "StatusNotification", {
      connectorId,
      errorCode: "NoError",
      status: "Available",
    });
    appendTxLog({
      started_at: new Date(startedAt).toISOString(),
      finished_at: finishedAt,
      session_seconds: Math.round((Date.now() - startedAt) / 1000),
      sim: sim.id,
      connector_id: connectorId,
      id_tag: idTag,
      label,
      transaction_id: txn.transactionId,
      kwh,
      source: "manual",
      status: acked
        ? `quick_tx_${kwh}kwh`
        : `quick_tx_${kwh}kwh+stop_unconfirmed`,
    });
    return c.json({
      ok: acked,
      transactionId: txn.transactionId,
      kwh,
      meterStop,
      stopAcked: acked,
    });
  } finally {
    releaseKey(idTag);
  }
});

// Arm/disarm an auto-stop energy target on a sim. The charger stops ITSELF at
// exactly this energy on whatever session is active -- including one the mobile
// app started via RemoteStart -- forcing meterStop to the target so it lands on
// e.g. 1.000 kWh, never 1.05. Optionally also sets the ramp rate/cadence.
// Test chargers only. Body: { kwh: number|null, kw?: number, intervalMs?: number }
app.post("/api/sims/:id/charge-target", async (c) => {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  if (!isTestCharger(sim.cpId)) {
    return c.json(
      { error: "charge-target is only allowed on test chargers" },
      403,
    );
  }
  let body: unknown;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "invalid JSON body" }, 400);
  }
  try {
    const res = await fetch(`http://127.0.0.1:${sim.adminPort}/charge-target`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    return c.json({ ok: res.ok, status: res.status, response: text });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 502);
  }
});

// Toggle RemoteStartTransaction fault injection on a sim (proxies to its admin
// POST /fail-mode). Body: { mode: off|ignore|reject|accept_no_start, durationMs? }
app.post("/api/sims/:id/fail-mode", async (c) => {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  let body: unknown;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "invalid JSON body" }, 400);
  }
  try {
    const res = await fetch(`http://127.0.0.1:${sim.adminPort}/fail-mode`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    return c.json({ ok: res.ok, status: res.status, response: text });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 502);
  }
});

app.get("/api/sims/:id/logs", (c) => {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  const lines = Number.parseInt(c.req.query("lines") ?? "200", 10);
  // The station logs in this host's local time without an offset; the page
  // needs the offset to line those lines up with the frames' UTC stamps.
  return c.json({
    log: tailFile(sim.logFile, lines),
    tzOffsetMinutes: new Date().getTimezoneOffset(),
  });
});

app.post("/api/sims/:id/execute", async (c) => {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  let body: { action?: string; payload?: unknown };
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "invalid JSON body" }, 400);
  }
  if (!body.action) return c.json({ error: "missing 'action'" }, 400);
  try {
    const res = await fetch(`http://127.0.0.1:${sim.adminPort}/execute`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: body.action,
        payload: body.payload ?? {},
      }),
    });
    const text = await res.text();
    return c.json({ ok: res.ok, status: res.status, response: text });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 502);
  }
});

// Proxy a JSON POST to one of the sim's admin endpoints and hand its answer
// back as-is (status + body), so the page can show what the sim said.
async function proxyAdminPost(c: Context, path: string, bodyRequired = true) {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  let body: unknown = {};
  if (bodyRequired) {
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: "invalid JSON body" }, 400);
    }
  }
  try {
    const res = await fetch(`http://127.0.0.1:${sim.adminPort}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    return c.json({ ok: res.ok, status: res.status, response: text });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 502);
  }
}

// Take the station offline and bring it back without restarting its process.
// POST /disconnect closes the OCPP socket and holds the station's auto-restart
// off, so it stays offline until /connect (socket reopened, fresh
// BootNotification) or /restart. No body on either.
app.post("/api/sims/:id/disconnect", (c) =>
  proxyAdminPost(c, "/disconnect", false),
);
app.post("/api/sims/:id/connect", (c) => proxyAdminPost(c, "/connect", false));
// The two delay clocks. Body: { replyMs?: number, actMs?: number } (ms).
app.post("/api/sims/:id/delays", (c) => proxyAdminPost(c, "/delays"));
// What a connector does after a stop. Body: { mode: "auto" | "manual" }
app.post("/api/sims/:id/unplug-mode", (c) => proxyAdminPost(c, "/unplug-mode"));
// Periodic MeterValues on/off and cadence. Body: { auto?: boolean, intervalSeconds?: number }
app.post("/api/sims/:id/meter", (c) => proxyAdminPost(c, "/meter"));
// One MeterValues now. Body: { connectorId: number }
app.post("/api/sims/:id/meter-tick", (c) => proxyAdminPost(c, "/meter-tick"));
// Simulate an EV battery on a connector: SoC in MeterValues, SuspendedEV at
// 100 %. Body: { connectorId, enabled, batteryKwh?, startPercent? }
app.post("/api/sims/:id/soc", (c) => proxyAdminPost(c, "/soc"));
// Driver-side connector actions with the status reports that follow. Body:
// { connectorId, action: plug_in|authorize|start|stop|suspend|resume|unplug,
//   idTag?, reason? }
app.post("/api/sims/:id/connector-action", (c) =>
  proxyAdminPost(c, "/connector-action"),
);

// The sim's last OCPP frames both ways and the CSMS's last reply to one of
// its calls, from the sim's own ring buffer (no log file needed).
app.get("/api/sims/:id/frames", async (c) => {
  const sim = loadSims().find((s) => s.id === c.req.param("id"));
  if (!sim) return c.json({ error: "unknown sim" }, 404);
  const limit = Number.parseInt(c.req.query("limit") ?? "200", 10);
  const data = await adminGet(sim.adminPort, `/frames?limit=${limit}`);
  return c.json(data ?? { frames: [], lastReply: null });
});

app.get("/api/tx/keys", (c) => {
  const text = existsSync(KEYS_FILE) ? readFileSync(KEYS_FILE, "utf8") : "";
  return c.json({
    text,
    keys: allKeys,
    free: freeQueue.length,
    total: allKeys.length,
  });
});

app.post("/api/tx/keys", async (c) => {
  let body: { text?: string };
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "invalid JSON body" }, 400);
  }
  if (typeof body.text !== "string")
    return c.json({ error: "missing 'text'" }, 400);
  writeFileSync(KEYS_FILE, body.text);
  reloadKeys();
  return c.json({ ok: true, keys: allKeys });
});

app.get("/api/tx/state", (c) => {
  const states = Array.from(connectorStates.values()).map((s) => ({
    simId: s.sim.id,
    cpId: s.sim.cpId,
    connectorId: s.connectorId,
    mode: s.mode,
    status: s.status,
    sessionCount: s.sessionCount,
    pauseReason: s.pauseReason ?? null,
    consecutiveFailures: s.consecutiveFailures,
    tx: s.tx
      ? {
          transactionId: s.tx.transactionId,
          idTag: s.tx.idTag,
          label: s.tx.label,
          source: s.tx.source,
          elapsedSeconds: Math.round((Date.now() - s.tx.startedAt) / 1000),
        }
      : null,
  }));
  return c.json({
    states,
    freeKeys: freeQueue.length,
    totalKeys: allKeys.length,
    txSettings: {
      minMinutes: TX_MIN_MS / 60_000,
      maxMinutes: TX_MAX_MS / 60_000,
      gapSeconds: TX_GAP_MS / 1_000,
      sessionLimit: TX_SESSION_LIMIT,
      failureAlertThreshold: FAILURE_ALERT_THRESHOLD,
    },
  });
});

app.post("/api/tx/:simId/:connectorId/start", async (c) => {
  const state = connectorStates.get(
    stateKey(
      c.req.param("simId"),
      Number.parseInt(c.req.param("connectorId"), 10),
    ),
  );
  if (!state) return c.json({ error: "unknown connector" }, 404);
  return c.json(await manualStart(state));
});

app.post("/api/tx/:simId/:connectorId/stop", async (c) => {
  const state = connectorStates.get(
    stateKey(
      c.req.param("simId"),
      Number.parseInt(c.req.param("connectorId"), 10),
    ),
  );
  if (!state) return c.json({ error: "unknown connector" }, 404);
  await manualStop(state);
  return c.json({ ok: true });
});

app.post("/api/tx/:simId/:connectorId/resume-auto", (c) => {
  const state = connectorStates.get(
    stateKey(
      c.req.param("simId"),
      Number.parseInt(c.req.param("connectorId"), 10),
    ),
  );
  if (!state) return c.json({ error: "unknown connector" }, 404);
  resumeAuto(state);
  return c.json({ ok: true });
});

app.get("/api/tx/log", (c) => {
  const lines = Number.parseInt(c.req.query("lines") ?? "150", 10);
  return c.json({ rows: readTxLog(lines) });
});

initTransactionSim();
adoptExistingProfiles().catch((err) =>
  console.error(`[sims] could not start the existing profiles: ${err}`),
);
process.once("SIGINT", stopAllStations);
process.once("SIGTERM", stopAllStations);

serve({ fetch: app.fetch, hostname: HOST, port: PORT }, (info) => {
  console.log(
    `OCPP control panel on http://${HOST}:${info.port} (profiles: ${PROFILES_DIR}, logs: ${LOG_DIR})`,
  );
});
