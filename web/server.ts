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
  meter: { auto: boolean; intervalMs: number; kw: number | null } | null;
}

async function checkHealth(port: number): Promise<HealthState> {
  const down: HealthState = {
    up: false,
    connected: null,
    offline: null,
    delays: null,
    meter: null,
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
        meter: body.meter ?? null,
      };
    } catch {
      return {
        up: true,
        connected: null,
        offline: null,
        delays: null,
        meter: null,
      };
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
  status: "Available" | "Charging";
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
  state.status = "Available";
  await adminExecute(state.sim, "StatusNotification", {
    connectorId: state.connectorId,
    errorCode: "NoError",
    status: "Available",
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

app.get("/", (c) => c.html(PAGE));

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
        meter: health.meter,
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
  return c.json({ log: tailFile(sim.logFile, lines) });
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
// Periodic MeterValues on/off and cadence. Body: { auto?: boolean, intervalSeconds?: number }
app.post("/api/sims/:id/meter", (c) => proxyAdminPost(c, "/meter"));
// One MeterValues now. Body: { connectorId: number }
app.post("/api/sims/:id/meter-tick", (c) => proxyAdminPost(c, "/meter-tick"));

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

const PAGE = /* html */ `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>OCPP Simulator Control</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.5 -apple-system, Segoe UI, Roboto, sans-serif;
         background: #0f1115; color: #e6e6e6; }
  header { padding: 12px 20px; border-bottom: 1px solid #23262d;
           display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 16px; margin: 0; font-weight: 600; }
  .layout { display: grid; grid-template-columns: 260px 1fr; min-height: calc(100vh - 50px); }
  aside { border-right: 1px solid #23262d; background: #12151b; padding: 12px; min-width: 0; }
  main.content { padding: 20px; min-width: 0; display: grid; gap: 16px; align-content: start; }
  .navtitle { font-size: 11px; text-transform: uppercase; letter-spacing: .04em; color: #8b93a1;
              font-weight: 600; margin: 14px 0 6px; }
  ul.nav { list-style: none; margin: 0; padding: 0; }
  ul.nav li { display: flex; align-items: center; gap: 6px; padding: 6px 8px; border-radius: 6px;
              cursor: pointer; font-size: 13px; }
  ul.nav li:hover { background: #1c2029; }
  ul.nav li.selected { background: #23506f; color: #fff; }
  ul.nav li .navid { flex: 1; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  ul.nav li .navstate { font-size: 11px; color: #8b93a1; }
  ul.nav li.selected .navstate { color: #cfe3f3; }
  .navempty { color: #8b93a1; font-size: 12px; padding: 4px 8px; }
  .addform { display: grid; gap: 6px; }
  .addform label { display: grid; gap: 4px; font-size: 11px; color: #8b93a1; font-weight: 600; }
  .addform button { justify-self: start; }
  .card { background: #171a21; border: 1px solid #23262d; border-radius: 10px; overflow: hidden; }
  .card h2 { font-size: 14px; margin: 0; padding: 12px 14px; border-bottom: 1px solid #23262d;
             display: flex; align-items: center; gap: 8px; }
  .card h2 button { padding: 3px 8px; font-size: 11px; }
  .card .sub { color: #8b93a1; font-size: 12px; font-weight: 400; margin-left: auto; }
  .card .body { padding: 12px 14px; }
  .dot { width: 9px; height: 9px; border-radius: 50%; background: #555; flex: none; }
  .dot.up { background: #35c46a; box-shadow: 0 0 6px #35c46a88; }
  .dot.down { background: #e0533d; }
  .dim { color: #8b93a1; font-size: 12px; margin-bottom: 10px; }
  .badge { font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: 600; letter-spacing: .02em; white-space: nowrap; }
  .badge-charging, .badge-on { background: #35c46a33; color: #35c46a; }
  .badge-available { background: #8b93a133; color: #8b93a1; }
  .badge-off { background: #e0533d33; color: #e0533d; }
  .badge-warn { background: #e0a83d33; color: #e0a83d; }
  .badge-info { background: #6fb3e033; color: #6fb3e0; }
  .alert { color: #e0533d; font-size: 11px; margin-bottom: 10px; font-weight: 600; }
  table.txlog { width: 100%; border-collapse: collapse; font-size: 11px; }
  table.txlog th, table.txlog td { text-align: left; padding: 4px 6px; border-bottom: 1px solid #23262d; white-space: nowrap; vertical-align: top; }
  table.txlog th { color: #8b93a1; font-weight: 500; position: sticky; top: 0; background: #171a21; }
  table.txlog td button { padding: 2px 6px; font-size: 10px; }
  .link { color: #6fb3e0; cursor: pointer; font-weight: 600; }
  .link:hover { text-decoration: underline; }
  .logscroll { overflow: auto; max-height: 320px; }
  .empty { color: #8b93a1; padding: 40px 0; text-align: center; }
  .stationhead { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
  .stationhead h2 { font-size: 18px; margin: 0; font-weight: 600; }
  .stationhead .meta { color: #8b93a1; font-size: 12px; }
  .stationhead .spacer { flex: 1; }
  .restart-btn { padding: 3px 8px; font-size: 11px; background: #3a2b2b; border-color: #5a3a3a; flex: none; }
  .restart-btn:hover { background: #4a3535; }
  .remove-btn { padding: 3px 8px; font-size: 11px; background: #3a2b2b; border-color: #5a3a3a; flex: none; }
  .remove-btn:hover { background: #7a2f2f; border-color: #e0533d; }
  .wsurl { flex-basis: 100%; color: #8b93a1; font-size: 11px; word-break: break-all; }
  details.card summary { cursor: pointer; font-size: 14px; font-weight: 600; padding: 12px 14px;
                         border-bottom: 1px solid #23262d; list-style: none; display: flex; align-items: center; gap: 8px; }
  details.card summary::-webkit-details-marker { display: none; }
  details.card summary::before { content: "▾"; color: #8b93a1; font-size: 12px; }
  details.card:not([open]) summary::before { content: "▸"; }
  details.card:not([open]) summary { border-bottom: none; }
  .setgroup { padding: 10px 14px 0; border-bottom: 1px solid #23262d; }
  .setgroup:last-child { border-bottom: none; }
  .setlabel { font-size: 11px; color: #8b93a1; font-weight: 600; text-transform: uppercase; letter-spacing: .04em; margin-bottom: 8px; }
  .row { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; align-items: center; }
  button { background: #262b34; color: #e6e6e6; border: 1px solid #333a45; border-radius: 6px;
           padding: 6px 10px; font-size: 12px; cursor: pointer; }
  button:hover { background: #2f3540; }
  button.primary { background: #2f6f4f; border-color: #35c46a55; }
  button.primary:hover { background: #367a58; }
  .failbox { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin-bottom: 10px;
             padding: 8px; border: 1px solid #3a2b2b; background: #1c1516; border-radius: 6px; }
  .failbox .faillabel { font-size: 11px; color: #e0a83d; font-weight: 600; }
  .failbox button { background: #3a2b2b; border-color: #5a3a3a; }
  .failbox button:hover { background: #4a3535; }
  .failstate { font-size: 11px; color: #8b93a1; margin-left: auto; }
  .failstate.active { color: #e0533d; font-weight: 600; }
  .faultbox { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin-bottom: 10px;
              padding: 8px; border: 1px solid #3a2b2b; background: #1c1516; border-radius: 6px; }
  .faultbox .faultlabel { font-size: 11px; color: #e0a83d; font-weight: 600; }
  .faultbox button.faulton { background: #7a2f2f; border-color: #e0533d; color: #fff; font-weight: 600; }
  .speedbox, .quickbox { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin-bottom: 10px;
              padding: 8px; border: 1px solid #23303a; background: #141a20; border-radius: 6px; }
  .speedbox .speedlabel, .quickbox .quicklabel { font-size: 11px; color: #6fb3e0; font-weight: 600; }
  .speedbox button.speedon { background: #23506f; border-color: #6fb3e0; color: #fff; font-weight: 600; }
  .speedbox button.zerohold { border-color: #e0a83d; color: #e0a83d; }
  .speedbox button.zeroon { background: #7a5a1f; border-color: #e0a83d; color: #fff; font-weight: 600; }
  .speedstate { font-size: 11px; color: #8b93a1; margin-left: auto; }
  .quickbox button { background: #2f5f4f; border-color: #35c46a55; }
  .quickbox button:hover { background: #367a58; }
  .quickbox button.autostopon { background: #23506f; border-color: #6fb3e0; color: #fff; font-weight: 600; }
  .autostopstate { font-size: 11px; color: #8b93a1; margin-left: auto; }
  .autostopstate.active { color: #6fb3e0; font-weight: 600; }
  .urlbox { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin-bottom: 10px;
            padding: 8px; border: 1px solid #2a2f3a; background: #14171d; border-radius: 6px; }
  .urlbox .urllabel { font-size: 11px; color: #9a8fd0; font-weight: 600; }
  .urlbox input { flex: 1 1 180px; width: auto; min-width: 140px; }
  .urlbox button { background: #2e2a45; border-color: #4a4370; }
  .urlbox button:hover { background: #3a3557; }
  .urlbox button.urlreset { background: #232830; border-color: #3a4150; }
  .urlstate { flex-basis: 100%; font-size: 11px; color: #8b93a1; word-break: break-all; }
  .urlstate.active { color: #9a8fd0; font-weight: 600; }
  .delaybox { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin-bottom: 10px;
              padding: 8px; border: 1px solid #3a3326; background: #1b1812; border-radius: 6px; }
  .delaybox .delaylabel { font-size: 11px; color: #e0c23d; font-weight: 600; flex-basis: 100%; }
  .delaybox input { width: 80px; }
  .delaybox button.delayon { background: #6f5a23; border-color: #e0c23d; color: #fff; font-weight: 600; }
  .delayhint { flex-basis: 100%; font-size: 11px; color: #8b93a1; }
  .conncards { display: grid; gap: 12px; grid-template-columns: repeat(auto-fill, minmax(380px, 1fr)); margin-bottom: 10px; }
  .conn { padding: 10px; border: 1px solid #23262d; border-radius: 6px; background: #14171d; }
  .connhead { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; font-size: 12px; font-weight: 600; flex-wrap: wrap; }
  .connhead .sub { margin-left: auto; font-weight: 400; color: #8b93a1; font-size: 11px; }
  .conn .row { margin-bottom: 6px; }
  .conn .row:last-child { margin-bottom: 0; }
  .conn select, .conn input { width: auto; }
  .conn input.idtag { width: 200px; }
  label.inline { display: inline-flex; align-items: center; gap: 4px; font-size: 12px; color: #8b93a1; }
  label.inline input { width: auto; }
  pre.frames { background: #0b0d11; border: 1px solid #23262d; border-radius: 6px; padding: 8px;
               height: 240px; overflow: auto; font-size: 11px; margin: 0 0 10px; white-space: pre-wrap;
               word-break: break-all; }
  pre.frames .fin { color: #4ade80; }
  pre.frames .fout { color: #22d3ee; }
  pre.reply { background: #0b0d11; border: 1px solid #23262d; border-radius: 6px; padding: 8px;
              max-height: 160px; overflow: auto; font-size: 11px; margin: 0 0 10px; white-space: pre-wrap; }
  .paneltitle { font-size: 11px; color: #8b93a1; font-weight: 600; margin: 0 0 4px; }
  textarea, input { width: 100%; background: #0f1115; color: #e6e6e6; border: 1px solid #333a45;
                    border-radius: 6px; padding: 6px 8px; font: 12px monospace; }
  .custom { display: grid; gap: 6px; margin-bottom: 0; }
  pre.log { background: #0b0d11; border: 1px solid #23262d; border-radius: 6px; padding: 8px;
            height: 220px; overflow: auto; font-size: 11px; margin: 0; white-space: pre-wrap;
            word-break: break-word; }
  .toast { position: fixed; bottom: 16px; right: 16px; background: #262b34; border: 1px solid #333a45;
           border-radius: 8px; padding: 10px 14px; font-size: 12px; max-width: 380px;
           opacity: 0; transform: translateY(8px); transition: .2s; pointer-events: none; }
  .toast.show { opacity: 1; transform: none; }
  .toast.err { border-color: #e0533d; }
  .hint { color: #8b93a1; font-size: 11px; margin-top: 10px; }
  .formerr { color: #e0533d; font-size: 12px; font-weight: 600; margin-top: 8px; }
</style>
</head>
<body>
<header>
  <h1>OCPP Simulator Control</h1>
  <span class="badge badge-available" id="meta">loading…</span>
</header>
<div class="layout">
<aside>
  <ul class="nav" id="nav-top">
    <li data-nav="overview" class="selected"><span class="navid">Overview</span><span class="navstate" id="nav-count"></span></li>
  </ul>
  <div class="navtitle">Add station</div>
  <form class="addform" id="add-form">
    <label>Station id <input data-cpid placeholder="SIM-0004" autocomplete="off" required /></label>
    <label>Connectors <input data-connectors type="number" min="1" max="20" value="1" /></label>
    <label>OCPP URL (optional) <input data-wsurl placeholder="ws://localhost:9000" /></label>
    <button class="primary" type="submit" id="add-submit">Add</button>
  </form>
  <div class="hint">The id must exist as a charger in Spark with that serial: Spark registers it with Cosmos through cosmos-hub. Until then Cosmos refuses the connection and the station keeps retrying.</div>
  <div class="formerr" id="add-error" style="display:none;"></div>
  <div class="navtitle">Stations</div>
  <ul class="nav" id="nav-stations"></ul>
  <div class="navempty" id="nav-empty">no stations yet</div>
  <div class="hint" id="add-dirs"></div>
</aside>
<main class="content">
  <section id="overview" style="display:contents;">
    <div class="card">
      <h2>Stations<span class="sub" id="tx-meta">loading…</span></h2>
      <div class="body">
        <div class="logscroll">
          <table class="txlog" id="fleet-table">
            <thead><tr><th>Station</th><th>Admin</th><th>Connected</th><th>Transaction</th><th>Meter Wh</th><th>Runner</th><th>pid</th><th>Scheduler</th></tr></thead>
            <tbody id="fleet-body"></tbody>
          </table>
        </div>
        <div class="empty" id="fleet-empty">No stations. Add one on the left.</div>
      </div>
    </div>
    <div class="card">
      <h2>Charging keys<span class="sub">idTag[,label] per line, one per account</span></h2>
      <div class="body">
        <textarea id="keys-text" rows="6" placeholder="idTag,label"></textarea>
        <div class="row" style="margin: 6px 0 0;"><button class="primary" id="keys-save">Save keys</button></div>
      </div>
    </div>
    <div class="card">
      <h2>Transaction log<span class="sub">most recent first</span></h2>
      <div class="body">
        <div class="logscroll">
          <table class="txlog">
            <thead><tr><th>Started</th><th>Duration</th><th>Sim</th><th>Key (label)</th><th>Txn</th><th>kWh</th><th>Source</th><th>Status</th></tr></thead>
            <tbody id="tx-log-body"></tbody>
          </table>
        </div>
      </div>
    </div>
  </section>
  <section id="station" style="display:none;"></section>
</main>
</div>
<div class="toast" id="toast"></div>
<script>
// OCPP 1.6 connector statuses, error codes and stop reasons for the pickers.
const STATUSES = ["Available", "Preparing", "Charging", "SuspendedEV", "SuspendedEVSE", "Finishing", "Reserved", "Unavailable", "Faulted"];
const ERROR_CODES = ["NoError", "ConnectorLockFailure", "EVCommunicationError", "GroundFailure", "HighTemperature", "InternalError",
  "LocalListConflict", "OtherError", "OverCurrentFailure", "OverVoltage", "PowerMeterFailure", "PowerSwitchFailure",
  "ReaderFailure", "ResetFailure", "UnderVoltage", "WeakSignal"];
const STOP_REASONS = ["Local", "Remote", "EVDisconnected", "PowerLoss", "DeAuthorized", "EmergencyStop", "Other"];
// "__TOKEN__" is the VCP's placeholder for the TOKEN in the station's own env file.
const DEFAULT_ID_TAG = "__TOKEN__";
const SELECTED_KEY = "ocpp-panel-selected";
const optionsHtml = (values) => values.map((v) => \`<option value="\${v}">\${v}</option>\`).join("");

function escapeHtml(v) {
  return String(v ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

function toast(msg, err) {
  const t = document.getElementById("toast");
  t.textContent = msg; t.className = "toast show" + (err ? " err" : "");
  clearTimeout(t._h); t._h = setTimeout(() => (t.className = "toast"), 4000);
}

// --- API calls (one function per endpoint the buttons use) -----------------

async function exec(id, action, payload) {
  try {
    const r = await fetch(\`/api/sims/\${id}/execute\`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, payload }),
    });
    const d = await r.json();
    if (d.ok) toast(\`\${id}: \${action} sent ✓\`);
    else toast(\`\${id}: \${action} failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(\`\${id}: \${action} error — \${e}\`, true); }
}

async function setFailMode(id, mode, durationMs) {
  try {
    const body = mode === "off" ? { mode } : { mode, durationMs };
    const r = await fetch(\`/api/sims/\${id}/fail-mode\`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if (d.ok) toast(\`\${id}: fail mode -> \${mode}\${mode !== "off" ? " ("+(durationMs/1000)+"s)" : ""} ✓\`);
    else toast(\`\${id}: fail-mode failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(\`\${id}: fail-mode error — \${e}\`, true); }
  refreshSims();
}

async function setFault(id, type, on) {
  const label = type === "high_temperature" ? "high temp" : "faulted";
  try {
    const r = await fetch(\`/api/sims/\${id}/fault\`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ type, on }),
    });
    const d = await r.json();
    if (d.ok) toast(\`\${id}: \${label} \${on ? "ON" : "off"} ✓\`);
    else toast(\`\${id}: \${label} failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(\`\${id}: \${label} error — \${e}\`, true); }
  refreshSims();
}

async function setChargingPower(id, kw, intervalMs) {
  // kw === null restores the legacy fixed rate; intervalMs === null restores
  // the default 15 s MeterValues cadence.
  const fast = kw === 180 && intervalMs === 5000;
  const label = fast ? "0.25 kWh / 5s" : kw == null ? "default" : kw + " kW";
  try {
    const r = await fetch(\`/api/sims/\${id}/charging-power\`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ kw, intervalMs }),
    });
    const d = await r.json();
    if (d.ok) toast(\`\${id}: charging speed -> \${label} ✓\`);
    else toast(\`\${id}: speed failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(\`\${id}: speed error — \${e}\`, true); }
  refreshSims();
}

async function setDelays(id, body) {
  const label = Object.entries(body).map(([k, v]) => \`\${k === "replyMs" ? "reply" : "act"} \${v} ms\`).join(", ");
  try {
    const r = await fetch(\`/api/sims/\${id}/delays\`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if (d.ok) toast(\`\${id}: delays -> \${label} ✓\`);
    else toast(\`\${id}: delays failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(\`\${id}: delays error — \${e}\`, true); }
  refreshSims();
}

async function setAutoMeter(id, auto) {
  try {
    const r = await fetch(\`/api/sims/\${id}/meter\`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ auto }),
    });
    const d = await r.json();
    if (d.ok) toast(\`\${id}: auto meter \${auto ? "on" : "off"} ✓\`);
    else toast(\`\${id}: auto meter failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(\`\${id}: auto meter error — \${e}\`, true); }
  refreshSims();
}

async function meterTick(id, connectorId) {
  try {
    const r = await fetch(\`/api/sims/\${id}/meter-tick\`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ connectorId }),
    });
    const d = await r.json();
    if (d.ok) toast(\`\${id} c\${connectorId}: MeterValues sent ✓\`);
    else toast(\`\${id} c\${connectorId}: meter tick failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(\`\${id}: meter tick error — \${e}\`, true); }
}

async function removeSim(s) {
  if (!confirm(s.cpId + ": remove this station? Its process is stopped and its profile deleted; the log file is kept.")) return;
  toast(s.cpId + ": removing…");
  try {
    const r = await fetch(\`/api/sims/\${encodeURIComponent(s.id)}\`, { method: "DELETE" });
    const d = await r.json();
    if (r.ok) toast(s.cpId + ": removed ✓");
    else toast(\`\${s.cpId}: remove failed — \${d.error || r.status}\`, true);
  } catch (e) { toast(\`\${s.cpId}: remove error — \${e}\`, true); }
  refreshSims(); refreshTxState();
}

// One button for both directions: Disconnect while the socket is open,
// Connect while the station is manually offline.
async function toggleConnection(id) {
  const s = simsById[id];
  const offline = !!(s && (s.offline || !s.connected));
  const path = offline ? "connect" : "disconnect";
  toast(id + (offline ? ": connecting…" : ": disconnecting…"));
  try {
    const r = await fetch(\`/api/sims/\${id}/\${path}\`, { method: "POST" });
    const d = await r.json();
    if (d.ok) toast(id + (offline ? ": connected ✓ (BootNotification sent)" : ": disconnected — stays offline until Connect or Restart"));
    else toast(\`\${id}: \${path} failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(id + ": " + path + " error — " + e, true); }
  refreshSims();
}

async function restartSim(id) {
  if (!confirm(id + ": restart this simulator? Any active session on it will be dropped; it reconnects in ~3s.")) return;
  toast(id + ": restarting…");
  try {
    const r = await fetch(\`/api/sims/\${id}/restart\`, { method: "POST" });
    const d = await r.json();
    if (d.ok) toast(id + ": restart signal sent ✓ (reconnecting…)");
    else toast(\`\${id}: restart failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(id + ": restart error — " + e, true); }
  // Give the supervisor time to relaunch (~3s down) before re-polling.
  setTimeout(refreshSims, 4500);
}

async function setWsUrl(id, url) {
  // url === null resets to the .env baseline; a string sets a persisted override.
  const label = url == null ? "the .env default" : url;
  if (!confirm(id + ": point OCPP at " + label + "? The charger restarts and reconnects in ~3s; any active session on it is dropped.")) return;
  toast(id + ": switching OCPP URL…");
  try {
    const r = await fetch(\`/api/sims/\${id}/ws-url\`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ url }),
    });
    const d = await r.json();
    if (d.ok) toast(\`\${id}: OCPP URL -> \${label} ✓ (reconnecting…)\`);
    else toast(\`\${id}: ws-url failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(\`\${id}: ws-url error — \${e}\`, true); }
  // Give the supervisor time to relaunch (~3s down) before re-polling.
  setTimeout(refreshSims, 4500);
}

async function quickTx(id, kwh) {
  toast(\`\${id}: creating \${kwh} kWh transaction…\`);
  try {
    const r = await fetch(\`/api/sims/\${id}/quick-tx\`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ kwh }),
    });
    const d = await r.json();
    if (d.ok) toast(\`\${id}: \${kwh} kWh transaction created (txn #\${d.transactionId}) ✓\`);
    else toast(\`\${id}: quick-tx failed — \${d.error || d.response || d.status}\`, true);
  } catch (e) { toast(\`\${id}: quick-tx error — \${e}\`, true); }
  refreshTxState(); refreshTxLog();
}

async function setAutoStop(id, kwh) {
  // kwh === null disarms. Arming also sets the 0.25 kWh / 5s ramp (180 kW).
  const arm = kwh != null;
  try {
    const r = await fetch(\`/api/sims/\${id}/charge-target\`, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify(arm ? { kwh, kw: 180, intervalMs: 5000 } : { kwh: null }),
    });
    const d = await r.json();
    if (d.ok) toast(arm
      ? \`\${id}: armed — next session auto-stops at exactly \${kwh.toFixed(2)} kWh (0.25/5s) ✓\`
      : \`\${id}: auto-stop cleared ✓\`);
    else toast(\`\${id}: auto-stop failed — \${d.response || d.error || d.status}\`, true);
  } catch (e) { toast(\`\${id}: auto-stop error — \${e}\`, true); }
  refreshSims();
}

// The scheduler's start / stop / resume-auto for one connector.
async function txExec(simId, connectorId, action) {
  try {
    const r = await fetch(\`/api/tx/\${simId}/\${connectorId}/\${action}\`, { method: "POST" });
    const d = await r.json();
    if (d.ok !== false) toast(\`\${simId} c\${connectorId}: \${action} ✓\`);
    else toast(\`\${simId} c\${connectorId}: \${action} failed — \${d.error || "unknown error"}\` +
      (action === "start" && /no free charging keys/.test(d.error || "") ? " (add keys on the Overview, or type an idTag)" : ""), true);
  } catch (e) { toast(\`\${simId} c\${connectorId}: \${action} error — \${e}\`, true); }
  refreshTxState(); refreshSims();
}

// --- client state -----------------------------------------------------------

// Last /api/sims answer, by sim id and in listing order.
const simsById = {};
let simOrder = [];
// The live transaction the sim holds on a connector, from the last /api/sims poll.
const liveTx = {};
function txOn(id, connectorId) {
  return (liveTx[id] || []).find((t) => t.connectorId === connectorId) || null;
}
// The scheduler's view of each connector, from the last /api/tx/state poll.
const schedStates = {}; // "simId:connectorId" -> state
function schedOn(id, connectorId) {
  return schedStates[id + ":" + connectorId] || null;
}
let txSessionLimit = 0;
let txFailureThreshold = 5;

// What the main area shows: "overview" or a sim id. Remembered across reloads.
let selected = "overview";
try { selected = localStorage.getItem(SELECTED_KEY) || "overview"; } catch {}
function isOverview() { return selected === "overview"; }
function stationVisible() { return !isOverview() && stationView && stationView.id === selected; }

function select(id) {
  selected = id;
  try { localStorage.setItem(SELECTED_KEY, id); } catch {}
  renderNavSelection();
  renderMain();
}

function renderNavSelection() {
  for (const li of document.querySelectorAll("ul.nav li[data-nav]")) {
    li.classList.toggle("selected", li.dataset.nav === selected);
  }
}

document.querySelector("#nav-top li[data-nav=overview]").onclick = () => select("overview");

// --- sidebar ----------------------------------------------------------------

let navShape = "";
function renderNav() {
  const host = document.getElementById("nav-stations");
  const shape = simOrder.join(",");
  if (shape !== navShape) {
    navShape = shape;
    host.innerHTML = simOrder.map((id) => \`<li data-nav="\${escapeHtml(id)}">
      <span class="navid" title="\${escapeHtml(simsById[id].id)}">\${escapeHtml(simsById[id].cpId)}</span>
      <span class="badge badge-available" data-nav-conn></span>
      <span class="navstate" data-nav-state></span></li>\`).join("");
    for (const li of host.querySelectorAll("li")) li.onclick = () => select(li.dataset.nav);
    renderNavSelection();
  }
  document.getElementById("nav-empty").style.display = simOrder.length ? "none" : "";
  for (const li of host.querySelectorAll("li")) {
    const s = simsById[li.dataset.nav];
    const conn = li.querySelector("[data-nav-conn]");
    conn.textContent = connectionLabel(s);
    conn.className = "badge " + (s.connected ? "badge-on" : "badge-off");
    li.querySelector("[data-nav-state]").textContent = s.up ? "up" : "down";
  }
  document.getElementById("nav-count").textContent = simOrder.length ? String(simOrder.length) : "";
}

// "offline" is the station holding its socket closed on purpose (Disconnect);
// "disconnected" is a socket it would reopen by itself.
function connectionLabel(s) {
  return s.connected ? "connected" : s.offline ? "offline" : "disconnected";
}

function connectedBadge(el, s) {
  el.textContent = !s.up ? "down" : connectionLabel(s);
  el.className = "badge " + (s.connected ? "badge-on" : "badge-off");
}

// --- overview ---------------------------------------------------------------

function schedLabel(st) {
  if (!st) return "";
  if (st.mode === "auto") {
    const current = st.tx ? st.sessionCount + 1 : st.sessionCount;
    return \`AUTO · \${current}\${txSessionLimit > 0 ? "/" + txSessionLimit : ""}\`;
  }
  return st.pauseReason === "batch_complete" ? "BATCH DONE" : "MANUAL";
}

let fleetShape = "";
function renderOverview() {
  const body = document.getElementById("fleet-body");
  const shape = simOrder.map((id) => id + ":" + simsById[id].numConnectors).join(",");
  if (shape !== fleetShape) {
    fleetShape = shape;
    body.innerHTML = simOrder.map((id) => {
      const s = simsById[id];
      const plugs = [];
      for (let c = 1; c <= s.numConnectors; c++) plugs.push(c);
      const per = (attr) => plugs.map((c) => \`<div data-\${attr}="\${c}">\${s.numConnectors > 1 ? "c" + c + ": " : ""}<span></span></div>\`).join("");
      return \`<tr data-row="\${escapeHtml(id)}">
        <td><span class="link" data-select>\${escapeHtml(s.cpId)}</span><br /><span class="dim">\${escapeHtml(s.id)}</span></td>
        <td data-cell="port"></td>
        <td><span class="badge badge-available" data-cell="conn"></span></td>
        <td>\${per("tx")}</td>
        <td>\${per("wh")}</td>
        <td data-cell="runner"></td>
        <td data-cell="pid"></td>
        <td>\${plugs.map((c) => \`<div data-sched="\${c}">\${s.numConnectors > 1 ? "c" + c + ": " : ""}<span data-sched-label></span> <button data-resume title="Hand this connector back to the auto scheduler (a fresh batch)" style="display:none;">Resume auto</button></div>\`).join("")}</td>
      </tr>\`;
    }).join("");
    for (const tr of body.querySelectorAll("tr")) {
      tr.querySelector("[data-select]").onclick = () => select(tr.dataset.row);
      for (const div of tr.querySelectorAll("[data-sched]")) {
        div.querySelector("[data-resume]").onclick = () => txExec(tr.dataset.row, Number(div.dataset.sched), "resume-auto");
      }
    }
  }
  document.getElementById("fleet-table").style.display = simOrder.length ? "" : "none";
  document.getElementById("fleet-empty").style.display = simOrder.length ? "none" : "";
  for (const tr of body.querySelectorAll("tr")) {
    const s = simsById[tr.dataset.row];
    tr.querySelector("[data-cell=port]").textContent = ":" + s.adminPort;
    connectedBadge(tr.querySelector("[data-cell=conn]"), s);
    tr.querySelector("[data-cell=runner]").textContent = s.managed ? "panel" : "external";
    tr.querySelector("[data-cell=pid]").textContent = s.pid != null ? String(s.pid) : "—";
    for (const div of tr.querySelectorAll("[data-tx]")) {
      const tx = txOn(s.id, Number(div.dataset.tx));
      div.querySelector("span").textContent = tx ? "#" + tx.transactionId : "—";
    }
    for (const div of tr.querySelectorAll("[data-wh]")) {
      const tx = txOn(s.id, Number(div.dataset.wh));
      div.querySelector("span").textContent = tx ? String(tx.meterWh) : "—";
    }
  }
  renderOverviewScheduler();
}

function renderOverviewScheduler() {
  for (const tr of document.querySelectorAll("#fleet-body tr")) {
    for (const div of tr.querySelectorAll("[data-sched]")) {
      const st = schedOn(tr.dataset.row, Number(div.dataset.sched));
      div.querySelector("[data-sched-label]").textContent = st ? schedLabel(st) : "—";
      div.querySelector("[data-resume]").style.display = st && st.mode === "manual" ? "" : "none";
    }
  }
}

// --- station page -----------------------------------------------------------

// The card header's detail line: profile, admin port, plugs, who runs the
// process (the panel, with its pid, or a shell script outside it).
function subLine(s) {
  const plugs = \`\${s.numConnectors} plug\${s.numConnectors > 1 ? "s" : ""}\`;
  const runner = s.managed ? \`panel\${s.pid ? " pid " + s.pid : ""}\` : "external process";
  return \`\${s.id} · admin :\${s.adminPort} · \${plugs} · \${runner}\`;
}

let stationView = null; // { id, shape, el }
const framesClearedAt = {}; // sim id -> ISO timestamp; frames at or before it are hidden
const lastFrameAt = {}; // sim id -> ISO timestamp of the newest frame seen

// Everything about one station, built once per shape and then updated in place
// by updateStation / updateStationScheduler / refreshFrames / refreshLogs.
function buildStation(s) {
  const el = document.createElement("div");
  el.style.display = "contents";
  el.innerHTML = \`
    <div class="stationhead">
      <span class="dot \${s.up ? "up" : "down"}"></span>
      <h2>\${escapeHtml(s.cpId)}</h2>
      <span class="badge badge-available" data-conn-badge></span>
      <span class="badge badge-available" data-proc-badge></span>
      <span class="badge badge-warn" data-fail-badge style="display:none;"></span>
      <span class="badge badge-off" data-fault-badge style="display:none;"></span>
      <span class="meta" data-sub>\${escapeHtml(subLine(s))}</span>
      <span class="spacer"></span>
      <button class="restart-btn" data-connect-toggle title="Disconnect closes the OCPP socket and keeps this station offline (no auto-reconnect, no respawn) until Connect or Restart. Connect reopens it with a fresh BootNotification.">⇅ Disconnect</button>
      <button class="restart-btn" data-restart title="Restart this simulator process — reloads its .env (WS_URL etc.) + code. Drops any active session; reconnects in ~3s.">⟲ Restart</button>
      \${s.managed ? \`<button class="remove-btn" data-remove title="Stop this station's process and delete its profile (\${escapeHtml(s.profile)}). The log file is kept.">✕ Remove</button>\` : ""}
      <div class="wsurl" data-wsurl>\${escapeHtml(s.wsUrl)}/\${escapeHtml(s.cpId)}</div>
    </div>
    <details class="card" open data-settings>
      <summary>Settings<span class="sub">configuration only; actions are below</span></summary>
      \${s.isTestCharger ? \`
      <div class="setgroup">
        <div class="setlabel">Connection</div>
        <div class="urlbox">
          <span class="urllabel">OCPP URL:</span>
          <input data-wsurl-input placeholder="ws://host or wss://host" />
          <button data-wsurl-set title="Point this charger at a new OCPP backend. The value is persisted (survives restarts) and the charger restarts (~3s) to reconnect. The CP id is appended automatically — enter just the host.">Set &amp; reconnect</button>
          <button data-wsurl-reset class="urlreset" title="Clear the override and revert to the WS_URL baseline in this profile's .env, then reconnect.">Reset to .env</button>
          <span class="urlstate" data-wsurl-state></span>
        </div>
      </div>\` : ""}
      <div class="setgroup">
        <div class="setlabel">Timing &amp; metering</div>
        <div class="delaybox">
          <span class="delaylabel">Delays</span>
          <label class="inline">Answer commands after <input type="number" min="0" max="600000" step="500" data-reply-delay /> ms</label>
          <button data-reply-apply>Apply</button>
          <button data-reply-preset="12000" title="Hold every CALLRESULT 12 s, past Spark's COSMOS_TIMEOUT (8 s default, 10 s in dev): Spark reports the command unreachable while this station still accepts it late">Outlast Spark (12 s)</button>
          <button data-reply-preset="0">At once</button>
          <span class="delayhint" data-reply-hint></span>
          <label class="inline">Act on RemoteStart/Stop after <input type="number" min="0" max="600000" step="500" data-act-delay /> ms</label>
          <button data-act-apply>Apply</button>
          <button data-act-preset="12000" title="Authorize/StartTransaction/StopTransaction and the status reports follow 12 s after the command">Slow charger (12 s)</button>
          <button data-act-preset="1000">Default (1 s)</button>
          <span class="delayhint" data-act-hint></span>
        </div>
        <div class="row">
          <label class="inline" title="The station's own periodic MeterValues for every running transaction (MeterValueSampleInterval). Off: tick by hand with Meter tick on the connector.">
            <input type="checkbox" data-auto-meter /> Auto meter <span data-meter-hint></span>
          </label>
        </div>
      </div>
      \${s.isTestCharger ? \`
      <div class="setgroup">
        <div class="setlabel">Charging</div>
        <div class="speedbox">
          <span class="speedlabel">Charging speed:</span>
          <button data-speed="0" title="Hold charging at 0 kW — the session reports 0 kWh no matter how long it runs. Set it before starting from the app; toggle anytime.">0 kW (hold)</button>
          <button data-speed="3.7">3.7 kW</button>
          <button data-speed="7.4">7.4 kW</button>
          <button data-speed="11">11 kW</button>
          <button data-speed="22">22 kW</button>
          <button data-speed="50">50 kW</button>
          <button data-speed="fast" data-kw="180" data-interval="5000" title="Fast test rate: energy register climbs 0.25 kWh per MeterValues report, sent every 5 s (equivalent to 180 kW)">0.25 kWh / 5s</button>
          <button data-speed="default" title="Restore the legacy fixed rate (~0.15 kWh every 15 s)">default</button>
          <span class="speedstate" data-speed-state></span>
        </div>
        <div class="quickbox">
          <span class="quicklabel">Auto-stop:</span>
          <button data-autostop="1" title="Arm the charger to stop ITSELF at exactly 1.000 kWh (no overshoot) on the next session — including one you start from the mobile app — and set the 0.25 kWh / 5s ramp. Stays armed until cleared.">Stop at 1 kWh @0.25/5s</button>
          <button data-autostop-clear title="Disarm the auto-stop target">Clear</button>
          <span class="autostopstate" data-autostop-state></span>
        </div>
      </div>
      <div class="setgroup">
        <div class="setlabel">Fault injection</div>
        <div class="failbox">
          <span class="faillabel">RemoteStart fault:</span>
          <button data-fail-ignore title="Drop the RemoteStartTransaction (no response) for 60s; connector stays Available">Fail (ignore) 60s</button>
          <button data-fail-reject title="Reject the RemoteStartTransaction for 60s; connector stays Available">Fail (reject) 60s</button>
          <button data-fail-clear title="Clear fail mode now">Clear</button>
          <span class="failstate" data-fail-state></span>
        </div>
        <div class="faultbox">
          <span class="faultlabel">Plug fault (email test):</span>
          <button data-fault-faulted title="Toggle StatusNotification status=Faulted / errorCode=OtherError">Faulted: off</button>
          <button data-fault-hightemp title="Toggle StatusNotification status=Faulted / errorCode=HighTemperature">High temp: off</button>
        </div>
      </div>\` : ""}
    </details>
    <div class="card">
      <h2>Actions</h2>
      <div class="body">
        <div class="row">
          <button data-boot title="Send BootNotification now">Boot</button>
          <button data-heartbeat title="Send one Heartbeat">Heartbeat</button>
          \${s.isTestCharger ? \`
          <span class="dim" style="margin: 0 0 0 8px;">Quick transaction on connector 1:</span>
          <button data-quick="0" title="Start + immediately stop with meterStop=0 (a complete 0 kWh session) on connector 1, with the next free charging key">0 kWh</button>
          <button data-quick="1" title="Start + immediately stop with meterStop=1000 Wh (a complete 1 kWh session) on connector 1, with the next free charging key">1 kWh</button>\` : ""}
        </div>
        <div class="conncards" data-connectors></div>
        <div class="custom">
          <input data-action placeholder="Custom action (e.g. MeterValues)" />
          <textarea data-payload rows="2" placeholder='{"payload": "as JSON"}'>{}</textarea>
          <div class="row" style="margin: 0;"><button class="primary" data-send>Send custom command</button></div>
        </div>
      </div>
    </div>
    <div class="card">
      <h2>Frames<span class="sub">last 200, newest last</span><button data-frames-clear title="Hide the frames received so far (the station keeps them)">Clear</button></h2>
      <div class="body">
        <pre class="frames" data-frames>…</pre>
        <div class="paneltitle">Last reply from the CSMS</div>
        <pre class="reply" data-reply>none yet</pre>
        <div class="paneltitle">Process log</div>
        <pre class="log" data-log>…</pre>
      </div>
    </div>\`;
  el.querySelector("[data-boot]").onclick = () => exec(s.id, "BootNotification", {
    chargePointVendor: "Solidstudio", chargePointModel: "VirtualChargePoint",
    chargePointSerialNumber: "S001", firmwareVersion: "1.0.0",
  });
  el.querySelector("[data-heartbeat]").onclick = () => exec(s.id, "Heartbeat", {});
  const autoMeter = el.querySelector("[data-auto-meter]");
  autoMeter.onchange = () => setAutoMeter(s.id, autoMeter.checked);
  const replyInput = el.querySelector("[data-reply-delay]");
  const actInput = el.querySelector("[data-act-delay]");
  el.querySelector("[data-reply-apply]").onclick = () => setDelays(s.id, { replyMs: Number(replyInput.value) });
  el.querySelector("[data-act-apply]").onclick = () => setDelays(s.id, { actMs: Number(actInput.value) });
  el.querySelectorAll("[data-reply-preset]").forEach((b) => {
    b.onclick = () => { replyInput.value = b.dataset.replyPreset; setDelays(s.id, { replyMs: Number(b.dataset.replyPreset) }); };
  });
  el.querySelectorAll("[data-act-preset]").forEach((b) => {
    b.onclick = () => { actInput.value = b.dataset.actPreset; setDelays(s.id, { actMs: Number(b.dataset.actPreset) }); };
  });
  // One card per connector with everything about that connector. Start and
  // Stop are the single place to start or stop a transaction on it: an empty
  // idTag field starts through the scheduler (next free charging key; the
  // session is tracked, timed and logged to the CSV), a typed idTag sends a
  // raw StartTransaction. Stop closes a scheduler-tracked session through the
  // scheduler (which releases the key and logs it) and any other live
  // transaction with a raw StopTransaction quoting its register and reason.
  const connectors = el.querySelector("[data-connectors]");
  for (let c = 1; c <= s.numConnectors; c++) {
    const block = document.createElement("div");
    block.className = "conn";
    block.dataset.connector = String(c);
    block.innerHTML = \`
      <div class="connhead">Connector \${c} <span class="badge badge-available" data-conn-status>no transaction</span><span class="sub" data-mode-label></span></div>
      <div class="dim" data-tx-line>…</div>
      <div class="alert" data-alert style="display:none;"></div>
      <div class="row">
        <select data-status>\${optionsHtml(STATUSES)}</select>
        <select data-error>\${optionsHtml(ERROR_CODES)}</select>
        <button data-send-status>Send status</button>
        <button data-plug-in title="StatusNotification Preparing">Plug in</button>
        <button data-unplug title="StatusNotification Available">Unplug</button>
      </div>
      <div class="row">
        <input class="idtag" data-idtag placeholder="idTag · empty = next free charging key" title="Empty: Start takes the next free charging key and the scheduler tracks, times and logs the session. Typed: a raw StartTransaction with this idTag (__TOKEN__ is replaced by the station's TOKEN env var). Authorize uses the typed idTag, or __TOKEN__ when empty." />
        <button data-authorize>Authorize</button>
        <button class="primary" data-start title="Empty idTag: start through the scheduler with the next free charging key. Typed idTag: raw StartTransaction with meterStart 0.">Start transaction</button>
        <button data-tick title="One MeterValues now with the register as it stands">Meter tick</button>
      </div>
      <div class="row">
        <select data-reason>\${optionsHtml(STOP_REASONS)}</select>
        <button data-stop title="A scheduler-tracked session is stopped through the scheduler (its key goes back to the pool, the CSV log gets the row). Any other live transaction gets a raw StopTransaction with this reason and the current register.">Stop transaction</button>
      </div>\`;
    const status = () => ({ connectorId: c, errorCode: block.querySelector("[data-error]").value, status: block.querySelector("[data-status]").value });
    const typedIdTag = () => block.querySelector("[data-idtag]").value.trim();
    block.querySelector("[data-send-status]").onclick = () => exec(s.id, "StatusNotification", status());
    block.querySelector("[data-plug-in]").onclick = () => exec(s.id, "StatusNotification", { connectorId: c, errorCode: "NoError", status: "Preparing" });
    block.querySelector("[data-unplug]").onclick = () => exec(s.id, "StatusNotification", { connectorId: c, errorCode: "NoError", status: "Available" });
    block.querySelector("[data-authorize]").onclick = () => exec(s.id, "Authorize", { idTag: typedIdTag() || DEFAULT_ID_TAG });
    block.querySelector("[data-start]").onclick = () => {
      const idTag = typedIdTag();
      if (!idTag) return txExec(s.id, c, "start");
      exec(s.id, "StartTransaction", {
        connectorId: c, idTag, meterStart: 0, timestamp: new Date().toISOString(),
      });
    };
    block.querySelector("[data-tick]").onclick = () => meterTick(s.id, c);
    block.querySelector("[data-stop]").onclick = () => {
      const st = schedOn(s.id, c);
      if (st && st.tx) return txExec(s.id, c, "stop");
      const tx = txOn(s.id, c);
      if (!tx) return toast(\`\${s.id}: connector \${c} has no transaction to stop\`, true);
      exec(s.id, "StopTransaction", {
        transactionId: tx.transactionId, meterStop: tx.meterWh,
        timestamp: new Date().toISOString(), reason: block.querySelector("[data-reason]").value,
      });
    };
    connectors.appendChild(block);
  }
  el.querySelector("[data-connect-toggle]").onclick = () => toggleConnection(s.id);
  el.querySelector("[data-restart]").onclick = () => restartSim(s.id);
  const removeBtn = el.querySelector("[data-remove]");
  if (removeBtn) removeBtn.onclick = () => removeSim(s);
  if (s.isTestCharger) {
    const urlInput = el.querySelector("[data-wsurl-input]");
    el.querySelector("[data-wsurl-set]").onclick = () => {
      const v = urlInput.value.trim();
      if (!v) return toast("Enter a ws:// or wss:// host", true);
      if (!/^wss?:\\/\\//i.test(v)) return toast("URL must start with ws:// or wss://", true);
      setWsUrl(s.id, v);
    };
    el.querySelector("[data-wsurl-reset]").onclick = () => setWsUrl(s.id, null);
    el.querySelector("[data-fail-ignore]").onclick = () => setFailMode(s.id, "ignore", 60000);
    el.querySelector("[data-fail-reject]").onclick = () => setFailMode(s.id, "reject", 60000);
    el.querySelector("[data-fail-clear]").onclick = () => setFailMode(s.id, "off");
    const ft = el.querySelector("[data-fault-faulted]");
    const ht = el.querySelector("[data-fault-hightemp]");
    ft.onclick = () => setFault(s.id, "faulted", ft.dataset.on !== "true");
    ht.onclick = () => setFault(s.id, "high_temperature", ht.dataset.on !== "true");
    el.querySelectorAll("[data-speed]").forEach((b) => {
      b.onclick = () => {
        const sp = b.dataset.speed;
        if (sp === "default") return setChargingPower(s.id, null, null);
        if (sp === "fast") return setChargingPower(s.id, parseFloat(b.dataset.kw), parseFloat(b.dataset.interval));
        return setChargingPower(s.id, parseFloat(sp), null);
      };
    });
    const autoStopBtn = el.querySelector("[data-autostop]");
    autoStopBtn.onclick = () => setAutoStop(s.id, parseFloat(autoStopBtn.dataset.autostop));
    el.querySelector("[data-autostop-clear]").onclick = () => setAutoStop(s.id, null);
    el.querySelectorAll("[data-quick]").forEach((b) => {
      b.onclick = () => quickTx(s.id, parseFloat(b.dataset.quick));
    });
  }
  el.querySelector("[data-send]").onclick = () => {
    const action = el.querySelector("[data-action]").value.trim();
    if (!action) return toast("Enter an action", true);
    let payload = {};
    try { payload = JSON.parse(el.querySelector("[data-payload]").value || "{}"); }
    catch { return toast("Payload is not valid JSON", true); }
    exec(s.id, action, payload);
  };
  el.querySelector("[data-frames-clear]").onclick = () => {
    framesClearedAt[s.id] = lastFrameAt[s.id] || new Date().toISOString();
    el.querySelector("[data-frames]").textContent = "(cleared)";
  };
  return el;
}

// What would force a rebuild of the station page: anything that changes which
// controls exist. Everything else is updated in place so inputs keep focus
// and half-typed values.
function stationShape(s) {
  return [s.id, s.numConnectors, s.isTestCharger ? "test" : "plain", s.managed ? "managed" : "external"].join("|");
}

function renderMain() {
  const overview = document.getElementById("overview");
  const station = document.getElementById("station");
  if (isOverview()) {
    overview.style.display = "contents";
    station.style.display = "none";
    renderOverview();
    refreshTxLog();
    return;
  }
  overview.style.display = "none";
  station.style.display = "contents";
  const s = simsById[selected];
  if (!s) {
    // Not loaded yet (first paint before /api/sims answered) or gone.
    station.innerHTML = \`<div class="empty">Loading \${escapeHtml(selected)}…</div>\`;
    stationView = null;
    return;
  }
  updateStation(s);
  updateStationScheduler();
  refreshFrames(); refreshLogs();
}

// Build the page for a station the first time (or when its shape changes),
// then update every live value in place from the /api/sims poll.
function updateStation(s) {
  const station = document.getElementById("station");
  const shape = stationShape(s);
  if (!stationView || stationView.shape !== shape) {
    stationView = { id: s.id, shape, el: buildStation(s) };
    station.replaceChildren(stationView.el);
  }
  const el = stationView.el;
  el.querySelector(".dot").className = "dot " + (s.up ? "up" : "down");
  connectedBadge(el.querySelector("[data-conn-badge]"), s);
  const toggle = el.querySelector("[data-connect-toggle]");
  toggle.textContent = s.offline || !s.connected ? "⇅ Connect" : "⇅ Disconnect";
  toggle.disabled = !s.up;
  const proc = el.querySelector("[data-proc-badge]");
  proc.textContent = s.up ? "process up" + (s.pid ? " · pid " + s.pid : "") : "process down";
  proc.className = "badge " + (s.up ? "badge-info" : "badge-off");
  el.querySelector("[data-sub]").textContent = subLine(s);
  liveTx[s.id] = s.charging || [];
  for (const block of el.querySelectorAll("[data-connector]")) {
    const tx = txOn(s.id, Number(block.dataset.connector));
    const badge = block.querySelector("[data-conn-status]");
    badge.textContent = !s.up ? "offline" : tx ? \`txn #\${tx.transactionId} · \${tx.idTag} · \${tx.meterWh} Wh\` : "no transaction";
    badge.className = "badge " + (tx ? "badge-charging" : "badge-available");
  }
  const delays = s.delays;
  const replyInput = el.querySelector("[data-reply-delay]");
  const actInput = el.querySelector("[data-act-delay]");
  if (delays) {
    if (document.activeElement !== replyInput) replyInput.value = delays.replyMs;
    if (document.activeElement !== actInput) actInput.value = delays.actMs;
    el.querySelector("[data-reply-hint]").textContent = delays.replyMs === 0
      ? "every CALLRESULT goes out at once"
      : \`every CALLRESULT is held \${delays.replyMs} ms; Spark gives up after COSMOS_TIMEOUT (8 s default, 10 s in dev) and the late answer is dropped\`;
    el.querySelector("[data-act-hint]").textContent =
      \`Authorize, StartTransaction/StopTransaction and the status reports follow \${delays.actMs} ms after the command arrives, independent of when it is answered\`;
    el.querySelectorAll("[data-reply-preset]").forEach((b) => { b.className = Number(b.dataset.replyPreset) === delays.replyMs ? "delayon" : ""; });
    el.querySelectorAll("[data-act-preset]").forEach((b) => { b.className = Number(b.dataset.actPreset) === delays.actMs ? "delayon" : ""; });
  }
  const meter = s.meter;
  const autoMeter = el.querySelector("[data-auto-meter]");
  if (meter && autoMeter) {
    autoMeter.checked = !!meter.auto;
    el.querySelector("[data-meter-hint]").textContent =
      \`(every \${Math.round(meter.intervalMs / 1000)} s\${meter.kw != null ? " at " + meter.kw + " kW" : ""})\`;
  }
  const fs = el.querySelector("[data-fail-state]");
  const failBadge = el.querySelector("[data-fail-badge]");
  if (fs) {
    const fm = s.failMode;
    if (fm && fm.mode && fm.mode !== "off") {
      const secs = fm.expiresInMs != null ? Math.ceil(fm.expiresInMs / 1000) + "s" : "until cleared";
      fs.textContent = \`⚠ \${fm.mode} (\${secs})\`;
      fs.className = "failstate active";
      failBadge.textContent = \`RemoteStart \${fm.mode} (\${secs})\`;
      failBadge.style.display = "";
    } else {
      fs.textContent = "off";
      fs.className = "failstate";
      failBadge.style.display = "none";
    }
  }
  const ft = el.querySelector("[data-fault-faulted]");
  const ht = el.querySelector("[data-fault-hightemp]");
  if (ft && ht) {
    // faults is keyed by connectorId; these test chargers use connector 1
    const f = (s.faults && s.faults["1"]) || { faulted: false, highTemperature: false };
    ft.dataset.on = String(!!f.faulted);
    ft.textContent = "Faulted: " + (f.faulted ? "ON" : "off");
    ft.className = f.faulted ? "faulton" : "";
    ht.dataset.on = String(!!f.highTemperature);
    ht.textContent = "High temp: " + (f.highTemperature ? "ON" : "off");
    ht.className = f.highTemperature ? "faulton" : "";
    const faultBadge = el.querySelector("[data-fault-badge]");
    const active = [f.faulted ? "Faulted" : null, f.highTemperature ? "HighTemperature" : null].filter(Boolean);
    faultBadge.textContent = active.join(" · ");
    faultBadge.style.display = active.length ? "" : "none";
  }
  const ss = el.querySelector("[data-speed-state]");
  if (ss) {
    const cp = s.chargingPower || {};
    const kw = cp.kw == null ? null : cp.kw;
    const iv = cp.intervalMs == null ? 15000 : cp.intervalMs;
    const fast = kw === 180 && iv === 5000;
    ss.textContent = fast ? "0.25 kWh / 5s" : kw == null ? "default rate" : kw + " kW";
    el.querySelectorAll("[data-speed]").forEach((b) => {
      const sp = b.dataset.speed;
      const on = sp === "fast" ? fast
        : sp === "default" ? (kw == null && !fast)
        : (parseFloat(sp) === kw && !fast);
      if (sp === "0") b.className = on ? "zeroon" : "zerohold";
      else b.className = on ? "speedon" : "";
    });
  }
  const as = el.querySelector("[data-autostop-state]");
  if (as) {
    const tgt = s.chargeTarget ? s.chargeTarget.kwh : null;
    as.textContent = tgt == null ? "off" : \`⏹ armed: stops at \${tgt.toFixed(2)} kWh\`;
    as.className = tgt == null ? "autostopstate" : "autostopstate active";
    const ab = el.querySelector("[data-autostop]");
    if (ab) ab.className = tgt != null && tgt === parseFloat(ab.dataset.autostop) ? "autostopon" : "";
  }
  const w = s.wsUrlState;
  if (w && w.url) {
    // Reflect the live connect URL (may differ from the .env baseline when an
    // override is active).
    el.querySelector("[data-wsurl]").textContent = w.fullUrl || (w.url + "/" + w.cpId);
  }
  const us = el.querySelector("[data-wsurl-state]");
  if (us && w && w.url) {
    const overridden = w.source === "override";
    us.textContent = overridden
      ? \`⚠ override active — \${w.url} (.env baseline: \${w.envUrl || "?"})\`
      : \`connected to \${w.url} (from .env)\`;
    us.className = overridden ? "urlstate active" : "urlstate";
    // Prefill the input with the current base URL, but never clobber typing.
    const ui = el.querySelector("[data-wsurl-input]");
    if (ui && document.activeElement !== ui && !ui.value) ui.value = w.url;
  }
}

// The scheduler's view of each connector of the shown station: mode label,
// the running session with its elapsed time, and the unreachable alert.
function updateStationScheduler() {
  if (!stationVisible()) return;
  for (const block of stationView.el.querySelectorAll("[data-connector]")) {
    const st = schedOn(stationView.id, Number(block.dataset.connector));
    const line = block.querySelector("[data-tx-line]");
    const alertEl = block.querySelector("[data-alert]");
    block.querySelector("[data-mode-label]").textContent = schedLabel(st);
    if (!st) { line.textContent = "Idle"; alertEl.style.display = "none"; continue; }
    line.textContent = st.tx
      ? \`Running: \${st.tx.idTag} (\${st.tx.label}) · txn #\${st.tx.transactionId} · \${st.tx.elapsedSeconds}s · \${st.tx.source}\`
      : st.pauseReason === "batch_complete"
        ? \`Idle · batch of \${txSessionLimit} sessions complete — Resume auto on the Overview starts another batch\`
        : "Idle";
    if (st.consecutiveFailures >= txFailureThreshold) {
      alertEl.style.display = "";
      alertEl.textContent = \`⚠ UNREACHABLE — \${st.consecutiveFailures} failed start attempts in a row (admin API down?)\`;
    } else {
      alertEl.style.display = "none";
    }
  }
}

// --- periodic refreshes -----------------------------------------------------
// Each one renders only what is visible and updates text in place.

async function refreshSims() {
  const d = await (await fetch("/api/sims")).json();
  const connected = d.sims.filter((s) => s.connected).length;
  const meta = document.getElementById("meta");
  meta.textContent = d.sims.length ? \`\${connected}/\${d.sims.length} connected\` : "no stations";
  meta.className = "badge " + (!d.sims.length ? "badge-available" : connected === d.sims.length ? "badge-on" : connected ? "badge-warn" : "badge-off");
  if (d.defaults) {
    document.querySelector("#add-form [data-wsurl]").placeholder = d.defaults.wsUrl;
    document.getElementById("add-dirs").textContent =
      \`profiles: \${d.defaults.profilesDir} · logs: \${d.defaults.logDir}\`;
  }
  const listed = new Set(d.sims.map((s) => s.id));
  for (const id of Object.keys(simsById)) {
    if (listed.has(id)) continue;
    delete simsById[id];
    delete liveTx[id];
    delete framesClearedAt[id];
    delete lastFrameAt[id];
  }
  simOrder = d.sims.map((s) => s.id);
  for (const s of d.sims) { simsById[s.id] = s; liveTx[s.id] = s.charging || []; }
  renderNav();
  if (!isOverview() && !simsById[selected]) {
    // The selected station is gone: back to the overview.
    stationView = null;
    select("overview");
    return;
  }
  if (isOverview()) renderOverview();
  else { updateStation(simsById[selected]); updateStationScheduler(); }
}

async function refreshFrames() {
  if (!stationVisible()) return;
  const id = stationView.id;
  try {
    const d = await (await fetch(\`/api/sims/\${id}/frames?limit=200\`)).json();
    if (!stationVisible() || stationView.id !== id) return;
    const pre = stationView.el.querySelector("[data-frames]");
    const atBottom = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 20;
    const frames = d.frames || [];
    if (frames.length) lastFrameAt[id] = frames[frames.length - 1].at;
    const since = framesClearedAt[id];
    const shown = since ? frames.filter((f) => f.at > since) : frames;
    pre.innerHTML = shown.map((f) =>
      \`<span class="\${f.direction === "in" ? "fin" : "fout"}">\${escapeHtml(f.at.slice(11, 23))} \${f.direction === "in" ? "<-" : "->"} \${escapeHtml(f.text)}</span>\`
    ).join("\\n") || (since ? "(cleared)" : "(no frames yet)");
    if (atBottom) pre.scrollTop = pre.scrollHeight;
    stationView.el.querySelector("[data-reply]").textContent =
      d.lastReply ? JSON.stringify(d.lastReply, null, 2) : "none yet";
  } catch {}
}

async function refreshLogs() {
  if (!stationVisible()) return;
  const id = stationView.id;
  try {
    const d = await (await fetch(\`/api/sims/\${id}/logs?lines=120\`)).json();
    if (!stationVisible() || stationView.id !== id) return;
    const pre = stationView.el.querySelector("[data-log]");
    const atBottom = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 20;
    pre.textContent = d.log || "(no log yet)";
    if (atBottom) pre.scrollTop = pre.scrollHeight;
  } catch {}
}

async function refreshTxState() {
  const d = await (await fetch("/api/tx/state")).json();
  txSessionLimit = d.txSettings.sessionLimit;
  txFailureThreshold = d.txSettings.failureAlertThreshold;
  document.getElementById("tx-meta").textContent =
    \`scheduler: \${d.freeKeys}/\${d.totalKeys} keys free · auto sessions \${d.txSettings.minMinutes}-\${d.txSettings.maxMinutes}min, \${d.txSettings.gapSeconds}s gap\` +
    (txSessionLimit > 0 ? \` · batches of \${txSessionLimit}\` : "");
  for (const k of Object.keys(schedStates)) delete schedStates[k];
  for (const st of d.states) schedStates[st.simId + ":" + st.connectorId] = st;
  if (isOverview()) renderOverviewScheduler();
  else updateStationScheduler();
}

async function refreshTxLog() {
  if (!isOverview()) return;
  const d = await (await fetch("/api/tx/log?lines=200")).json();
  document.getElementById("tx-log-body").innerHTML = d.rows.map((r) => \`<tr>
    <td>\${escapeHtml(r.started_at)}</td><td>\${escapeHtml(r.session_seconds)}s</td>
    <td>\${escapeHtml(r.sim)}#\${escapeHtml(r.connector_id)}</td>
    <td>\${escapeHtml(r.id_tag)} (\${escapeHtml(r.label)})</td>
    <td>\${escapeHtml(r.transaction_id)}</td><td>\${escapeHtml(r.kwh)}</td>
    <td>\${escapeHtml(r.source)}</td><td>\${escapeHtml(r.status)}</td>
  </tr>\`).join("");
}

// --- add station form, charging keys ---------------------------------------

function showAddError(msg) {
  const el = document.getElementById("add-error");
  el.textContent = msg || "";
  el.style.display = msg ? "" : "none";
}

document.getElementById("add-form").onsubmit = async (ev) => {
  ev.preventDefault();
  const form = ev.target;
  const cpId = form.querySelector("[data-cpid]").value.trim();
  const connectors = Number(form.querySelector("[data-connectors]").value || 1);
  const wsUrl = form.querySelector("[data-wsurl]").value.trim();
  if (!cpId) return showAddError("Enter a station id");
  const body = { cpId, connectors };
  if (wsUrl) body.wsUrl = wsUrl;
  const submit = document.getElementById("add-submit");
  submit.disabled = true;
  showAddError("");
  try {
    const r = await fetch("/api/sims", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body),
    });
    const d = await r.json();
    if (!r.ok) return showAddError(d.error || ("HTTP " + r.status));
    toast(\`\${d.sim.cpId}: added as \${d.sim.id} (admin :\${d.sim.adminPort}), connecting…\`);
    form.querySelector("[data-cpid]").value = "";
    form.querySelector("[data-connectors]").value = "1";
    form.querySelector("[data-wsurl]").value = "";
    selected = d.sim.id; // show the new station once /api/sims lists it
    try { localStorage.setItem(SELECTED_KEY, selected); } catch {}
    await refreshSims();
    renderNavSelection(); renderMain();
    refreshTxState();
  } catch (e) {
    showAddError("Request failed: " + e);
  } finally {
    submit.disabled = false;
  }
};

async function loadKeysIntoEditor() {
  const d = await (await fetch("/api/tx/keys")).json();
  document.getElementById("keys-text").value = d.text;
}

document.getElementById("keys-save").onclick = async () => {
  const text = document.getElementById("keys-text").value;
  const r = await fetch("/api/tx/keys", {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ text }),
  });
  const d = await r.json();
  if (d.ok) toast(\`Saved \${d.keys.length} key(s)\`); else toast(d.error || "Failed to save keys", true);
  refreshTxState();
};

(async function loop() {
  renderNavSelection();
  renderMain();
  await refreshSims();
  await refreshTxState();
  await refreshFrames();
  await refreshLogs();
  await refreshTxLog();
  await loadKeysIntoEditor();
  setInterval(refreshSims, 3000);
  setInterval(refreshFrames, 2000);
  setInterval(refreshLogs, 3000);
  setInterval(refreshTxState, 3000);
  setInterval(refreshTxLog, 5000);
})();
</script>
</body>
</html>`;
