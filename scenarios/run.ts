// Replay a scenario against a running VCP through its admin HTTP API.
//
// A scenario is a JSON file, either an array of steps or {"name",
// "description", "steps"}, that one station walks through in order. It exists
// so a reproducible charger story ("boot, authorise, charge for a minute,
// stop") can be checked in and replayed against Spark and Cosmos instead of
// living in someone's terminal history. Every step that sends a call records
// the CSMS's reply to that exact call (via POST /execute-sync), `expect`
// compares the last one against a subset of fields, and a mismatch fails the
// run with exit code 1, which is what makes it usable in CI.
//
//   npm run scenario -- scenarios/session.json                 # ADMIN_PORT from .env
//   npm run scenario -- scenarios/remote-start.json --admin http://localhost:9910
//   npm run scenario -- scenarios/boot.json --id-tag AABBCC --json
//
// The VCP owns its own socket, so there is no connect/disconnect step here:
// `connect` only waits for the admin API to answer and `disconnect` is a
// no-op. The fleet scenario runs one station per invocation; point it at each
// admin port of a run_simulators.sh fleet.
require("dotenv").config();

import { readFileSync } from "node:fs";

interface Step {
  step: string;
  // biome-ignore lint/suspicious/noExplicitAny: scenario steps are free-form JSON
  [key: string]: any;
}

interface Scenario {
  name: string;
  description: string;
  steps: Step[];
}

type CallOutcome =
  // biome-ignore lint/suspicious/noExplicitAny: ocpp payload
  | { status: "result"; payload: any }
  | { status: "error"; errorCode: string; errorDescription: string }
  | { status: "timeout" | "not_sent" };

interface StepResult {
  index: number;
  step: string;
  ok: boolean;
  ms: number;
  // biome-ignore lint/suspicious/noExplicitAny: ocpp payload
  reply?: any;
  error?: string;
}

class ScenarioError extends Error {}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function parseArgs(argv: string[]) {
  const options = {
    file: "",
    admin: `http://localhost:${process.env.ADMIN_PORT ?? "9999"}`,
    idTag: null as string | null,
    json: false,
    help: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const argument = argv[i];
    const value = () => {
      const next = argv[++i];
      if (next === undefined) throw new Error(`${argument} needs a value`);
      return next;
    };
    switch (argument) {
      case "--admin":
        options.admin = value().replace(/\/+$/, "");
        break;
      case "--id-tag":
        options.idTag = value();
        break;
      case "--json":
        options.json = true;
        break;
      case "-h":
      case "--help":
        options.help = true;
        break;
      default:
        if (argument.startsWith("--")) {
          throw new Error(`unknown option ${argument} (try --help)`);
        }
        options.file = argument;
    }
  }
  return options;
}

function loadScenario(path: string): Scenario {
  const parsed = JSON.parse(readFileSync(path, "utf8"));
  const steps = Array.isArray(parsed) ? parsed : parsed.steps;
  if (!Array.isArray(steps)) {
    throw new Error(
      `${path} is not a scenario: expected an array of steps, or {"steps": [...]}`,
    );
  }
  return {
    name: Array.isArray(parsed) ? path : (parsed.name ?? path),
    description: Array.isArray(parsed) ? "" : (parsed.description ?? ""),
    steps,
  };
}

/**
 * A subset comparison, so an expectation names only the fields it cares
 * about. Returns the first path that differs, or null when the actual reply
 * satisfies the expectation.
 */
export function compare(
  expected: unknown,
  actual: unknown,
  path = "lastReply",
): string | null {
  if (
    expected === null ||
    typeof expected !== "object" ||
    Array.isArray(expected)
  ) {
    const same = Array.isArray(expected)
      ? JSON.stringify(expected) === JSON.stringify(actual)
      : typeof expected === "number" && typeof actual === "string"
        ? expected === Number(actual)
        : expected === actual;
    return same
      ? null
      : `${path} expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`;
  }
  if (actual === null || typeof actual !== "object") {
    return `${path} expected an object, got ${JSON.stringify(actual)}`;
  }
  for (const [key, value] of Object.entries(expected)) {
    const mismatch = compare(
      value,
      (actual as Record<string, unknown>)[key],
      `${path}.${key}`,
    );
    if (mismatch !== null) return mismatch;
  }
  return null;
}

class Station {
  // biome-ignore lint/suspicious/noExplicitAny: ocpp payload
  lastReply: any = null;
  transaction: { transactionId: number; connectorId: number } | null = null;
  // Incoming CALL frames already handed to a waitForCommand step.
  private consumedFrames = new Set<string>();

  constructor(
    private admin: string,
    private idTagOverride: string | null,
  ) {}

  idTag(step: Step, fallback = "SIMTAG"): string {
    return this.idTagOverride ?? step.idTag ?? fallback;
  }

  private async http(
    method: "GET" | "POST",
    path: string,
    body?: unknown,
    // biome-ignore lint/suspicious/noExplicitAny: admin API JSON
  ): Promise<any> {
    const response = await fetch(`${this.admin}${path}`, {
      method,
      headers: { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await response.text();
    if (!response.ok) {
      throw new ScenarioError(
        `${method} ${path} answered ${response.status}: ${text.slice(0, 200)}`,
      );
    }
    return text ? JSON.parse(text) : null;
  }

  // Send one OCPP call and record what the CSMS answered to it.
  // biome-ignore lint/suspicious/noExplicitAny: ocpp payload
  async call(action: string, payload: unknown): Promise<any> {
    const outcome = (await this.http("POST", "/execute-sync", {
      action,
      payload,
      timeoutMs: 30_000,
    })) as CallOutcome;
    if (outcome.status === "result") {
      this.lastReply = outcome.payload;
      return outcome.payload;
    }
    if (outcome.status === "error") {
      this.lastReply = {
        errorCode: outcome.errorCode,
        errorDescription: outcome.errorDescription,
      };
      throw new ScenarioError(
        `${action} was answered CALLERROR ${outcome.errorCode}: ${outcome.errorDescription}`,
      );
    }
    throw new ScenarioError(`${action} was not answered (${outcome.status})`);
  }

  async waitForHealth(timeoutSeconds: number): Promise<void> {
    const deadline = Date.now() + timeoutSeconds * 1000;
    while (Date.now() < deadline) {
      try {
        const health = await this.http("GET", "/health");
        if (health?.connected !== false) return;
      } catch {
        // not up yet
      }
      await sleep(500);
    }
    throw new ScenarioError(
      `the VCP's admin API at ${this.admin} did not answer within ${timeoutSeconds}s`,
    );
  }

  // biome-ignore lint/suspicious/noExplicitAny: admin API JSON
  async transactions(): Promise<any[]> {
    return (await this.http("GET", "/transactions")) ?? [];
  }

  // The register the VCP holds for the transaction, in Wh.
  async meterWh(): Promise<number> {
    const transactionId = this.transaction?.transactionId;
    const transaction = (await this.transactions()).find(
      (candidate) => candidate.transactionId === transactionId,
    );
    return Math.round(transaction?.meterWh ?? 0);
  }

  async setPowerW(powerW: number): Promise<void> {
    const current = await this.http("GET", "/charging-power");
    await this.http("POST", "/charging-power", {
      kw: powerW / 1000,
      intervalMs: current?.intervalMs ?? null,
    });
  }

  // Wait for the next incoming CALL of this action that no earlier step has
  // consumed. Frames are polled from the admin API, so a command that landed
  // while the scenario was in a `wait` is not lost.
  // biome-ignore lint/suspicious/noExplicitAny: ocpp payload
  async waitForCommand(action: string, timeoutSeconds: number): Promise<any> {
    const deadline = Date.now() + timeoutSeconds * 1000;
    while (Date.now() < deadline) {
      const { frames } = await this.http("GET", "/frames?limit=500");
      for (const frame of frames as {
        at: string;
        direction: string;
        text: string;
      }[]) {
        if (frame.direction !== "in") continue;
        const key = `${frame.at} ${frame.text}`;
        if (this.consumedFrames.has(key)) continue;
        let parsed: unknown;
        try {
          parsed = JSON.parse(frame.text);
        } catch {
          continue;
        }
        if (Array.isArray(parsed) && parsed[0] === 2 && parsed[2] === action) {
          this.consumedFrames.add(key);
          return parsed[3] ?? {};
        }
      }
      await sleep(500);
    }
    throw new ScenarioError(
      `waited ${timeoutSeconds}s for ${action} and it never came`,
    );
  }

  // One MeterValues per interval for `seconds`, driven from here so the number
  // of samples is exactly seconds / interval. The station's own periodic
  // timer is paused for the duration and restored afterwards.
  async meter(step: Step): Promise<void> {
    if (this.transaction === null) {
      throw new ScenarioError("no transaction to meter");
    }
    if (step.powerW !== undefined) await this.setPowerW(Number(step.powerW));
    const seconds = Number(step.seconds ?? 60);
    const meterBefore = await this.http("GET", "/meter");
    const interval = Number(
      step.intervalSeconds ?? (meterBefore?.intervalMs ?? 15_000) / 1000,
    );
    await this.http("POST", "/meter", {
      auto: false,
      intervalSeconds: interval,
    });
    try {
      const samples = Math.floor(seconds / interval);
      for (let sample = 0; sample < samples; sample++) {
        await sleep(interval * 1000);
        await this.http("POST", "/meter-tick", {
          connectorId: this.transaction.connectorId,
        });
      }
      const remainder = seconds - samples * interval;
      if (remainder > 0) await sleep(remainder * 1000);
    } finally {
      await this.http("POST", "/meter", { auto: meterBefore?.auto ?? true });
    }
  }

  async startTransaction(step: Step): Promise<void> {
    const connectorId = Number(step.connectorId ?? 1);
    const reply = await this.call("StartTransaction", {
      connectorId,
      idTag: this.idTag(step),
      meterStart: 0,
      timestamp: new Date().toISOString(),
    });
    const transactionId = Number(reply?.transactionId ?? 0);
    if (transactionId === 0 || reply?.idTagInfo?.status !== "Accepted") {
      // The VCP itself sends StopTransaction(DeAuthorized) for a refused
      // start, so nothing is left open on the station.
      this.transaction = null;
      return;
    }
    this.transaction = { transactionId, connectorId };
  }

  async stopTransaction(step: Step): Promise<void> {
    if (this.transaction === null) {
      throw new ScenarioError("no transaction to stop");
    }
    const payload: Record<string, unknown> = {
      transactionId: this.transaction.transactionId,
      meterStop: await this.meterWh(),
      timestamp: new Date().toISOString(),
      reason: step.reason ?? "Local",
    };
    if (step.idTag) payload.idTag = this.idTag(step);
    await this.call("StopTransaction", payload);
    this.transaction = null;
  }

  async status(connectorId: number, status: string, errorCode = "NoError") {
    return this.call("StatusNotification", { connectorId, errorCode, status });
  }
}

// biome-ignore lint/suspicious/noExplicitAny: ocpp payload
async function runStep(station: Station, step: Step): Promise<any> {
  switch (step.step) {
    case "connect":
      await station.waitForHealth(Number(step.timeoutSeconds ?? 30));
      return null;
    case "disconnect":
      // The VCP process owns its socket; stop the process to disconnect.
      return null;
    case "boot":
      return station.call("BootNotification", {
        chargePointVendor: "Solidstudio",
        chargePointModel: "VirtualChargePoint",
        chargePointSerialNumber: "S001",
        firmwareVersion: "1.0.0",
      });
    case "heartbeat":
      return station.call("Heartbeat", {});
    case "status":
      return station.status(
        Number(step.connectorId ?? 1),
        step.status ?? "Available",
        step.errorCode ?? "NoError",
      );
    case "authorize":
      return station.call("Authorize", { idTag: station.idTag(step) });
    case "startTransaction":
      await station.startTransaction(step);
      return station.lastReply;
    case "expectTransactionId": {
      const transactionId = Number(station.lastReply?.transactionId ?? 0);
      const allocated = step.allocated !== false;
      if (allocated && transactionId === 0) {
        throw new ScenarioError(
          `expected an allocated transactionId, got 0 (${JSON.stringify(station.lastReply)})`,
        );
      }
      if (!allocated && transactionId !== 0) {
        throw new ScenarioError(
          `expected transactionId 0 (a refusal), got ${transactionId}`,
        );
      }
      return station.lastReply;
    }
    case "meterValues":
      await station.meter(step);
      return station.lastReply;
    case "stopTransaction":
      await station.stopTransaction(step);
      return station.lastReply;
    case "session": {
      const connectorId = Number(step.connectorId ?? 1);
      await station.status(connectorId, "Preparing");
      await station.call("Authorize", { idTag: station.idTag(step) });
      await station.startTransaction(step);
      if (station.transaction === null) {
        await station.status(connectorId, "Available");
        return station.lastReply;
      }
      await station.status(connectorId, "Charging");
      await station.meter(step);
      await station.stopTransaction({
        step: "stopTransaction",
        reason: "Local",
      });
      await station.status(connectorId, "Finishing");
      await station.status(connectorId, "Available");
      return station.lastReply;
    }
    case "wait":
      await sleep(Number(step.seconds ?? 1) * 1000);
      return null;
    case "waitForCommand":
      return station.waitForCommand(
        String(step.action),
        Number(step.timeoutSeconds ?? 120),
      );
    case "expect": {
      const mismatch = compare(step.lastReply ?? {}, station.lastReply);
      if (mismatch !== null) {
        throw new ScenarioError(
          `expectation failed: ${mismatch} (last reply was ${JSON.stringify(station.lastReply)})`,
        );
      }
      return station.lastReply;
    }
    default:
      throw new ScenarioError(`unknown scenario step "${step.step}"`);
  }
}

async function main(): Promise<number> {
  const options = parseArgs(process.argv.slice(2));
  if (options.help || !options.file) {
    console.log(
      "usage: npm run scenario -- <scenario.json> [--admin http://localhost:9999] [--id-tag TAG] [--json]",
    );
    return options.help ? 0 : 2;
  }
  const scenario = loadScenario(options.file);
  const station = new Station(options.admin, options.idTag);
  const results: StepResult[] = [];
  let failure: string | null = null;
  const log = (line: string) => {
    if (!options.json) console.log(line);
  };
  log(`scenario ${scenario.name} against ${options.admin}`);
  for (const [index, step] of scenario.steps.entries()) {
    const startedAt = Date.now();
    try {
      const reply = await runStep(station, step);
      results.push({
        index,
        step: step.step,
        ok: true,
        ms: Date.now() - startedAt,
        reply: reply ?? null,
      });
      log(
        `  ok   ${step.step} ${reply == null ? "" : JSON.stringify(reply)}`.trimEnd(),
      );
    } catch (error) {
      failure = error instanceof Error ? error.message : String(error);
      results.push({
        index,
        step: step.step,
        ok: false,
        ms: Date.now() - startedAt,
        error: failure,
      });
      log(`  FAIL ${step.step}: ${failure}`);
      break;
    }
  }
  const summary = {
    scenario: scenario.name,
    admin: options.admin,
    ok: failure === null,
    steps: results,
    error: failure,
  };
  if (options.json) console.log(JSON.stringify(summary, null, 2));
  else log(failure === null ? "passed" : `failed: ${failure}`);
  return failure === null ? 0 : 1;
}

main()
  .then((code) => process.exit(code))
  .catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(2);
  });
