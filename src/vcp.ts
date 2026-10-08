import * as util from "node:util";
import { WebSocket } from "ws";

import { serve, type ServerType } from "@hono/node-server";
import { zValidator } from "@hono/zod-validator";
import { Hono } from "hono";
import { z } from "zod";
import { ChargePointConfiguration } from "./chargePointConfiguration";
import { logger } from "./logger";
import { call } from "./messageFactory";
import type { OcppCall, OcppCallError, OcppCallResult } from "./ocppMessage";
import {
  type OcppMessageHandler,
  resolveMessageHandler,
} from "./ocppMessageHandler";
import { ocppOutbox } from "./ocppOutbox";
import { type OcppVersion, toProtocolVersion } from "./ocppVersion";
import {
  getOcppOutgoingMessages,
  validateOcppIncomingRequest,
  validateOcppIncomingResponse,
  validateOcppOutgoingRequest,
  validateOcppOutgoingResponse,
} from "./schemaValidator";
import { type SocConfig, TransactionManager } from "./transactionManager";
import { countFromEnv, range } from "./utils";
import { heartbeatOcppMessage } from "./v16/messages/heartbeat";
import { close } from "./close";
import {
  clearWsUrlOverride,
  normalizeWsUrl,
  readWsUrlOverride,
  writeWsUrlOverride,
} from "./wsUrlOverride";

interface VCPOptions {
  ocppVersion: OcppVersion;
  endpoint: string;
  chargePointId: string;
  basicAuthPassword?: string;
  adminPort?: number;
  // What the station sends once its socket is open (BootNotification, the
  // connector statuses). Runs on the first connect() and again on every admin
  // /connect after a /disconnect, so the CSMS sees a fresh boot each time.
  boot?: (vcp: VCP) => void;
}

// How long an admin /connect waits for the CSMS to accept the socket before
// it gives up and leaves the station offline.
const CONNECT_TIMEOUT_MS = 15_000;

// Runtime fault injection for RemoteStartTransaction, used to exercise the
// backend's authorization-recapture path. See remoteStartTransaction.ts.
//   off             - normal behaviour (accept + StartTransaction + Charging)
//   ignore          - drop the request: send NO response at all, do not start.
//                     Connector stays Available. Backend must time out & recapture.
//   reject          - respond RemoteStartTransaction.conf = Rejected, do not
//                     start. Connector stays Available. Conformant decline.
//   accept_no_start - respond Accepted but never send StartTransaction and stay
//                     Available. Backend thinks it was accepted but nothing charges.
export type RemoteStartFailMode =
  | "off"
  | "ignore"
  | "reject"
  | "accept_no_start";

const DEFAULT_FAIL_MODE_MS = 60_000;

// How long a charger takes between answering RemoteStart/RemoteStop and doing
// something about it (Authorize, StartTransaction, the status reports;
// StopTransaction). A real charger needs about a second. Settable at runtime
// through the admin /delays endpoint; ACT_DELAY_MS seeds it.
const DEFAULT_ACT_DELAY_MS = 1000;
const MAX_DELAY_MS = 600_000;

// How often the station looks for a simulated battery that just filled up (or
// got room again), so the SuspendedEV report follows within a second instead
// of waiting for the next MeterValues tick.
const SOC_WATCH_MS = 1000;

const CONNECTOR_STATUSES = [
  "Available",
  "Preparing",
  "Charging",
  "SuspendedEVSE",
  "SuspendedEV",
  "Finishing",
  "Reserved",
  "Unavailable",
  "Faulted",
] as const;

const STOP_REASONS = [
  "DeAuthorized",
  "EmergencyStop",
  "EVDisconnected",
  "HardReset",
  "Local",
  "Other",
  "PowerLoss",
  "Reboot",
  "Remote",
  "SoftReset",
  "UnlockCommand",
] as const;

// The last N OCPP frames in and out, kept for the admin /frames endpoint so a
// panel can show the wire without tailing a log file.
const FRAMES_KEPT = 500;

export interface Frame {
  at: string;
  direction: "in" | "out";
  text: string;
}

// The payload of the last CALLRESULT or CALLERROR the CSMS sent for one of
// this station's own calls, with the call it answered.
export type LastReply =
  | {
      kind: "result";
      action: string;
      messageId: string;
      at: string;
      // biome-ignore lint/suspicious/noExplicitAny: ocpp payload
      payload: any;
    }
  | {
      kind: "error";
      action: string | null;
      messageId: string;
      at: string;
      errorCode: string;
      errorDescription: string;
      // biome-ignore lint/suspicious/noExplicitAny: ocpp payload
      errorDetails: any;
    };

const parseDelay = (raw: string | undefined, fallback: number): number => {
  const value = Number.parseInt(raw ?? "");
  return Number.isNaN(value) || value < 0 || value > MAX_DELAY_MS
    ? fallback
    : value;
};

interface LogEntry {
  type: "Application";
  timestamp: string;
  level: string;
  message: string;
  metadata: Record<string, unknown>;
}

type CallOutcome =
  // biome-ignore lint/suspicious/noExplicitAny: ocpp payload
  | { status: "result"; payload: any }
  | { status: "error"; errorCode: string; errorDescription: string }
  | { status: "timeout" };

const CONNECTOR_ACTIONS = [
  "plug_in",
  "authorize",
  "start",
  "stop",
  "suspend",
  "resume",
  "unplug",
] as const;
type ConnectorAction = (typeof CONNECTOR_ACTIONS)[number];

const UNPLUG_MODES = ["auto", "manual"] as const;
export type UnplugMode = (typeof UNPLUG_MODES)[number];

export class VCP {
  private ws?: WebSocket;
  private adminServer?: ServerType;
  private messageHandler: OcppMessageHandler;
  private heartbeatIntervalId?: ReturnType<typeof setInterval>;

  private isFinishing = false;

  // What a connector reports once its transaction closes. "auto": the driver
  // unplugs at once (Available). "manual": the cable stays in (Finishing) until
  // the panel's Unplug, so the CSMS can run idle fees. Runtime-set via /unplug-mode.
  unplugMode: UnplugMode = "auto";

  // Set by the admin /disconnect: the socket is closed on purpose and must stay
  // closed. While it is set neither _onClose nor the error handler hands the
  // station to close() (the auto-restart loop), so nothing reconnects until
  // /connect clears it or /restart replaces the process.
  manuallyOffline = false;

  private postMessageActions: Record<string, () => void | Promise<void>> = {};

  // Waiters for /execute-sync: resolved with the CSMS's actual reply (keyed by
  // the OCPP messageId) so admin callers learn exactly what the CSMS answered
  // to *their* call, instead of guessing from local state afterwards.
  private callWaiters = new Map<string, (outcome: CallOutcome) => void>();
  // StartTransaction calls whose /execute-sync caller gave up waiting. If the
  // .conf still arrives later, nobody owns that transaction -- close it at once
  // rather than let it meter forever as an orphan.
  private abandonedStarts = new Set<string>();

  // RemoteStartTransaction fault injection (runtime-toggled via /fail-mode).
  remoteStartFailMode: RemoteStartFailMode = "off";
  private remoteStartFailUntil = 0;
  private remoteStartFailTimer?: ReturnType<typeof setTimeout>;

  transactionManager = new TransactionManager();

  // OCPP 1.6 configuration keys (GetConfiguration / ChangeConfiguration). The
  // MeterValueSampleInterval entry mirrors transactionManager.meterIntervalMs.
  configuration = new ChargePointConfiguration(
    Math.round(this.transactionManager.meterIntervalMs / 1000),
  );

  // Two independent clocks, both per station and both runtime-settable through
  // the admin /delays endpoint.
  //
  // replyDelayMs holds every CALLRESULT and CALLERROR to a CSMS command (the
  // answer to RemoteStart, RemoteStop, Reset, TriggerMessage, all of them) for
  // that long before it goes on the wire. Nothing else is held: heartbeats,
  // status reports and meter values keep flowing, so the socket stays alive.
  // It reproduces a connected but slow charger. Set above Spark's read
  // timeout (COSMOS_TIMEOUT, 8 s by default, 10 s in the dev env) and Spark
  // reports the command unreachable while this station still accepts it later.
  //
  // actDelayMs is how long the station waits after receiving RemoteStart or
  // RemoteStop before acting on it. The two are independent, so both orders a
  // real charger produces can be reproduced: answer first then act (the
  // usual), or hold the answer and act at once, which is how a
  // StartTransaction reaches the CSMS while the RemoteStart is still open.
  replyDelayMs = parseDelay(process.env.REPLY_DELAY_MS, 0);
  actDelayMs = parseDelay(process.env.ACT_DELAY_MS, DEFAULT_ACT_DELAY_MS);

  private frames: Frame[] = [];
  lastReply: LastReply | null = null;

  // When the current socket opened, and when the CSMS last answered a
  // Heartbeat or BootNotification. Both null until it happens.
  connectedSince: string | null = null;
  lastHeartbeatAt: string | null = null;

  // The last status each connector reported, recorded in send() so every path
  // (remote start, faults, admin commands, a raw /execute) keeps it right.
  private connectorStatus = new Map<
    number,
    { status: string; errorCode: string; at: string }
  >();
  // Connectors this station suspended itself because the battery filled up,
  // so it knows to report Charging again if the battery gets room.
  private suspendedFull = new Set<number>();
  private socWatchTimer?: ReturnType<typeof setInterval>;

  // The status the station last reported for a connector, for a boot that
  // must re-announce what is going on instead of claiming Available mid-session.
  knownStatus(connectorId: number): { status: string; errorCode: string } {
    return (
      this.connectorStatus.get(connectorId) ?? {
        status: "Available",
        errorCode: "NoError",
      }
    );
  }

  getConnectorStates() {
    return range(countFromEnv("CONNECTORS")).map((connectorId) => {
      const known = this.connectorStatus.get(connectorId);
      const transaction = this.transactionManager.onConnector(connectorId);
      const snapshot = transaction
        ? this.transactionManager.snapshot(transaction.transactionId)
        : null;
      const soc = this.transactionManager.getSocConfig(connectorId);
      return {
        connectorId,
        status: known?.status ?? null,
        errorCode: known?.errorCode ?? null,
        statusAt: known?.at ?? null,
        soc: soc
          ? {
              batteryKwh: soc.batteryWh / 1000,
              startPercent: soc.startPercent,
              curve: soc.curve,
            }
          : null,
        transaction:
          transaction && snapshot
            ? {
                transactionId: transaction.transactionId,
                idTag: transaction.idTag,
                startedAt: transaction.startedAt.toISOString(),
                ...snapshot,
              }
            : null,
      };
    });
  }

  // Simulate (or stop simulating) an EV battery on a connector.
  setSoc(connectorId: number, config: SocConfig | null): void {
    this.transactionManager.setSocConfig(connectorId, config);
    logger.info(
      config
        ? `Connector ${connectorId}: simulating a ${config.batteryWh / 1000} kWh battery from ${config.startPercent}%${config.curve ? ", tapering with SoC" : ""}`
        : `Connector ${connectorId}: battery simulation off`,
    );
    this.checkBatteries();
  }

  // A full battery stops drawing: the last reading goes out and the connector
  // reports SuspendedEV, the transaction stays open until it is stopped. A
  // battery that gets room again (made bigger, started lower) resumes.
  private checkBatteries(): void {
    if (this.ws?.readyState !== WebSocket.OPEN) {
      return;
    }
    for (const transaction of Array.from(
      this.transactionManager.transactions.values(),
    )) {
      const { connectorId, transactionId } = transaction;
      if (!this.transactionManager.getSocConfig(connectorId)) {
        continue;
      }
      const snapshot = this.transactionManager.snapshot(transactionId);
      const status = this.connectorStatus.get(connectorId)?.status;
      if (snapshot?.full && status === "Charging") {
        logger.info(
          `Connector ${connectorId}: battery full at ${Math.round(snapshot.meterWh)} Wh, car stopped drawing`,
        );
        this.suspendedFull.add(connectorId);
        this.transactionManager.tick(connectorId);
        this.sendStatus(connectorId, "SuspendedEV");
      } else if (
        snapshot &&
        !snapshot.full &&
        status === "SuspendedEV" &&
        this.suspendedFull.has(connectorId)
      ) {
        logger.info(`Connector ${connectorId}: battery has room again`);
        this.suspendedFull.delete(connectorId);
        this.sendStatus(connectorId, "Charging");
      }
    }
  }

  sendStatus(connectorId: number, status: string, errorCode = "NoError") {
    this.send(call("StatusNotification", { connectorId, errorCode, status }));
  }

  // What a connector status means for the energy flow, applied as it goes out.
  private recordStatus(payload: {
    connectorId?: number;
    status?: string;
    errorCode?: string;
  }): void {
    const { connectorId, status } = payload;
    if (!connectorId || !status) {
      return;
    }
    this.connectorStatus.set(connectorId, {
      status,
      errorCode: payload.errorCode ?? "NoError",
      at: new Date().toISOString(),
    });
    if (status !== "SuspendedEV") {
      this.suspendedFull.delete(connectorId);
    }
    if (status === "Charging") {
      this.transactionManager.setPaused(connectorId, false);
    } else if (status === "SuspendedEV" || status === "SuspendedEVSE") {
      this.transactionManager.setPaused(connectorId, true);
    }
  }

  // The panel's connector buttons. Each one is what a real charger does when
  // a driver plugs in, badges, unplugs..., including the status that follows.
  // Throws (with a message for the caller) when the action does not fit.
  connectorAction(
    connectorId: number,
    action: ConnectorAction,
    options: { idTag?: string; reason?: (typeof STOP_REASONS)[number] } = {},
  ): void {
    const transaction = this.transactionManager.onConnector(connectorId);
    const stop = (reason: (typeof STOP_REASONS)[number]) => {
      if (!transaction) {
        throw new Error(`connector ${connectorId} has no transaction`);
      }
      this.send(
        call("StopTransaction", {
          transactionId: Number(transaction.transactionId),
          idTag: transaction.idTag,
          meterStop: Math.floor(
            this.transactionManager.getMeterValue(transaction.transactionId),
          ),
          reason,
          timestamp: new Date().toISOString(),
        }),
      );
    };
    switch (action) {
      case "plug_in":
        this.sendStatus(connectorId, "Preparing");
        return;
      case "authorize":
        this.send(call("Authorize", { idTag: options.idTag ?? "__TOKEN__" }));
        return;
      case "start":
        if (transaction) {
          throw new Error(
            `connector ${connectorId} already has transaction ${transaction.transactionId}`,
          );
        }
        // The transaction itself starts when the CSMS answers with its id
        // (StartTransaction's resHandler), like a RemoteStart.
        this.send(
          call("StartTransaction", {
            connectorId,
            idTag: options.idTag ?? "__TOKEN__",
            meterStart: 0,
            timestamp: new Date().toISOString(),
          }),
        );
        this.sendStatus(connectorId, "Charging");
        return;
      case "stop":
        stop(options.reason ?? "Local");
        this.sendStatus(connectorId, this.statusAfterStop());
        return;
      case "suspend":
        if (!transaction) {
          throw new Error(`connector ${connectorId} has no transaction`);
        }
        this.sendStatus(connectorId, "SuspendedEV");
        return;
      case "resume": {
        if (!transaction) {
          throw new Error(`connector ${connectorId} has no transaction`);
        }
        if (this.transactionManager.snapshot(transaction.transactionId)?.full) {
          throw new Error(
            `connector ${connectorId}: the battery is full, the car draws nothing`,
          );
        }
        this.sendStatus(connectorId, "Charging");
        return;
      }
      case "unplug":
        if (transaction) {
          stop(options.reason ?? "EVDisconnected");
        }
        this.sendStatus(connectorId, "Available");
        return;
    }
  }

  setUnplugMode(mode: UnplugMode): void {
    this.unplugMode = mode;
    logger.info(
      mode === "manual"
        ? "After a stop the cable stays in (Finishing) until Unplug"
        : "After a stop the driver unplugs at once (Available)",
    );
  }

  // The OCPP 1.6 status a connector reports right after its transaction closes.
  statusAfterStop(): "Available" | "Finishing" {
    return this.unplugMode === "manual" ? "Finishing" : "Available";
  }

  setDelays(delays: { replyMs?: number; actMs?: number }): void {
    if (delays.replyMs !== undefined) {
      this.replyDelayMs = delays.replyMs;
      logger.info(
        this.replyDelayMs === 0
          ? "Answering CSMS commands at once"
          : `Holding every answer to a CSMS command for ${this.replyDelayMs} ms`,
      );
    }
    if (delays.actMs !== undefined) {
      this.actDelayMs = delays.actMs;
      logger.info(
        `Acting on RemoteStart/RemoteStop ${this.actDelayMs} ms after they arrive`,
      );
    }
  }

  getDelayState() {
    return { replyMs: this.replyDelayMs, actMs: this.actDelayMs };
  }

  private recordFrame(direction: "in" | "out", text: string): void {
    this.frames.push({ at: new Date().toISOString(), direction, text });
    if (this.frames.length > FRAMES_KEPT) {
      this.frames.splice(0, this.frames.length - FRAMES_KEPT);
    }
  }

  getFrames(limit = FRAMES_KEPT): Frame[] {
    return this.frames.slice(-Math.max(1, Math.min(limit, FRAMES_KEPT)));
  }

  getHealthState() {
    return {
      status: "OK",
      cpId: this.vcpOptions.chargePointId,
      connected: this.ws?.readyState === WebSocket.OPEN,
      offline: this.manuallyOffline,
      delays: this.getDelayState(),
      unplugMode: this.unplugMode,
      meter: this.getMeterState(),
      transactions: this.transactionManager.transactions.size,
      connectedSince: this.connectedSince,
      lastHeartbeatAt: this.lastHeartbeatAt,
      connectors: this.getConnectorStates(),
    };
  }

  getMeterState() {
    return {
      auto: this.transactionManager.autoMeterValues,
      intervalMs: this.transactionManager.meterIntervalMs,
      kw:
        this.transactionManager.chargingPowerW == null
          ? null
          : this.transactionManager.chargingPowerW / 1000,
    };
  }

  // Keep the configuration key and the timer in step whichever side changes.
  setMeterIntervalSeconds(seconds: number): void {
    this.transactionManager.setMeterIntervalMs(seconds * 1000);
    this.configuration.set("MeterValueSampleInterval", String(seconds));
  }

  // Set/clear the RemoteStartTransaction fail mode. A positive durationMs
  // (default 60s) auto-clears back to "off" so a forgotten toggle can't wedge
  // the charger permanently; durationMs <= 0 means "until explicitly cleared".
  setRemoteStartFailMode(mode: RemoteStartFailMode, durationMs?: number): void {
    if (this.remoteStartFailTimer) {
      clearTimeout(this.remoteStartFailTimer);
      this.remoteStartFailTimer = undefined;
    }
    this.remoteStartFailMode = mode;
    if (mode === "off") {
      this.remoteStartFailUntil = 0;
      logger.info("RemoteStart fail mode cleared (off)");
      return;
    }
    const ttl = durationMs ?? DEFAULT_FAIL_MODE_MS;
    if (ttl > 0) {
      this.remoteStartFailUntil = Date.now() + ttl;
      this.remoteStartFailTimer = setTimeout(() => {
        logger.info(`RemoteStart fail mode "${mode}" expired -> off`);
        this.remoteStartFailMode = "off";
        this.remoteStartFailUntil = 0;
        this.remoteStartFailTimer = undefined;
      }, ttl);
    } else {
      this.remoteStartFailUntil = 0; // sticky until cleared
    }
    const ttlLabel =
      ttl > 0
        ? ` for ${ttl >= 1000 ? `${Math.round(ttl / 1000)}s` : `${ttl}ms`}`
        : " (until cleared)";
    logger.info(`RemoteStart fail mode set to "${mode}"${ttlLabel}`);
  }

  getRemoteStartFailState() {
    return {
      mode: this.remoteStartFailMode,
      expiresInMs:
        this.remoteStartFailUntil > 0
          ? Math.max(0, this.remoteStartFailUntil - Date.now())
          : null,
    };
  }

  // Connector fault injection (runtime-toggled via /fault). Used to exercise the
  // backend's fault-email logic. Mutually exclusive per connector: asserting one
  // condition clears the other, since a connector reports a single
  // StatusNotification errorCode at a time.
  //   faulted          -> status=Faulted, errorCode=OtherError
  //   high_temperature -> status=Faulted, errorCode=HighTemperature
  //   (both off)       -> status=Available, errorCode=NoError
  private connectorFaults: Map<
    number,
    { faulted: boolean; highTemperature: boolean }
  > = new Map();

  setConnectorFault(
    connectorId: number,
    type: "faulted" | "high_temperature",
    on: boolean,
  ): void {
    const state = this.connectorFaults.get(connectorId) ?? {
      faulted: false,
      highTemperature: false,
    };
    if (on) {
      // mutually exclusive: asserting one condition clears the other
      state.faulted = type === "faulted";
      state.highTemperature = type === "high_temperature";
    } else if (type === "faulted") {
      state.faulted = false;
    } else {
      state.highTemperature = false;
    }
    this.connectorFaults.set(connectorId, state);
    this.emitConnectorStatus(connectorId, state);
  }

  private emitConnectorStatus(
    connectorId: number,
    state: { faulted: boolean; highTemperature: boolean },
  ): void {
    let status = "Available";
    let errorCode = "NoError";
    if (state.faulted) {
      status = "Faulted";
      errorCode = "OtherError";
    } else if (state.highTemperature) {
      status = "Faulted";
      errorCode = "HighTemperature";
    }
    logger.info(
      `Connector ${connectorId} fault -> status=${status} errorCode=${errorCode}`,
    );
    try {
      this.send(call("StatusNotification", { connectorId, errorCode, status }));
    } catch (err) {
      // ws may be mid-reconnect; state is still recorded and GET /fault reflects it
      logger.warn(`Could not emit StatusNotification for fault toggle: ${err}`);
    }
  }

  getConnectorFaultState() {
    const out: Record<number, { faulted: boolean; highTemperature: boolean }> =
      {};
    for (const [connectorId, state] of Array.from(this.connectorFaults)) {
      out[connectorId] = { ...state };
    }
    return out;
  }

  // Auto-stop energy target (Wh). When set, the charger stops ITSELF the moment
  // an active session's energy register reaches this value -- regardless of who
  // started the session (e.g. the mobile app via RemoteStart) -- and records
  // meterStop at exactly this target, so the session lands on a precise energy
  // (e.g. 1.000 kWh) instead of overshooting. null = disarmed. Sticky: stays
  // armed across sessions until cleared. Enforced in the MeterValues tick
  // (src/v16/messages/startTransaction.ts). Runtime-toggled via /charge-target.
  chargeTargetWh: number | null = null;

  setChargeTargetKwh(kwh: number | null): void {
    this.chargeTargetWh = kwh == null ? null : Math.round(kwh * 1000);
    logger.info(
      `Charge auto-stop target ${this.chargeTargetWh == null ? "cleared" : `set to ${this.chargeTargetWh} Wh (${kwh} kWh)`}`,
    );
  }

  getChargeTargetState() {
    return {
      kwh: this.chargeTargetWh == null ? null : this.chargeTargetWh / 1000,
    };
  }

  // Charging speed (kW) -> controls how fast the energy register climbs and thus
  // the MeterValues readings sent to the CSMS. null restores the legacy fixed
  // rate. Runtime-toggled via /charging-power.
  setChargingPowerKw(kw: number | null): void {
    const watts = kw == null ? null : Math.round(kw * 1000);
    this.transactionManager.setChargingPowerW(watts);
    logger.info(
      `Charging power set to ${kw == null ? "default (legacy rate)" : `${kw} kW`}`,
    );
  }

  getChargingPowerState() {
    const w = this.transactionManager.chargingPowerW;
    return {
      kw: w == null ? null : w / 1000,
      fluctuate: this.transactionManager.fluctuate,
      intervalMs: this.transactionManager.meterIntervalMs,
    };
  }

  // OCPP endpoint state: the base URL this VCP is currently connected to, plus
  // the .env baseline and any persisted override. `url` is the bare host; the
  // full connect URL appends "/<CP_ID>". Runtime-changed via /ws-url.
  getWsUrlState() {
    const cpId = this.vcpOptions.chargePointId;
    const override = readWsUrlOverride(cpId);
    return {
      cpId,
      url: this.vcpOptions.endpoint,
      fullUrl: `${this.vcpOptions.endpoint}/${cpId}`,
      envUrl: process.env.WS_URL ?? null,
      override,
      source: override ? "override" : "env",
    };
  }

  constructor(private vcpOptions: VCPOptions) {
    this.messageHandler = resolveMessageHandler(vcpOptions.ocppVersion);
    if (vcpOptions.adminPort) {
      const adminApi = new Hono();
      adminApi.get("/health", (c) => c.json(this.getHealthState()));
      // Every connector: its last reported status, simulated battery and live
      // transaction reading.
      adminApi.get("/connectors", (c) => c.json(this.getConnectorStates()));
      // Simulate an EV battery on a connector: SoC goes into MeterValues and
      // the car stops drawing at 100 % (SuspendedEV). enabled=false clears it.
      adminApi.post(
        "/soc",
        zValidator(
          "json",
          z.object({
            connectorId: z.number().int().positive().default(1),
            enabled: z.boolean(),
            batteryKwh: z.number().positive().max(1000).default(60),
            startPercent: z.number().min(0).max(100).default(20),
            // Taper the power as the battery fills (see chargeCurve).
            curve: z.boolean().default(false),
          }),
        ),
        (c) => {
          const { connectorId, enabled, batteryKwh, startPercent, curve } =
            c.req.valid("json");
          this.setSoc(
            connectorId,
            enabled
              ? { batteryWh: batteryKwh * 1000, startPercent, curve }
              : null,
          );
          return c.json(this.getConnectorStates());
        },
      );
      // Driver-side actions on one connector (plug in, badge, start, stop,
      // suspend, resume, unplug), each with the status reports that follow.
      adminApi.post(
        "/connector-action",
        zValidator(
          "json",
          z.object({
            connectorId: z.number().int().positive().default(1),
            action: z.enum(CONNECTOR_ACTIONS),
            idTag: z.string().min(1).max(36).optional(),
            reason: z.enum(STOP_REASONS).optional(),
          }),
        ),
        (c) => {
          const { connectorId, action, idTag, reason } = c.req.valid("json");
          try {
            this.connectorAction(connectorId, action, { idTag, reason });
          } catch (err) {
            return c.json(
              { error: err instanceof Error ? err.message : String(err) },
              409,
            );
          }
          return c.json(this.getConnectorStates());
        },
      );
      // The two delay clocks. Either field may be omitted to leave it alone.
      adminApi.get("/delays", (c) => c.json(this.getDelayState()));
      adminApi.post(
        "/delays",
        zValidator(
          "json",
          z.object({
            replyMs: z.number().int().min(0).max(MAX_DELAY_MS).optional(),
            actMs: z.number().int().min(0).max(MAX_DELAY_MS).optional(),
          }),
        ),
        (c) => {
          this.setDelays(c.req.valid("json"));
          return c.json(this.getDelayState());
        },
      );
      // Auto or manual unplug after a stop (see unplugMode).
      adminApi.get("/unplug-mode", (c) => c.json({ mode: this.unplugMode }));
      adminApi.post(
        "/unplug-mode",
        zValidator("json", z.object({ mode: z.enum(UNPLUG_MODES) })),
        (c) => {
          this.setUnplugMode(c.req.valid("json").mode);
          return c.json({ mode: this.unplugMode });
        },
      );
      // The last OCPP frames both ways, plus the CSMS's last reply to one of
      // this station's own calls.
      adminApi.get("/frames", (c) => {
        const limit = Number.parseInt(c.req.query("limit") ?? "");
        return c.json({
          frames: this.getFrames(Number.isNaN(limit) ? undefined : limit),
          lastReply: this.lastReply,
        });
      });
      // Periodic MeterValues on/off for every transaction, and the cadence in
      // seconds (the MeterValueSampleInterval configuration key).
      adminApi.get("/meter", (c) => c.json(this.getMeterState()));
      adminApi.post(
        "/meter",
        zValidator(
          "json",
          z.object({
            auto: z.boolean().optional(),
            intervalSeconds: z.number().positive().optional(),
          }),
        ),
        (c) => {
          const { auto, intervalSeconds } = c.req.valid("json");
          if (intervalSeconds !== undefined) {
            this.setMeterIntervalSeconds(intervalSeconds);
          }
          if (auto !== undefined) {
            this.transactionManager.setAutoMeterValues(auto);
          }
          return c.json(this.getMeterState());
        },
      );
      // One MeterValues now for the transaction on a connector: the periodic
      // timer's tick on demand, with the register as it stands.
      adminApi.post(
        "/meter-tick",
        zValidator(
          "json",
          z.object({ connectorId: z.number().int().positive().default(1) }),
        ),
        (c) => {
          const { connectorId } = c.req.valid("json");
          if (!this.transactionManager.tick(connectorId)) {
            return c.json(
              { error: `connector ${connectorId} has no transaction` },
              409,
            );
          }
          return c.json({ ok: true, connectorId });
        },
      );
      adminApi.post(
        "/execute",
        zValidator(
          "json",
          z.object({
            action: z.string(),
            payload: z.any(),
          }),
        ),
        (c) => {
          const validated = c.req.valid("json");
          try {
            this.send(call(validated.action, validated.payload));
          } catch (error) {
            logger.error(
              `Admin command ${validated.action} not sent: ${String(error)}`,
            );
            return c.text(`Not sent: ${String(error)}`, 503);
          }
          return c.text("OK");
        },
      );
      // Send an OCPP call and wait for the CSMS's reply to that exact
      // messageId. Returns {messageId, status: "result"|"error"|"timeout", ...}.
      adminApi.post(
        "/execute-sync",
        zValidator(
          "json",
          z.object({
            action: z.string(),
            payload: z.any(),
            timeoutMs: z.number().int().positive().max(120_000).optional(),
          }),
        ),
        async (c) => {
          const { action, payload, timeoutMs = 15_000 } = c.req.valid("json");
          const ocppCall = call(action, payload);
          const outcome = new Promise<CallOutcome>((resolve) => {
            this.callWaiters.set(ocppCall.messageId, resolve);
          });
          try {
            this.send(ocppCall);
          } catch (err) {
            this.callWaiters.delete(ocppCall.messageId);
            return c.json(
              {
                messageId: ocppCall.messageId,
                status: "not_sent",
                error: String(err),
              },
              503,
            );
          }
          let timer: ReturnType<typeof setTimeout> | undefined;
          const timedOut = new Promise<CallOutcome>((resolve) => {
            timer = setTimeout(() => resolve({ status: "timeout" }), timeoutMs);
          });
          const result = await Promise.race([outcome, timedOut]);
          clearTimeout(timer);
          if (result.status === "timeout") {
            this.callWaiters.delete(ocppCall.messageId);
            if (action === "StartTransaction")
              this.abandonedStarts.add(ocppCall.messageId);
          }
          return c.json({ messageId: ocppCall.messageId, ...result });
        },
      );
      adminApi.get("/transactions", (c) => {
        const transactions = Array.from(
          this.transactionManager.transactions.values(),
        ).map(({ meterValuesTimer, meterValuesCallback, ...transaction }) => ({
          ...transaction,
          meterWh: this.transactionManager.getMeterValue(
            transaction.transactionId,
          ),
        }));
        return c.json(transactions);
      });
      // RemoteStartTransaction fault injection.
      adminApi.get("/fail-mode", (c) => c.json(this.getRemoteStartFailState()));
      adminApi.post(
        "/fail-mode",
        zValidator(
          "json",
          z.object({
            mode: z.enum(["off", "ignore", "reject", "accept_no_start"]),
            durationMs: z.number().optional(),
          }),
        ),
        (c) => {
          const { mode, durationMs } = c.req.valid("json");
          this.setRemoteStartFailMode(mode, durationMs);
          return c.json(this.getRemoteStartFailState());
        },
      );
      // Connector fault injection (faulted / high temperature).
      adminApi.get("/fault", (c) => c.json(this.getConnectorFaultState()));
      adminApi.post(
        "/fault",
        zValidator(
          "json",
          z.object({
            connectorId: z.number().int().optional(),
            type: z.enum(["faulted", "high_temperature"]),
            on: z.boolean(),
          }),
        ),
        (c) => {
          const { connectorId, type, on } = c.req.valid("json");
          this.setConnectorFault(connectorId ?? 1, type, on);
          return c.json(this.getConnectorFaultState());
        },
      );
      // Charging speed (kW) + MeterValues report cadence (intervalMs).
      // kw=null restores the legacy fixed rate; intervalMs=null (or absent)
      // restores the default 15 s cadence. fluctuate=true draws a varying
      // power always under kw; absent leaves it as it is.
      adminApi.get("/charging-power", (c) =>
        c.json(this.getChargingPowerState()),
      );
      adminApi.post(
        "/charging-power",
        zValidator(
          "json",
          z.object({
            kw: z.number().nullable().optional(),
            fluctuate: z.boolean().optional(),
            intervalMs: z.number().positive().nullable().optional(),
          }),
        ),
        (c) => {
          const { kw, fluctuate, intervalMs } = c.req.valid("json");
          if (kw !== undefined) {
            this.setChargingPowerKw(kw);
          }
          if (fluctuate !== undefined) {
            this.transactionManager.setFluctuate(fluctuate);
            logger.info(
              `Charging power ${fluctuate ? "fluctuates under the set power" : "steady"}`,
            );
          }
          // Only touch the cadence when the caller says so: a speed change
          // alone must not reset an interval set through /meter.
          if (intervalMs !== undefined) {
            this.transactionManager.setMeterIntervalMs(intervalMs);
            this.configuration.set(
              "MeterValueSampleInterval",
              String(
                Math.round(this.transactionManager.meterIntervalMs / 1000),
              ),
            );
          }
          return c.json(this.getChargingPowerState());
        },
      );
      // Auto-stop energy target. kwh=null disarms. Optional kw/intervalMs set
      // the ramp rate + MeterValues cadence in the same call (so one button can
      // arm "stop at 1 kWh" AND set the 0.25 kWh / 5 s rate).
      adminApi.get("/charge-target", (c) =>
        c.json(this.getChargeTargetState()),
      );
      adminApi.post(
        "/charge-target",
        zValidator(
          "json",
          z.object({
            kwh: z.number().positive().nullable(),
            kw: z.number().positive().nullable().optional(),
            intervalMs: z.number().positive().nullable().optional(),
          }),
        ),
        (c) => {
          const { kwh, kw, intervalMs } = c.req.valid("json");
          this.setChargeTargetKwh(kwh);
          if (kw !== undefined) this.setChargingPowerKw(kw);
          if (intervalMs !== undefined) {
            this.transactionManager.setMeterIntervalMs(intervalMs ?? null);
          }
          return c.json(this.getChargeTargetState());
        },
      );
      // OCPP endpoint (WS_URL) control. GET reports the current base URL, the
      // .env baseline, and any persisted override. POST { url } persists a new
      // per-CP override (or clears it when url is null, reverting to the .env
      // baseline) and then restarts the process so the fresh boot reconnects to
      // it -- same supervisor-relaunch mechanism as /restart. Pass the bare host
      // (ws:// or wss://, no port path); "/<CP_ID>" is appended on connect.
      adminApi.get("/ws-url", (c) => c.json(this.getWsUrlState()));
      adminApi.post(
        "/ws-url",
        zValidator(
          "json",
          z.object({
            url: z.string().min(1).nullable(),
          }),
        ),
        (c) => {
          const { url } = c.req.valid("json");
          const cpId = this.vcpOptions.chargePointId;
          if (url === null) {
            clearWsUrlOverride(cpId);
          } else {
            const normalized = normalizeWsUrl(url);
            if (!/^wss?:\/\//i.test(normalized)) {
              return c.json(
                { error: "url must start with ws:// or wss://" },
                400,
              );
            }
            writeWsUrlOverride(cpId, normalized);
          }
          logger.info(
            "WS_URL change requested via admin API -- exiting for supervisor relaunch",
          );
          setTimeout(() => process.exit(0), 250);
          return c.json({ ...this.getWsUrlState(), restarting: true });
        },
      );
      // Take the station offline and bring it back without touching the
      // process: /disconnect closes the OCPP socket and holds the auto-restart
      // off, /connect reopens it with the boot sequence. The admin server stays
      // up throughout, so the panel keeps its handle on the station.
      adminApi.post("/disconnect", (c) => {
        this.disconnect();
        return c.json({ ok: true, connected: false });
      });
      adminApi.post("/connect", async (c) => {
        try {
          await this.reconnect();
        } catch (err) {
          return c.json(
            { ok: false, connected: false, error: String(err) },
            502,
          );
        }
        return c.json({ ok: true, connected: true });
      });
      // Full process restart. Exits the process so the shell supervisor
      // (run_simulators.sh / run_one_sim.sh, both a `while true` loop) relaunches
      // it -- a fresh process that reloads .env (WS_URL etc.) and code. The HTTP
      // reply is flushed first, then the process exits on a short delay.
      adminApi.post("/restart", (c) => {
        logger.info(
          "Restart requested via admin API -- exiting for supervisor relaunch",
        );
        setTimeout(() => process.exit(0), 250);
        return c.json({ ok: true, restarting: true });
      });
      this.adminServer = serve({
        fetch: adminApi.fetch,
        port: vcpOptions.adminPort,
      });
    }
    this.socWatchTimer = setInterval(() => {
      try {
        this.checkBatteries();
      } catch (err) {
        logger.warn(`Battery check failed: ${String(err)}`);
      }
    }, SOC_WATCH_MS);
    this.socWatchTimer.unref();
  }

  async connect(): Promise<void> {
    logger.info(`Connecting... | ${util.inspect(this.vcpOptions)}`);
    this.isFinishing = false;
    return new Promise((resolve, reject) => {
      const websocketUrl = `${this.vcpOptions.endpoint}/${this.vcpOptions.chargePointId}`;
      const protocol = toProtocolVersion(this.vcpOptions.ocppVersion);
      this.ws = new WebSocket(websocketUrl, [protocol], {
        rejectUnauthorized: false,
        followRedirects: true,
        headers: {
          ...(this.vcpOptions.basicAuthPassword && {
            Authorization: `Basic ${Buffer.from(
              `${this.vcpOptions.chargePointId}:${this.vcpOptions.basicAuthPassword}`,
            ).toString("base64")}`,
          }),
        },
      });

      this.ws.on("open", () => {
        this.connectedSince = new Date().toISOString();
        resolve();
        this.runBoot();
      });
      this.ws.on("message", (message: string) => this._onMessage(message));
      this.ws.on("ping", () => {
        logger.info("Received PING");
      });
      this.ws.on("pong", () => {
        logger.info("Received PONG");
      });
      this.ws.on("close", (code: number, reason: string) =>
        this._onClose(code, reason),
      );
      this.ws.on("error", (error: Error) => {
        logger.error("Websocket error:");
        logger.error(error);
        if (this.manuallyOffline) {
          // A /connect the CSMS refused, or a socket the admin closed on
          // purpose: the station stays offline, nothing is restarted. The
          // reject only matters while the open is still pending.
          reject(error);
          return;
        }
        close(this);
      });
    });
  }

  private runBoot(): void {
    if (!this.vcpOptions.boot) {
      return;
    }
    try {
      this.vcpOptions.boot(this);
    } catch (err) {
      logger.error(`Boot sequence failed: ${String(err)}`);
    }
  }

  // Admin /disconnect: close the OCPP socket only. The admin server stays up,
  // the timers stop so nothing tries to send on a closed socket, and the open
  // transactions stay in memory. Idempotent.
  disconnect(): void {
    this.manuallyOffline = true;
    this.connectedSince = null;
    this.stopTimers();
    if (!this.ws) {
      return;
    }
    logger.info("Going offline on request: closing the OCPP socket");
    this.isFinishing = true;
    this.ws.close();
    this.ws = undefined;
  }

  // Admin /connect: open the socket again on this same instance and run the
  // boot sequence. The flag stays set while the attempt is pending so a
  // refusal leaves the station offline instead of waking the auto-restart.
  // Resolves once the socket is open; rejects with the CSMS's refusal or on
  // CONNECT_TIMEOUT_MS.
  async reconnect(): Promise<void> {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.manuallyOffline = false;
      return;
    }
    this.manuallyOffline = true;
    this.stopTimers();
    if (this.ws) {
      this.isFinishing = true;
      this.ws.terminate();
      this.ws = undefined;
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(
        () =>
          reject(
            new Error(
              `CSMS did not accept the socket within ${CONNECT_TIMEOUT_MS} ms`,
            ),
          ),
        CONNECT_TIMEOUT_MS,
      );
    });
    try {
      await Promise.race([this.connect(), timedOut]);
    } catch (err) {
      this.isFinishing = true;
      this.ws?.terminate();
      this.ws = undefined;
      throw err;
    } finally {
      clearTimeout(timer);
    }
    this.manuallyOffline = false;
    this.transactionManager.resumeMeterTimers();
  }

  private stopTimers(): void {
    if (this.heartbeatIntervalId) {
      clearInterval(this.heartbeatIntervalId);
      this.heartbeatIntervalId = undefined;
    }
    this.transactionManager.suspendMeterTimers();
  }

  // biome-ignore lint/suspicious/noExplicitAny: ocpp types
  send(ocppCall: OcppCall<any>) {
    if (!this.ws) {
      throw new Error("Websocket not initialized. Call connect() first");
    }
    // Applied before enqueueing so the outbox holds exactly what went out on
    // the wire - resHandlers read the payload back from there.
    const beforeSend = getOcppOutgoingMessages(this.vcpOptions.ocppVersion)[
      ocppCall.action
    ]?.beforeSend;
    const resolvedCall = beforeSend
      ? { ...ocppCall, payload: beforeSend(this, ocppCall.payload) }
      : ocppCall;
    ocppOutbox.enqueue(resolvedCall);
    const jsonMessage = JSON.stringify([
      2,
      resolvedCall.messageId,
      resolvedCall.action,
      resolvedCall.payload,
    ]);
    logger.info(`Sending message ➡️  ${jsonMessage}`);
    this.recordFrame("out", jsonMessage);
    validateOcppOutgoingRequest(
      this.vcpOptions.ocppVersion,
      resolvedCall.action,
      JSON.parse(JSON.stringify(resolvedCall.payload)),
    );
    this.ws.send(jsonMessage);
    if (resolvedCall.action === "StatusNotification") {
      this.recordStatus(resolvedCall.payload ?? {});
    }
    // A real charger stops metering the moment it ends a session, whether or
    // not the CSMS ever acks the StopTransaction. Waiting for the .conf (the
    // resHandler) left transactions metering forever when staging stopped
    // replying -- 3-day "sessions" flooding MeterValues (2026-09-28).
    if (
      ocppCall.action === "StopTransaction" &&
      ocppCall.payload?.transactionId != null
    ) {
      this.transactionManager.stopTransaction(ocppCall.payload.transactionId);
    }
  }

  // biome-ignore lint/suspicious/noExplicitAny: ocpp types
  respond(result: OcppCallResult<any>) {
    if (!this.ws) {
      throw new Error("Websocket not initialized. Call connect() first");
    }
    const jsonMessage = JSON.stringify([3, result.messageId, result.payload]);
    validateOcppIncomingResponse(
      this.vcpOptions.ocppVersion,
      result.action,
      JSON.parse(JSON.stringify(result.payload)),
    );
    this.afterReplyDelay(result.messageId, () => {
      logger.info(`Responding with ➡️  ${jsonMessage}`);
      this.recordFrame("out", jsonMessage);
      this.ws?.send(jsonMessage);
    });
  }

  // Runs the answer now, or after replyDelayMs when one is set. The socket
  // check stays inside the callback: a socket that dropped during the hold
  // has nothing to answer on, and a Reset that closed it is not an error.
  private afterReplyDelay(messageId: string, send: () => void): void {
    if (this.replyDelayMs <= 0) {
      send();
      return;
    }
    logger.info(
      `Holding the answer to ${messageId} for ${this.replyDelayMs} ms`,
    );
    setTimeout(() => {
      if (this.ws?.readyState !== WebSocket.OPEN) {
        logger.warn(
          `Dropping the held answer to ${messageId}: the socket is gone`,
        );
        return;
      }
      send();
    }, this.replyDelayMs);
  }

  // biome-ignore lint/suspicious/noExplicitAny: ocpp types
  respondError(error: OcppCallError<any>) {
    if (!this.ws) {
      throw new Error("Websocket not initialized. Call connect() first");
    }
    const jsonMessage = JSON.stringify([
      4,
      error.messageId,
      error.errorCode,
      error.errorDescription,
      error.errorDetails,
    ]);
    this.afterReplyDelay(error.messageId, () => {
      logger.info(`Responding with ➡️  ${jsonMessage}`);
      this.recordFrame("out", jsonMessage);
      this.ws?.send(jsonMessage);
    });
  }

  configureHeartbeat(interval: number) {
    if (this.heartbeatIntervalId) {
      clearInterval(this.heartbeatIntervalId);
    }
    this.heartbeatIntervalId = setInterval(() => {
      this.send(heartbeatOcppMessage.request({}));
    }, interval);
  }

  close() {
    if (!this.ws) {
      throw new Error(
        "Trying to close a Websocket that was not opened. Call connect() first",
      );
    }
    this.isFinishing = true;
    if (this.heartbeatIntervalId) {
      clearInterval(this.heartbeatIntervalId);
      this.heartbeatIntervalId = undefined;
    }
    clearInterval(this.socWatchTimer);
    this.ws.close();
    this.ws = undefined;
    if (this.adminServer) {
      this.adminServer.close();
      this.adminServer = undefined;
    }
  }

  async getDiagnosticData(): Promise<LogEntry[]> {
    try {
      // Get logs from Winston logger's memory
      const transport = logger.transports[0];

      // Create a promise that resolves with collected logs
      const logStream = new Promise<LogEntry[]>((resolve) => {
        const entries: LogEntry[] = [];

        // Listen for new logs
        transport.on(
          "logged",
          (info: {
            timestamp: string;
            level: string;
            message: string;
            [key: string]: unknown;
          }) => {
            entries.push({
              type: "Application",
              timestamp: info.timestamp || new Date().toISOString(),
              level: info.level,
              message: info.message,
              metadata: Object.fromEntries(
                Object.entries(info).filter(
                  ([key]) => !["timestamp", "level", "message"].includes(key),
                ),
              ),
            });
          },
        );

        // Resolve after a short delay to collect recent logs
        setTimeout(() => resolve(entries), 10000);
      });

      return await logStream;
    } catch (err) {
      logger.error("Failed to read application logs:", err);
      return [];
    }
  }

  async postMessageAction(
    action: string,
    callback: () => void | Promise<void>,
  ) {
    this.postMessageActions[action] = callback;
  }

  private _onMessage(message: string) {
    logger.info(`Receive message ⬅️  ${message}`);
    this.recordFrame("in", String(message));
    // biome-ignore lint/suspicious/noExplicitAny: ocpp message format
    let data: any[];
    try {
      data = JSON.parse(message);
    } catch (err) {
      logger.error(`Failed to parse message: ${err}`);
      return;
    }
    const [type, ...rest] = data;
    if (type === 2) {
      const [messageId, action, payload] = rest;
      validateOcppIncomingRequest(this.vcpOptions.ocppVersion, action, payload);
      this.messageHandler.handleCall(this, { messageId, action, payload });
      if (this.postMessageActions[action]) {
        logger.info(`Executing postMessageAction for ${action}`);
        this.postMessageActions[action]();
      }
    } else if (type === 3) {
      const [messageId, payload] = rest;
      const enqueuedCall = ocppOutbox.get(messageId);
      if (!enqueuedCall) {
        if (process.env.CONTINUE_ON_UNKNOWN_MESSAGE_ID) {
          return;
        }
        throw new Error(
          `Received CallResult for unknown messageId=${messageId}`,
        );
      }
      if (
        enqueuedCall.action === "Heartbeat" ||
        enqueuedCall.action === "BootNotification"
      ) {
        this.lastHeartbeatAt = new Date().toISOString();
      }
      this.lastReply = {
        kind: "result",
        action: enqueuedCall.action,
        messageId,
        at: new Date().toISOString(),
        payload,
      };
      validateOcppOutgoingResponse(
        this.vcpOptions.ocppVersion,
        enqueuedCall.action,
        payload,
      );
      this.messageHandler.handleCallResult(this, enqueuedCall, {
        messageId,
        payload,
        action: enqueuedCall.action,
      });
      this.callWaiters.get(messageId)?.({ status: "result", payload });
      this.callWaiters.delete(messageId);
      if (
        this.abandonedStarts.delete(messageId) &&
        payload?.transactionId != null
      ) {
        logger.warn(
          `Late StartTransaction.conf (transactionId=${payload.transactionId}) after the caller timed out -- stopping it`,
        );
        this.send(
          call("StopTransaction", {
            transactionId: payload.transactionId,
            meterStop: 0,
            timestamp: new Date().toISOString(),
            reason: "Other",
          }),
        );
        this.send(
          call("StatusNotification", {
            connectorId: enqueuedCall.payload.connectorId,
            errorCode: "NoError",
            status: "Available",
          }),
        );
      }
    } else if (type === 4) {
      const [messageId, errorCode, errorDescription, errorDetails] = rest;
      // Cosmos answers an internal failure (Spark 401, a thrown handler) with
      // a CALLERROR carrying a fresh uuid rather than the id of the call that
      // failed, so this is often not in the outbox. It is still the last
      // thing the CSMS said, which is what a panel wants to see.
      const failedCall = ocppOutbox.get(messageId);
      this.lastReply = {
        kind: "error",
        action: failedCall?.action ?? null,
        messageId,
        at: new Date().toISOString(),
        errorCode,
        errorDescription,
        errorDetails,
      };
      this.messageHandler.handleCallError(this, {
        messageId,
        errorCode,
        errorDescription,
        errorDetails,
      });
      this.callWaiters.get(messageId)?.({
        status: "error",
        errorCode,
        errorDescription,
      });
      this.callWaiters.delete(messageId);
      this.abandonedStarts.delete(messageId);
    } else {
      throw new Error(`Unrecognized message type ${type}`);
    }
  }

  private _onClose(code: number, reason: string) {
    this.connectedSince = null;
    if (this.isFinishing || this.manuallyOffline) {
      return;
    }
    logger.info(`Connection closed. code=${code}, reason=${reason}`);
    close(this);
  }
}
