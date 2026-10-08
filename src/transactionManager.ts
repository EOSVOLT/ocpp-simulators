import { logger } from "./logger";
import type { VCP } from "./vcp";

const METER_VALUES_INTERVAL_SEC = 15;

// The legacy fixed synthetic rate (no charging power set): 10 Wh per second,
// i.e. 36 kW. What the production fleet runs at.
export const LEGACY_RATE_W = 36_000;

// Periodic MeterValues are sent for every ongoing transaction. Set
// DISABLE_METER_VALUES=true to stop sending them - useful when they only add
// noise, or when the values are driven by admin commands instead.
const METER_VALUES_DISABLED = process.env.DISABLE_METER_VALUES === "true";

// While the power varies (charge curve, fluctuation) the register is
// integrated in steps of at most this long.
const INTEGRATION_STEP_MS = 1000;

// The share of the charger's power a car takes at this state of charge, the
// way a lithium pack tapers once the cells near their voltage limit. AC (up
// to 22 kW) is held back by the on-board charger, so full power lasts to 80 %
// and then falls to a tenth by 100 %. DC eases off from 50 % (70 % of the
// power at 80 %) before the same fall to a tenth.
const chargeCurve = (socPercent: number, powerW: number): number => {
  const soc = Math.min(100, Math.max(0, socPercent));
  if (powerW <= 22_000) {
    return soc < 80 ? 1 : 1 - (0.9 * (soc - 80)) / 20;
  }
  if (soc < 50) {
    return 1;
  }
  if (soc < 80) {
    return 1 - (0.3 * (soc - 50)) / 30;
  }
  return 0.7 - (0.6 * (soc - 80)) / 20;
};

// A slow, smooth wobble between 82 % and 98 % of the power: three sines a
// few tens of seconds to a few minutes long, phased per transaction so two
// connectors never move in step. Never reaches the set power.
const fluctuation = (seed: number, time: number): number => {
  const s = time / 1000;
  const wave =
    (Math.sin(s / 3 + seed) +
      0.7 * Math.sin(s / 11 + seed * 2.3) +
      0.5 * Math.sin(s / 37 + seed * 4.1)) /
    2.2;
  return 0.9 + 0.08 * wave;
};

export type TransactionId = string | number;

interface TransactionState {
  startedAt: Date;
  idTag: string;
  transactionId: TransactionId;
  meterValue: number;
  evseId?: number;
  connectorId: number;
  // The register as integrated up to baseTime. Every read brings it up to
  // now at the power of the moment, so it stays monotonic whatever changes.
  baseWh: number;
  baseTime: number;
  // Phase of this transaction's power fluctuation.
  seed: number;
  // The connector reports SuspendedEV/SuspendedEVSE: no energy flows, so the
  // register holds still until the connector reports Charging again.
  paused: boolean;
}

// A simulated EV battery on one connector (admin /soc). The session's energy
// fills it from startPercent, and once it is full the car stops drawing: the
// register holds at the battery's room and the power drops to 0.
export interface SocConfig {
  batteryWh: number;
  startPercent: number;
  // Taper the power with the state of charge (see chargeCurve).
  curve: boolean;
}

// One transaction's live reading, as MeterValues and the admin API report it.
export interface MeterSnapshot {
  meterWh: number;
  powerW: number;
  // null when no battery is simulated on the connector.
  socPercent: number | null;
  full: boolean;
  paused: boolean;
}

interface StartTransactionProps {
  transactionId: TransactionId;
  idTag: string;
  evseId?: number;
  connectorId: number;
  meterValuesCallback: (transactionState: TransactionState) => Promise<void>;
}

export class TransactionManager {
  transactions: Map<
    TransactionId,
    TransactionState & {
      // Absent when periodic MeterValues are disabled (DISABLE_METER_VALUES or
      // the admin /meter switch), rather than a timer that wakes up to do nothing.
      meterValuesTimer?: ReturnType<typeof setInterval>;
      // Kept so the MeterValues timer can be re-armed when the report cadence
      // (meterIntervalMs) changes mid-session.
      meterValuesCallback: (
        transactionState: TransactionState,
      ) => Promise<void>;
    }
  > = new Map();

  // Configurable charging power in watts, 22 kW unless changed. null -> legacy
  // behaviour (the fixed 36 kW synthetic rate). Set via the admin
  // /charging-power endpoint to control how fast the energy register climbs,
  // and therefore the MeterValues readings sent to the CSMS.
  chargingPowerW: number | null = 22_000;

  // How often each transaction pushes a MeterValues report to the CSMS.
  // Defaults to the legacy 15 s cadence; test chargers can crank this down (via
  // the admin /charging-power endpoint) to make the energy register visibly
  // climb faster -- e.g. 2000 ms so a report lands every 2 s.
  meterIntervalMs: number = METER_VALUES_INTERVAL_SEC * 1000;

  // Whether every transaction runs its own periodic MeterValues timer. Starts
  // from DISABLE_METER_VALUES and can be flipped at runtime (admin /meter).
  autoMeterValues = !METER_VALUES_DISABLED;

  // Draw a varying power, always under the set one (admin /charging-power).
  fluctuate = false;

  // Simulated batteries by connector id. Sticky across sessions until cleared.
  private socConfigs = new Map<number, SocConfig>();

  // Rebase every active transaction to its current reading, then switch power,
  // so changing speed never makes the energy register jump backwards.
  setChargingPowerW(watts: number | null): void {
    this.rebaseAll();
    this.chargingPowerW = watts;
  }

  setFluctuate(on: boolean): void {
    this.rebaseAll();
    this.fluctuate = on;
  }

  private rebaseAll(): void {
    for (const transaction of Array.from(this.transactions.values())) {
      this.rebase(transaction);
    }
  }

  getSocConfig(connectorId: number): SocConfig | null {
    return this.socConfigs.get(connectorId) ?? null;
  }

  // Set or clear the battery on a connector. The running transaction is
  // rebased first so the register never moves backwards when the room shrinks.
  setSocConfig(connectorId: number, config: SocConfig | null): void {
    const transaction = this.onConnector(connectorId);
    if (transaction) {
      this.rebase(transaction);
    }
    if (config) {
      this.socConfigs.set(connectorId, config);
    } else {
      this.socConfigs.delete(connectorId);
    }
  }

  // Hold or release the energy flow on a connector (its status went to
  // Suspended* or back to Charging). No-op without a transaction.
  setPaused(connectorId: number, paused: boolean): void {
    const transaction = this.onConnector(connectorId);
    if (!transaction || transaction.paused === paused) {
      return;
    }
    this.rebase(transaction);
    transaction.paused = paused;
  }

  onConnector(connectorId: number) {
    return Array.from(this.transactions.values()).find(
      (candidate) => candidate.connectorId === connectorId,
    );
  }

  // Bring the register up to now. A constant power is one step; a varying
  // one is stepped so the curve and the wobble are followed as they move.
  private rebase(transaction: TransactionState): void {
    const now = Date.now();
    const room = this.roomWh(transaction.connectorId);
    const varying =
      this.fluctuate || !!this.socConfigs.get(transaction.connectorId)?.curve;
    let wh = transaction.baseWh;
    let time = transaction.baseTime;
    while (time < now && (room === null || wh < room)) {
      const step = varying
        ? Math.min(INTEGRATION_STEP_MS, now - time)
        : now - time;
      wh += (this.powerAt(transaction, wh, time) * step) / 3_600_000;
      time += step;
    }
    // A full battery takes nothing more; never below the last rebase, so a
    // battery made smaller mid-session holds the register instead of rolling it back.
    transaction.baseWh =
      room === null ? wh : Math.max(transaction.baseWh, Math.min(wh, room));
    transaction.baseTime = now;
  }

  // The power a transaction draws with this much energy delivered, at this
  // moment: the set power, tapered by the charge curve, wobbled by the
  // fluctuation. Ignores a full battery; the caller clamps to its room.
  private powerAt(
    transaction: TransactionState,
    meterWh: number,
    time: number,
  ): number {
    if (transaction.paused) {
      return 0;
    }
    const setW = this.chargingPowerW ?? LEGACY_RATE_W;
    let powerW = setW;
    const config = this.socConfigs.get(transaction.connectorId);
    if (config?.curve) {
      powerW *= chargeCurve(
        config.startPercent + (meterWh / config.batteryWh) * 100,
        setW,
      );
    }
    if (this.fluctuate) {
      powerW *= fluctuation(transaction.seed, time);
    }
    return powerW;
  }

  // The energy a connector's battery still takes, or null without a battery.
  private roomWh(connectorId: number): number | null {
    const config = this.socConfigs.get(connectorId);
    if (!config) {
      return null;
    }
    return Math.max(0, ((100 - config.startPercent) / 100) * config.batteryWh);
  }

  snapshot(transactionId: TransactionId): MeterSnapshot | null {
    const transaction = this.transactions.get(transactionId);
    if (!transaction) {
      return null;
    }
    const meterWh = this.getMeterValue(transactionId);
    const room = this.roomWh(transaction.connectorId);
    const full = room !== null && meterWh >= room - 0.001;
    const config = this.socConfigs.get(transaction.connectorId);
    const socPercent = config
      ? full
        ? 100
        : Math.min(
            100,
            config.startPercent + (meterWh / config.batteryWh) * 100,
          )
      : null;
    return {
      meterWh,
      powerW: full
        ? 0
        : Math.round(this.powerAt(transaction, meterWh, Date.now())),
      socPercent,
      full,
      paused: transaction.paused,
    };
  }

  // Change the MeterValues report cadence. null restores the default 15 s.
  // Re-arms the timer on every active transaction so the new cadence takes
  // effect immediately rather than only on the next session.
  setMeterIntervalMs(ms: number | null): void {
    this.meterIntervalMs = ms == null ? METER_VALUES_INTERVAL_SEC * 1000 : ms;
    this.rearmMeterTimers();
  }

  // Periodic MeterValues on/off at runtime (admin /meter). Off clears every
  // timer but keeps the transactions, so the register still climbs and a
  // one-off tick (/meter-tick) or a later "on" reports the right reading.
  setAutoMeterValues(enabled: boolean): void {
    this.autoMeterValues = enabled;
    this.rearmMeterTimers();
  }

  // Offline on purpose (admin /disconnect): clear every timer but keep the
  // transactions, so nothing tries to send on a closed socket and the register
  // still climbs. resumeMeterTimers() re-arms them when the socket is back.
  suspendMeterTimers(): void {
    for (const transaction of Array.from(this.transactions.values())) {
      if (transaction.meterValuesTimer) {
        clearInterval(transaction.meterValuesTimer);
        transaction.meterValuesTimer = undefined;
      }
    }
  }

  resumeMeterTimers(): void {
    this.rearmMeterTimers();
  }

  private rearmMeterTimers(): void {
    for (const transaction of Array.from(this.transactions.values())) {
      if (transaction.meterValuesTimer) {
        clearInterval(transaction.meterValuesTimer);
      }
      transaction.meterValuesTimer = this.autoMeterValues
        ? this.armMeterTimer(
            transaction.transactionId,
            transaction.meterValuesCallback,
          )
        : undefined;
    }
  }

  // One MeterValues now for the transaction on this connector: the timer's
  // tick, on demand. Returns false when the connector has no transaction.
  tick(connectorId: number): boolean {
    const transaction = Array.from(this.transactions.values()).find(
      (candidate) => candidate.connectorId === connectorId,
    );
    if (!transaction) {
      return false;
    }
    const {
      meterValuesTimer: _timer,
      meterValuesCallback,
      ...state
    } = transaction;
    meterValuesCallback({
      ...state,
      meterValue: this.getMeterValue(transaction.transactionId),
    });
    return true;
  }

  // Create the recurring MeterValues timer for a transaction at the current
  // cadence. Extracted so setMeterIntervalMs() can re-arm it in place.
  private armMeterTimer(
    transactionId: TransactionId,
    meterValuesCallback: (transactionState: TransactionState) => Promise<void>,
  ): ReturnType<typeof setInterval> {
    return setInterval(() => {
      // stopTransaction() may have already removed this entry if a tick was
      // already queued when the transaction stopped -- skip it rather than
      // crash the process on an undefined destructure.
      const currentTransactionState = this.transactions.get(transactionId);
      if (!currentTransactionState) return;
      const {
        meterValuesTimer,
        meterValuesCallback: _cb,
        ...currentTransaction
      } = currentTransactionState;
      meterValuesCallback({
        ...currentTransaction,
        meterValue: this.getMeterValue(transactionId),
      });
    }, this.meterIntervalMs);
  }

  canStartNewTransaction(connectorId: number) {
    return !Array.from(this.transactions.values()).some(
      (transaction) => transaction.connectorId === connectorId,
    );
  }

  startTransaction(vcp: VCP, startTransactionProps: StartTransactionProps) {
    // A connector carries at most one transaction, and each transaction owns
    // exactly one MeterValues timer. Without this, a CSMS that hands back an
    // already-open transactionId (or a stale tx left behind by an unacked
    // StopTransaction) made us overwrite the map entry and leak the old
    // interval -- every cycle added another timer, multiplying MeterValues
    // (seen at 14-18x on staging, 2026-09-28).
    for (const existing of Array.from(this.transactions.values())) {
      if (
        existing.transactionId === startTransactionProps.transactionId ||
        existing.connectorId === startTransactionProps.connectorId
      ) {
        logger.warn(
          `Dropping local transaction ${existing.transactionId} on connector ${existing.connectorId} ` +
            `before starting ${startTransactionProps.transactionId}`,
        );
        this.stopTransaction(existing.transactionId);
      }
    }
    // No timer at all when disabled, rather than one that wakes up to do
    // nothing.
    const meterValuesTimer = this.autoMeterValues
      ? this.armMeterTimer(
          startTransactionProps.transactionId,
          startTransactionProps.meterValuesCallback,
        )
      : undefined;
    this.transactions.set(startTransactionProps.transactionId, {
      transactionId: startTransactionProps.transactionId,
      idTag: startTransactionProps.idTag,
      meterValue: 0,
      startedAt: new Date(),
      evseId: startTransactionProps.evseId,
      connectorId: startTransactionProps.connectorId,
      meterValuesTimer: meterValuesTimer,
      meterValuesCallback: startTransactionProps.meterValuesCallback,
      baseWh: 0,
      baseTime: Date.now(),
      seed: Math.random() * 2 * Math.PI,
      paused: false,
    });
  }

  /**
   * The id of the only ongoing transaction, or undefined when there is no
   * transaction or more than one - i.e. when it would be ambiguous.
   */
  onlyTransactionId(): TransactionId | undefined {
    if (this.transactions.size !== 1) {
      return undefined;
    }
    return this.transactions.keys().next().value;
  }

  stopTransaction(transactionId: TransactionId) {
    const transaction = this.transactions.get(transactionId);
    if (transaction?.meterValuesTimer) {
      clearInterval(transaction.meterValuesTimer);
    }
    this.transactions.delete(transactionId);
  }

  getMeterValue(transactionId: TransactionId) {
    const transaction = this.transactions.get(transactionId);
    if (!transaction) {
      return 0;
    }
    this.rebase(transaction);
    return transaction.baseWh;
  }
}
