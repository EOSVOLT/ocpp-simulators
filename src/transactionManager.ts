import { logger } from "./logger";
import type { VCP } from "./vcp";

const METER_VALUES_INTERVAL_SEC = 15;

// Periodic MeterValues are sent for every ongoing transaction. Set
// DISABLE_METER_VALUES=true to stop sending them - useful when they only add
// noise, or when the values are driven by admin commands instead.
const METER_VALUES_DISABLED = process.env.DISABLE_METER_VALUES === "true";

type TransactionId = string | number;

interface TransactionState {
  startedAt: Date;
  idTag: string;
  transactionId: TransactionId;
  meterValue: number;
  evseId?: number;
  connectorId: number;
  // Rebase anchor for power-based metering (only used when chargingPowerW is
  // set): the accumulated Wh and wall-clock at the last power change, so the
  // energy register stays monotonic when the charging speed is changed mid-session.
  baseWh: number;
  baseTime: number;
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

  // Configurable charging power in watts. null -> legacy behaviour (a fixed
  // synthetic rate), which is what the production fleet uses. Set via the admin
  // /charging-power endpoint (test chargers) to control how fast the energy
  // register climbs, and therefore the MeterValues readings sent to the CSMS.
  chargingPowerW: number | null = null;

  // How often each transaction pushes a MeterValues report to the CSMS.
  // Defaults to the legacy 15 s cadence; test chargers can crank this down (via
  // the admin /charging-power endpoint) to make the energy register visibly
  // climb faster -- e.g. 2000 ms so a report lands every 2 s.
  meterIntervalMs: number = METER_VALUES_INTERVAL_SEC * 1000;

  // Whether every transaction runs its own periodic MeterValues timer. Starts
  // from DISABLE_METER_VALUES and can be flipped at runtime (admin /meter).
  autoMeterValues = !METER_VALUES_DISABLED;

  // Rebase every active transaction to its current reading, then switch power,
  // so changing speed never makes the energy register jump backwards.
  setChargingPowerW(watts: number | null): void {
    for (const transaction of Array.from(this.transactions.values())) {
      transaction.baseWh = this.getMeterValue(transaction.transactionId);
      transaction.baseTime = Date.now();
    }
    this.chargingPowerW = watts;
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
    if (this.chargingPowerW == null) {
      // Legacy fixed synthetic rate (production fleet).
      return (new Date().getTime() - transaction.startedAt.getTime()) / 100;
    }
    // Power-based: accumulated Wh since the last rebase + power * elapsed hours.
    const elapsedHours = (Date.now() - transaction.baseTime) / 3_600_000;
    return transaction.baseWh + elapsedHours * this.chargingPowerW;
  }
}
